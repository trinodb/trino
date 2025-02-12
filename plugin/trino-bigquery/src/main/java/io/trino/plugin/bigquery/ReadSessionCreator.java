/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.bigquery;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.List;
import java.util.Optional;

import static com.google.cloud.bigquery.TableDefinition.Type.EXTERNAL;
import static com.google.cloud.bigquery.TableDefinition.Type.MATERIALIZED_VIEW;
import static com.google.cloud.bigquery.TableDefinition.Type.SNAPSHOT;
import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec.ZSTD;
import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_CREATE_READ_SESSION_ERROR;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.isViewMaterializationWithFilter;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toList;

// A helper class, also handles view materialization
public class ReadSessionCreator
{
    private static final Logger log = Logger.get(ReadSessionCreator.class);

    private final BigQueryClientFactory bigQueryClientFactory;
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final boolean viewEnabled;
    private final boolean arrowSerializationEnabled;
    private final Duration viewExpiration;
    private final int maxCreateReadSessionRetries;

    public ReadSessionCreator(
            BigQueryClientFactory bigQueryClientFactory,
            BigQueryReadClientFactory bigQueryReadClientFactory,
            boolean viewEnabled,
            boolean arrowSerializationEnabled,
            Duration viewExpiration,
            int maxCreateReadSessionRetries)
    {
        this.bigQueryClientFactory = bigQueryClientFactory;
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.viewEnabled = viewEnabled;
        this.arrowSerializationEnabled = arrowSerializationEnabled;
        this.viewExpiration = viewExpiration;
        this.maxCreateReadSessionRetries = maxCreateReadSessionRetries;
    }

    public ReadSession create(ConnectorSession session, TableId remoteTable, List<BigQueryColumnHandle> selectedFields, Optional<String> filter, int currentWorkerCount)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        TableInfo tableDetails = client.getTable(remoteTable)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(remoteTable.getDataset(), remoteTable.getTable())));

        TableInfo actualTable = getActualTable(client, tableDetails, selectedFields, isViewMaterializationWithFilter(session) ? filter : Optional.empty());

        List<String> filteredSelectedFields = selectedFields.stream()
                .map(BigQueryColumnHandle::getQualifiedName)
                .map(BigQueryUtil::toBigQueryColumnName)
                .collect(toList());

        try (BigQueryReadClient bigQueryReadClient = bigQueryReadClientFactory.create(session)) {
            ReadSession.TableReadOptions.Builder readOptions = ReadSession.TableReadOptions.newBuilder()
                    .addAllSelectedFields(filteredSelectedFields);
            filter.ifPresent(readOptions::setRowRestriction);
            DataFormat format = DataFormat.AVRO;
            if (arrowSerializationEnabled) {
                format = DataFormat.ARROW;
                readOptions.setArrowSerializationOptions(ArrowSerializationOptions.newBuilder()
                        .setBufferCompression(ZSTD)
                        .build());
            }
            // At least 100 to cater for cluster scale up
            int desiredParallelism = Math.min(currentWorkerCount * 3, 100);
            CreateReadSessionRequest createReadSessionRequest = CreateReadSessionRequest.newBuilder()
                    .setParent("projects/" + client.getParentProjectId())
                    .setReadSession(ReadSession.newBuilder()
                            .setDataFormat(format)
                            .setTable(toTableResourceName(actualTable.getTableId()))
                            .setReadOptions(readOptions))
                    .setPreferredMinStreamCount(desiredParallelism)
                    .build();

            return Failsafe.with(RetryPolicy.builder()
                            .withMaxRetries(maxCreateReadSessionRetries)
                            .withBackoff(10, 500, MILLIS)
                            .onRetry(event -> log.debug("Request failed, retrying: %s", event.getLastException()))
                            .handleIf(BigQueryUtil::isRetryable)
                            .build())
                    .get(() -> {
                        try {
                            return bigQueryReadClient.createReadSession(createReadSessionRequest);
                        }
                        catch (ApiException e) {
                            throw new TrinoException(BIGQUERY_CREATE_READ_SESSION_ERROR, "Cannot create read session" + firstNonNull(e.getMessage(), e), e);
                        }
                    });
        }
    }

    String toTableResourceName(TableId tableId)
    {
        return format("projects/%s/datasets/%s/tables/%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }

    private TableInfo getActualTable(
            BigQueryClient client,
            TableInfo remoteTable,
            List<BigQueryColumnHandle> requiredColumns,
            Optional<String> filter)
    {
        TableDefinition tableDefinition = remoteTable.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        if (tableType == TABLE || tableType == SNAPSHOT || tableType == EXTERNAL) {
            return remoteTable;
        }
        if (tableType == VIEW || tableType == MATERIALIZED_VIEW) {
            if (!viewEnabled) {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Views are not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
                        BigQueryConfig.VIEWS_ENABLED));
            }
            // get it from the view
            return client.getCachedTable(viewExpiration, remoteTable, requiredColumns, filter);
        }
        // Storage API doesn't support reading other table types (materialized views, non-biglake external tables)
        throw new TrinoException(NOT_SUPPORTED, format("Table type '%s' of table '%s.%s' is not supported",
                tableType, remoteTable.getTableId().getDataset(), remoteTable.getTableId().getTable()));
    }
}
