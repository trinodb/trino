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

import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.protobuf.ByteString;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.google.cloud.bigquery.TableDefinition.Type.SNAPSHOT;
import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec.ZSTD;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toList;
import static org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeSchema;

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

    public ReadSession create(ConnectorSession session, TableId remoteTable, List<String> selectedFields, Optional<String> filter, int parallelism)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        TableInfo tableDetails = client.getTable(remoteTable)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(remoteTable.getDataset(), remoteTable.getTable())));

        TableInfo actualTable = getActualTable(client, tableDetails, selectedFields);

        List<String> filteredSelectedFields = selectedFields.stream()
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
            CreateReadSessionRequest createReadSessionRequest = CreateReadSessionRequest.newBuilder()
                    .setParent("projects/" + client.getParentProjectId())
                    .setReadSession(ReadSession.newBuilder()
                            .setDataFormat(format)
                            .setTable(toTableResourceName(actualTable.getTableId()))
                            .setReadOptions(readOptions))
                    .setMaxStreamCount(parallelism)
                    .build();

            return Failsafe.with(RetryPolicy.builder()
                            .withMaxRetries(maxCreateReadSessionRetries)
                            .withBackoff(10, 500, MILLIS)
                            .onRetry(event -> log.debug("Request failed, retrying: %s", event.getLastException()))
                            .abortOn(failure -> !BigQueryUtil.isRetryable(failure))
                            .build())
                    .get(() -> bigQueryReadClient.createReadSession(createReadSessionRequest));
        }
    }

    public String getSchemaAsString(ReadSession readSession)
    {
        if (arrowSerializationEnabled) {
            return deserializeArrowSchema(readSession.getArrowSchema().getSerializedSchema());
        }
        return readSession.getAvroSchema().getSchema();
    }

    private static String deserializeArrowSchema(ByteString serializedSchema)
    {
        try {
            return deserializeSchema(new ReadChannel(new ByteArrayReadableSeekableByteChannel(serializedSchema.toByteArray())))
                    .toJson();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    String toTableResourceName(TableId tableId)
    {
        return format("projects/%s/datasets/%s/tables/%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }

    private TableInfo getActualTable(
            BigQueryClient client,
            TableInfo remoteTable,
            List<String> requiredColumns)
    {
        TableDefinition tableDefinition = remoteTable.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        if (tableType == TABLE || tableType == SNAPSHOT) {
            return remoteTable;
        }
        if (tableType == VIEW) {
            if (!viewEnabled) {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Views are not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
                        BigQueryConfig.VIEWS_ENABLED));
            }
            // get it from the view
            return client.getCachedTable(viewExpiration, remoteTable, requiredColumns);
        }
        // not regular table or a view
        throw new TrinoException(NOT_SUPPORTED, format("Table type '%s' of table '%s.%s' is not supported",
                tableType, remoteTable.getTableId().getDataset(), remoteTable.getTableId().getTable()));
    }
}
