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
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

// A helper class, also handles view materialization
public class ReadSessionCreator
{
    private final BigQueryClient bigQueryClient;
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final boolean viewEnabled;
    private final Duration viewExpiration;

    public ReadSessionCreator(
            BigQueryClient bigQueryClient,
            BigQueryStorageClientFactory bigQueryStorageClientFactory,
            boolean viewEnabled,
            Duration viewExpiration)
    {
        this.bigQueryClient = bigQueryClient;
        this.bigQueryStorageClientFactory = bigQueryStorageClientFactory;
        this.viewEnabled = viewEnabled;
        this.viewExpiration = viewExpiration;
    }

    public Storage.ReadSession create(TableId remoteTable, List<String> selectedFields, Optional<String> filter, int parallelism)
    {
        TableInfo tableDetails = bigQueryClient.getTable(remoteTable);

        TableInfo actualTable = getActualTable(tableDetails, selectedFields);

        List<String> filteredSelectedFields = selectedFields.stream()
                .filter(BigQueryUtil::validColumnName)
                .collect(toList());

        try (BigQueryStorageClient bigQueryStorageClient = bigQueryStorageClientFactory.createBigQueryStorageClient()) {
            ReadOptions.TableReadOptions.Builder readOptions = ReadOptions.TableReadOptions.newBuilder()
                    .addAllSelectedFields(filteredSelectedFields);
            filter.ifPresent(readOptions::setRowRestriction);

            TableReferenceProto.TableReference tableReference = toTableReference(actualTable.getTableId());

            Storage.ReadSession readSession = bigQueryStorageClient.createReadSession(
                    Storage.CreateReadSessionRequest.newBuilder()
                            .setParent("projects/" + bigQueryClient.getProjectId())
                            .setFormat(Storage.DataFormat.AVRO)
                            .setRequestedStreams(parallelism)
                            .setReadOptions(readOptions)
                            .setTableReference(tableReference)
                            // The BALANCED sharding strategy causes the server to
                            // assign roughly the same number of rows to each stream.
                            .setShardingStrategy(Storage.ShardingStrategy.BALANCED)
                            .build());

            return readSession;
        }
    }

    TableReferenceProto.TableReference toTableReference(TableId tableId)
    {
        return TableReferenceProto.TableReference.newBuilder()
                .setProjectId(tableId.getProject())
                .setDatasetId(tableId.getDataset())
                .setTableId(tableId.getTable())
                .build();
    }

    private TableInfo getActualTable(
            TableInfo remoteTable,
            List<String> requiredColumns)
    {
        TableDefinition tableDefinition = remoteTable.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        if (TableDefinition.Type.TABLE == tableType) {
            return remoteTable;
        }
        if (TableDefinition.Type.VIEW == tableType) {
            if (!viewEnabled) {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Views are not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
                        BigQueryConfig.VIEWS_ENABLED));
            }
            // get it from the view
            return bigQueryClient.getCachedTable(viewExpiration, remoteTable, requiredColumns);
        }
        else {
            // not regular table or a view
            throw new TrinoException(NOT_SUPPORTED, format("Table type '%s' of table '%s.%s' is not supported",
                    tableType, remoteTable.getTableId().getDataset(), remoteTable.getTableId().getTable()));
        }
    }
}
