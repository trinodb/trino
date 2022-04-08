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
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

// A helper class, also handles view materialization
public class ReadSessionCreator
{
    private final BigQueryClientFactory bigQueryClientFactory;
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final boolean viewEnabled;
    private final Duration viewExpiration;

    public ReadSessionCreator(
            BigQueryClientFactory bigQueryClientFactory,
            BigQueryReadClientFactory bigQueryReadClientFactory,
            boolean viewEnabled,
            Duration viewExpiration)
    {
        this.bigQueryClientFactory = bigQueryClientFactory;
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.viewEnabled = viewEnabled;
        this.viewExpiration = viewExpiration;
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

            ReadSession readSession = bigQueryReadClient.createReadSession(
                    CreateReadSessionRequest.newBuilder()
                            .setParent("projects/" + client.getProjectId())
                            .setReadSession(ReadSession.newBuilder()
                                    .setDataFormat(DataFormat.AVRO)
                                    .setTable(toTableResourceName(actualTable.getTableId()))
                                    .setReadOptions(readOptions))
                            .setMaxStreamCount(parallelism)
                            .build());

            return readSession;
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
            return client.getCachedTable(viewExpiration, remoteTable, requiredColumns);
        }
        else {
            // not regular table or a view
            throw new TrinoException(NOT_SUPPORTED, format("Table type '%s' of table '%s.%s' is not supported",
                    tableType, remoteTable.getTableId().getDataset(), remoteTable.getTableId().getTable()));
        }
    }
}
