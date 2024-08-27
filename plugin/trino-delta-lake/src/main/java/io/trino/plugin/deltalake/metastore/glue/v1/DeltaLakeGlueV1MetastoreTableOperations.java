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
package io.trino.plugin.deltalake.metastore.glue.v1;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.Optional;

import static io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.tableMetadataParameters;
import static io.trino.plugin.hive.metastore.glue.v1.converter.GlueInputConverter.convertGlueTableToTableInput;
import static io.trino.plugin.hive.metastore.glue.v1.converter.GlueToTrinoConverter.getTableParameters;
import static java.util.Objects.requireNonNull;

public class DeltaLakeGlueV1MetastoreTableOperations
        implements DeltaLakeTableOperations
{
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;

    public DeltaLakeGlueV1MetastoreTableOperations(AWSGlueAsync glueClient, GlueMetastoreStats stats)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public void commitToExistingTable(SchemaTableName schemaTableName, long version, String schemaString, Optional<String> tableComment)
    {
        GetTableRequest getTableRequest = new GetTableRequest()
                .withDatabaseName(schemaTableName.getSchemaName())
                .withName(schemaTableName.getTableName());
        Table currentTable;
        try {
            currentTable = glueClient.getTable(getTableRequest).getTable();
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(schemaTableName);
        }
        String glueVersionId = currentTable.getVersionId();

        TableInput tableInput = convertGlueTableToTableInput(currentTable);
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        parameters.putAll(getTableParameters(currentTable));
        parameters.putAll(tableMetadataParameters(version, schemaString, tableComment));
        tableInput.withParameters(parameters.buildKeepingLast());

        UpdateTableRequest updateTableRequest = new UpdateTableRequest()
                .withDatabaseName(schemaTableName.getSchemaName())
                .withTableInput(tableInput)
                .withVersionId(glueVersionId);
        stats.getUpdateTable().call(() -> glueClient.updateTable(updateTableRequest));
    }
}
