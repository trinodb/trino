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
package io.trino.plugin.deltalake.metastore.glue.v2;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import java.util.Optional;

import static io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.tableMetadataParameters;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueInputConverter.convertGlueTableToTableInput;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.getTableParameters;
import static java.util.Objects.requireNonNull;

public class DeltaLakeGlueV2MetastoreTableOperations
        implements DeltaLakeTableOperations
{
    private final GlueClient glueClient;

    public DeltaLakeGlueV2MetastoreTableOperations(GlueClient glueClient, GlueMetastoreStats stats)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
    }

    @Override
    public void commitToExistingTable(SchemaTableName schemaTableName, long version, String schemaString, Optional<String> tableComment)
    {
        GetTableRequest getTableRequest = GetTableRequest.builder()
                .databaseName(schemaTableName.getSchemaName())
                .name(schemaTableName.getTableName())
                .build();
        Table currentTable;
        try {
            currentTable = glueClient.getTable(getTableRequest).table();
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(schemaTableName);
        }
        String glueVersionId = currentTable.versionId();

        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        parameters.putAll(getTableParameters(currentTable));
        parameters.putAll(tableMetadataParameters(version, schemaString, tableComment));

        TableInput tableInput = convertGlueTableToTableInput(currentTable)
                .toBuilder()
                .parameters(parameters.buildKeepingLast())
                .build();

        UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
                .databaseName(schemaTableName.getSchemaName())
                .tableInput(tableInput)
                .versionId(glueVersionId)
                .build();

        glueClient.updateTable(updateTableRequest);
    }
}
