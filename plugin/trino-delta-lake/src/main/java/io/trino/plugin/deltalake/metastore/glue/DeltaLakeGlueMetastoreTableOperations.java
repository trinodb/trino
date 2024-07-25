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
package io.trino.plugin.deltalake.metastore.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.hive.metastore.glue.GlueCache;
import io.trino.plugin.hive.metastore.glue.GlueContext;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.tableMetadataParameters;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.asTableInputBuilder;
import static java.util.Objects.requireNonNull;

public class DeltaLakeGlueMetastoreTableOperations
        implements DeltaLakeTableOperations
{
    private final GlueClient glueClient;
    private final GlueContext glueContext;
    private final GlueCache glueCache;
    private final GlueMetastoreStats stats;

    public DeltaLakeGlueMetastoreTableOperations(
            GlueClient glueClient,
            GlueContext glueContext,
            GlueCache glueCache,
            GlueMetastoreStats stats)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.glueContext = requireNonNull(glueContext, "glueContext is null");
        this.glueCache = requireNonNull(glueCache, "glueCache is null");
        this.stats = requireNonNull(stats, "stats is null");
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

        stats.getUpdateTable().call(() -> glueClient.updateTable(builder -> builder
                .applyMutation(glueContext::configureClient)
                .databaseName(schemaTableName.getSchemaName())
                .tableInput(convertGlueTableToTableInput(currentTable, version, schemaString, tableComment))
                .versionId(glueVersionId)));
        glueCache.invalidateTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), false);
    }

    private static TableInput convertGlueTableToTableInput(Table glueTable, long version, String schemaString, Optional<String> tableComment)
    {
        Map<String, String> parameters = ImmutableMap.<String, String>builder()
                .putAll(glueTable.parameters())
                .putAll(tableMetadataParameters(version, schemaString, tableComment))
                .buildKeepingLast();

        return asTableInputBuilder(glueTable)
                .parameters(parameters)
                .build();
    }
}
