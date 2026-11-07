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
package io.trino.plugin.iceberg.catalog.glue;

import io.trino.plugin.hive.metastore.glue.GlueCache;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.connector.SchemaTableName;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StatsRecordingGlueClient
{
    private final GlueClient glueClient;
    private final GlueCache glueCache;
    private final GlueMetastoreStats stats;

    public StatsRecordingGlueClient(GlueClient glueClient, GlueCache glueCache, GlueMetastoreStats stats)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.glueCache = requireNonNull(glueCache, "glueCache is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public Database getDatabase(String databaseName)
    {
        return stats.getGetDatabase().call(() ->
                glueClient.getDatabase(request -> request.name(databaseName)).database());
    }

    public List<String> listDatabases()
    {
        return stats.getGetDatabases().call(() ->
                glueClient.getDatabasesPaginator(_ -> {}).stream()
                        .map(GetDatabasesResponse::databaseList)
                        .flatMap(List::stream)
                        .map(Database::name)
                        .collect(toImmutableList()));
    }

    public void dropDatabase(String databaseName)
    {
        try {
            stats.getDeleteDatabase().call(() ->
                    glueClient.deleteDatabase(request -> request.name(databaseName)));
        }
        finally {
            glueCache.invalidateDatabase(databaseName);
            glueCache.invalidateDatabaseNames();
        }
    }

    public void createDatabase(DatabaseInput database)
    {
        try {
            stats.getCreateDatabase().call(() ->
                    glueClient.createDatabase(request -> request.databaseInput(database)));
        }
        finally {
            glueCache.invalidateDatabase(database.name());
            glueCache.invalidateDatabaseNames();
        }
    }

    public Table getTable(SchemaTableName tableName)
    {
        return stats.getGetTable().call(() ->
                glueClient
                        .getTable(request -> request
                                .databaseName(tableName.getSchemaName())
                                .name(tableName.getTableName()))
                        .table());
    }

    public void deleteTable(String databaseName, String tableName)
    {
        try {
            stats.getDeleteTable().call(() ->
                    glueClient.deleteTable(request -> request
                            .databaseName(databaseName)
                            .name(tableName)));
        }
        finally {
            glueCache.invalidateTable(databaseName, tableName, true);
            glueCache.invalidateTables(databaseName);
        }
    }

    public void updateTable(String databaseName, TableInput table, Optional<String> versionId)
    {
        try {
            stats.getUpdateTable().call(() ->
                    glueClient.updateTable(request -> {
                        request.databaseName(databaseName)
                                .tableInput(table);
                        versionId.ifPresent(request::versionId);
                    }));
        }
        finally {
            glueCache.invalidateTable(databaseName, table.name(), false);
        }
    }

    public void createTable(String databaseName, TableInput table)
    {
        try {
            stats.getCreateTable().call(() ->
                    glueClient.createTable(request -> request
                            .databaseName(databaseName)
                            .tableInput(table)));
        }
        finally {
            glueCache.invalidateTable(databaseName, table.name(), true);
            glueCache.invalidateTables(databaseName);
        }
    }

    public Stream<Table> streamTables(String databaseName)
    {
        return stats.getGetTables().call(() ->
                glueClient.getTablesPaginator(request -> request.databaseName(databaseName))
                        .stream()
                        .map(GetTablesResponse::tableList)
                        .flatMap(List::stream));
    }
}
