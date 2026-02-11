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

public class DefaultTrinoGlueClient
        implements TrinoGlueClient
{
    private final GlueClient glueClient;
    private final GlueMetastoreStats stats;

    public DefaultTrinoGlueClient(GlueClient glueClient, GlueMetastoreStats stats)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Table getTable(SchemaTableName tableName)
    {
        return stats.getGetTable().call(() ->
                glueClient.getTable(request -> request
                                .databaseName(tableName.getSchemaName())
                                .name(tableName.getTableName()))
                        .table());
    }

    @Override
    public void deleteTable(String databaseName, String tableName)
    {
        stats.getDeleteTable().call(() ->
                glueClient.deleteTable(request -> request
                        .databaseName(databaseName)
                        .name(tableName)));
    }

    @Override
    public void updateTable(String databaseName, TableInput table, Optional<String> versionId)
    {
        stats.getUpdateTable().call(() ->
                glueClient.updateTable(request -> {
                    request.databaseName(databaseName)
                            .tableInput(table);
                    versionId.ifPresent(request::versionId);
                }));
    }

    @Override
    public void createTable(String databaseName, TableInput table)
    {
        stats.getCreateTable().call(() ->
                glueClient.createTable(request -> request
                        .databaseName(databaseName)
                        .tableInput(table)));
    }

    @Override
    public Stream<Table> streamTables(String databaseName)
    {
        return stats.getGetTables().call(() ->
                glueClient.getTablesPaginator(request -> request.databaseName(databaseName))
                        .stream()
                        .map(GetTablesResponse::tableList)
                        .flatMap(List::stream));
    }

    @Override
    public Database getDatabase(String databaseName)
    {
        return stats.getGetDatabase().call(() ->
                glueClient.getDatabase(request -> request.name(databaseName)).database());
    }

    @Override
    public List<String> listDatabases()
    {
        return stats.getGetDatabases().call(() ->
                glueClient.getDatabasesPaginator(_ -> {}).stream()
                        .map(GetDatabasesResponse::databaseList)
                        .flatMap(List::stream)
                        .map(Database::name)
                        .collect(toImmutableList()));
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        stats.getDeleteDatabase().call(() ->
                glueClient.deleteDatabase(request -> request.name(databaseName)));
    }

    @Override
    public void createDatabase(DatabaseInput database)
    {
        stats.getCreateDatabase().call(() ->
                glueClient.createDatabase(request -> request.databaseInput(database)));
    }
}
