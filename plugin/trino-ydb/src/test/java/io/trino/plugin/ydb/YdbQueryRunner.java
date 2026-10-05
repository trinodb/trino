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
package io.trino.plugin.ydb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class YdbQueryRunner
{
    public static final String DEFAULT_SCHEMA = YdbClient.DEFAULT_SCHEMA;
    public static final String YDB_HIDDEN_PK_COLUMN = TestingYdbJdbcClient.YDB_HIDDEN_PK_COLUMN;

    private YdbQueryRunner() {}

    public static Builder builder(YdbHelperExtension ydb)
    {
        String jdbcUrl = buildJdbcUrl(ydb);
        return new Builder()
                // Transactional INSERT staging has dedicated coverage in TestYdbCreateTable.
                .addConnectorProperty("insert.non-transactional-insert.enabled", "true")
                .addConnectorProperty("merge.non-transactional-merge.enabled", "true")
                .addConnectorProperty("connection-url", jdbcUrl);
    }

    static String buildJdbcUrl(YdbHelperExtension ydb)
    {
        StringBuilder url = new StringBuilder("jdbc:ydb:");
        url.append(ydb.useTls() ? "grpcs://" : "grpc://");
        url.append(ydb.endpoint());
        url.append(ydb.database());
        url.append("?useQueryService=true&sessionPoolMaxSize=10");
        if (ydb.authToken() != null) {
            url.append("&token=").append(ydb.authToken());
        }
        return url.toString();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private boolean useTestingClient = true;
        private Module clientModule;

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("local")
                    .setSchema(DEFAULT_SCHEMA)
                    .build());
        }

        public Builder addConnectorProperty(String key, String value)
        {
            connectorProperties.put(key, value);
            return this;
        }

        public Builder setInitialTables(List<TpchTable<?>> tables)
        {
            this.initialTables = tables;
            return this;
        }

        public Builder useProductionClient()
        {
            this.useTestingClient = false;
            return this;
        }

        public Builder setClientModule(Module clientModule)
        {
            this.clientModule = clientModule;
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new YdbPlugin(clientModule != null
                        ? clientModule : useTestingClient ? new TestingYdbJdbcModule() : new YdbClientModule()));
                queryRunner.createCatalog("local", "ydb", ImmutableMap.copyOf(connectorProperties));

                for (TpchTable<?> table : initialTables) {
                    dropTable(queryRunner, table);
                }

                for (TpchTable<?> table : initialTables) {
                    createTableWithPk(queryRunner, table);
                }

                return queryRunner;
            }
            catch (Throwable e) {
                queryRunner.close();
                throw e;
            }
        }

        private static void createTableWithPk(DistributedQueryRunner queryRunner, TpchTable<?> table)
        {
            String tableName = table.getTableName();

            String columnNames = table.getColumns().stream()
                    .map(col -> col.getColumnName().substring(2))
                    .collect(Collectors.joining(", "));

            String createSql = String.format(
                    "CREATE TABLE %s AS SELECT %s, row_number() OVER () AS %s FROM tpch.tiny.%s",
                    tableName,
                    columnNames,
                    YDB_HIDDEN_PK_COLUMN,
                    tableName);

            queryRunner.execute(createSql);
        }

        private static void dropTable(DistributedQueryRunner queryRunner, TpchTable<?> table)
        {
            String tableName = table.getTableName();
            queryRunner.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
