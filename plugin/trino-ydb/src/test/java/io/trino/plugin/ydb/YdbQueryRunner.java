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
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.ydb.YdbColumnMappings.YDB_DATE32_SQL_TYPE;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

public final class YdbQueryRunner
{
    static {
        Logging logging = Logging.initialize();
        logging.setLevel("tech.ydb", Level.OFF);
    }

    public static final String DEFAULT_SCHEMA = YdbClient.DEFAULT_SCHEMA;

    private YdbQueryRunner() {}

    public static Builder builder(YdbHelperExtension ydb)
    {
        return new Builder(buildJdbcUrl(ydb));
    }

    static String buildJdbcUrl(YdbHelperExtension ydb)
    {
        return "jdbc:ydb:grpc://" + ydb.endpoint() + ydb.database()
                + "?useQueryService=true&sessionPoolMaxSize=10&useDiscovery=false";
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final String jdbcUrl;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(String jdbcUrl)
        {
            super(testSessionBuilder()
                    .setCatalog("ydb")
                    .setSchema(DEFAULT_SCHEMA)
                    .build());
            this.jdbcUrl = jdbcUrl;
            connectorProperties.put("connection-url", jdbcUrl);
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

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");
                queryRunner.installPlugin(new YdbPlugin());
                queryRunner.createCatalog("ydb", "ydb", ImmutableMap.copyOf(connectorProperties));

                try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
                    for (TpchTable<?> table : initialTables) {
                        loadTable(queryRunner, connection, table);
                    }
                }
                return queryRunner;
            }
            catch (Throwable failure) {
                queryRunner.close();
                throw failure;
            }
        }

        private static void loadTable(DistributedQueryRunner queryRunner, Connection connection, TpchTable<?> table)
                throws Exception
        {
            String tableName = table.getTableName();
            MaterializedResult rows = queryRunner.execute("SELECT * FROM tpch.tiny." + tableName).toTestTypes();
            List<String> columns = table.getColumns().stream()
                    .map(column -> column.getColumnName().substring(2))
                    .toList();
            String declarations = range(0, columns.size())
                    .mapToObj(index -> "`" + columns.get(index) + "` " + YdbTypeUtils.toTypeHandle(rows.getTypes().get(index))
                            .orElseThrow().jdbcTypeName().orElseThrow())
                    .collect(joining(", "));
            String primaryKeys = switch (tableName) {
                case "region" -> "regionkey";
                case "nation" -> "nationkey";
                case "supplier" -> "suppkey";
                case "customer" -> "custkey";
                case "part" -> "partkey";
                case "partsupp" -> "partkey, suppkey";
                case "orders" -> "orderkey";
                case "lineitem" -> "orderkey, linenumber";
                default -> throw new IllegalArgumentException("Unsupported TPCH fixture table: " + tableName);
            };
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS `" + tableName + "`");
                statement.execute("CREATE TABLE `" + tableName + "` (" + declarations + ", PRIMARY KEY (" + primaryKeys + "))");
            }

            String insert = "UPSERT INTO `" + tableName + "` (" + String.join(", ", columns) + ") VALUES (" +
                    String.join(", ", nCopies(columns.size(), "?")) + ")";
            try (PreparedStatement statement = connection.prepareStatement(insert)) {
                int pendingRows = 0;
                for (MaterializedRow row : rows.getMaterializedRows()) {
                    for (int index = 0; index < columns.size(); index++) {
                        Object value = row.getField(index);
                        if (rows.getTypes().get(index).equals(DATE)) {
                            statement.setObject(index + 1, ((LocalDate) value).toEpochDay(), YDB_DATE32_SQL_TYPE);
                        }
                        else {
                            statement.setObject(index + 1, value);
                        }
                    }
                    statement.addBatch();
                    pendingRows++;
                    if (pendingRows == 1000) {
                        statement.executeBatch();
                        pendingRows = 0;
                    }
                }
                if (pendingRows > 0) {
                    statement.executeBatch();
                }
            }
        }
    }

    static void main()
            throws Exception
    {
        QueryRunner queryRunner = builder(new YdbHelperExtension().failIfUnavailable())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(YdbQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
