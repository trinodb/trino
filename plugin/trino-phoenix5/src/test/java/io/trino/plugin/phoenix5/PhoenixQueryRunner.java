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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART_SUPPLIER;
import static java.lang.String.format;

public final class PhoenixQueryRunner
{
    private static final Logger LOG = Logger.get(PhoenixQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    private PhoenixQueryRunner()
    {
    }

    public static DistributedQueryRunner createPhoenixQueryRunner(TestingPhoenixServer server, Map<String, String> extraProperties, List<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("phoenix.connection-url", server.getJdbcUrl())
                .put("case-insensitive-name-matching", "true")
                .buildOrThrow();

        queryRunner.installPlugin(new PhoenixPlugin());
        queryRunner.createCatalog("phoenix", "phoenix5", properties);

        createSchema(server, TPCH_SCHEMA);
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

        return queryRunner;
    }

    private static void createSchema(TestingPhoenixServer phoenixServer, String schema)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
        try (Connection connection = DriverManager.getConnection(phoenixServer.getJdbcUrl(), properties);
                Statement statement = connection.createStatement()) {
            statement.execute(format("CREATE SCHEMA IF NOT EXISTS %s", schema));
        }
    }

    private static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        LOG.debug("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, session, sourceSchema, table);
        }
    }

    private static void copyTable(
            QueryRunner queryRunner,
            String catalog,
            Session session,
            String schema,
            TpchTable<?> table)
    {
        QualifiedObjectName source = new QualifiedObjectName(catalog, schema, table.getTableName());
        String target = table.getTableName();
        String tableProperties = "";
        if (LINE_ITEM.getTableName().equals(target)) {
            tableProperties = "WITH (ROWKEYS = 'ORDERKEY,LINENUMBER', SALT_BUCKETS=10)";
        }
        else if (ORDERS.getTableName().equals(target)) {
            tableProperties = "WITH (SALT_BUCKETS=10)";
        }
        else if (PART_SUPPLIER.getTableName().equals(target)) {
            tableProperties = "WITH (ROWKEYS = 'PARTKEY,SUPPKEY')";
        }
        @Language("SQL")
        String sql = format("CREATE TABLE IF NOT EXISTS %s %s AS SELECT * FROM %s", target, tableProperties, source);

        LOG.debug("Running import for %s %s", target, sql);
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        LOG.debug("%s rows loaded into %s", rows, target);
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("phoenix")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createPhoenixQueryRunner(
                TestingPhoenixServer.getInstance().get(),
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());

        Logger log = Logger.get(PhoenixQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
