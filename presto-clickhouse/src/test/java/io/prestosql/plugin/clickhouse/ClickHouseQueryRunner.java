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
package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tpch.TpchTable.LINE_ITEM;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.PART_SUPPLIER;
import static java.lang.String.format;

public final class ClickHouseQueryRunner
{
    private ClickHouseQueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";
    private static final Logger LOG = Logger.get(ClickHouseQueryRunner.class);

    public static QueryRunner createClickHouseQueryRunner(TestingClickHouseServer server, TpchTable<?>... tables)
            throws Exception
    {
        return createClickHouseQueryRunner(server, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createClickHouseQueryRunner(
            TestingClickHouseServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            // connectorProperties.putIfAbsent("allow-drop-table", "true");

            queryRunner.installPlugin(new ClickHousePlugin());
            queryRunner.createCatalog("clickhouse", "clickhouse", connectorProperties);

            if (!server.isTpchLoaded()) {
                server.execute("CREATE database " + TPCH_SCHEMA);
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
                server.setTpchLoaded();
            }
            else {
                server.waitTpchLoaded();
            }

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
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
//        String sql = format("CREATE TABLE %s %s AS SELECT * FROM %s", target, tableProperties, source);
        String sql = format("create table %s(%s) ENGINE=Log", target, table.getColumns());
        LOG.info("Running import for %s %s", target, sql);
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        LOG.info("%s rows loaded into %s", rows, target);
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("clickhouse")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = createClickHouseQueryRunner(
                new TestingClickHouseServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());

        Logger log = Logger.get(ClickHouseQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
