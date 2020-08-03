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
import io.prestosql.tpch.TpchColumn;
import io.prestosql.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
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
                URL url = ClickHouseQueryRunner.class.getResource("/tpch.sql");
                File file = new File(url.toURI());
                List<String> ddl = Files.readAllLines(file.toPath());
                for (String s : ddl) {
                    server.execute(s);
                }
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
        StringBuilder instCols = new StringBuilder();
        for (TpchColumn<?> col : table.getColumns()) {
            instCols.append(col.getSimplifiedColumnName() + ",");
        }
        instCols.deleteCharAt(instCols.lastIndexOf((",")));
        @Language("SQL")
        String sql = format("INSERT INTO %s(%s) SELECT %s FROM %s", target, instCols.toString(), genQuerySql(table), source);
        LOG.info("Running import for %s %s", target, sql);
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        LOG.info("%s rows loaded into %s", rows, target);
    }

    private static String genQuerySql(TpchTable<?> table)
    {
        List<? extends TpchColumn<?>> cols = table.getColumns();
        StringBuilder querySql = new StringBuilder();
        for (TpchColumn<?> col : cols) {
            if (col.getType().getBase().toString().equals("VARCHAR")
                    | col.getType().getBase().toString().equals("DATE")
                    | col.getType().getBase().toString().equals("DATETIME")) {
                querySql.append(String.format("cast(%s as varchar) as %s", col.getSimplifiedColumnName(), col.getSimplifiedColumnName()));
            }
            else {
                querySql.append(col.getSimplifiedColumnName());
            }
            querySql.append(",");
        }
        querySql.deleteCharAt(querySql.lastIndexOf(","));
        return querySql.toString();
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
