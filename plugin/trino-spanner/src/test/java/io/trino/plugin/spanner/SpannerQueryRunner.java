package io.trino.plugin.spanner;/*
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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class SpannerQueryRunner
{
    private static final Logger LOG = Logger.get(SpannerQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    private SpannerQueryRunner() {}

    public static DistributedQueryRunner createSpannerQueryRunner(
            TestingSpannerInstance instance,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables, boolean addTpcDsTables)
            throws Exception
    {
        return createSpannerQueryRunner(instance, extraProperties, ImmutableMap.of(), connectorProperties, tables, runner -> {}, addTpcDsTables);
    }

    public static DistributedQueryRunner createSpannerQueryRunner(
            TestingSpannerInstance instance,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> moreSetup,
            boolean addTpcDsTables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(
                            createSession())
                    .setExtraProperties(extraProperties)
                    .setCoordinatorProperties(coordinatorProperties)
                    .setAdditionalSetup(moreSetup)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            // note: additional copy via ImmutableList so that if fails on nulls
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("spanner.credentials.file", "credentials.json");
            connectorProperties.putIfAbsent("spanner.instanceId", instance.getInstanceId());
            connectorProperties.putIfAbsent("spanner.projectId", instance.getProjectId());
            connectorProperties.putIfAbsent("spanner.database", instance.getDatabaseId());
            connectorProperties.putIfAbsent("spanner.emulated", "true");
            connectorProperties.putIfAbsent("spanner.emulated.host", instance.getHost());
           /* connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("spanner.credentials.file", "credentials.json");
            connectorProperties.putIfAbsent("spanner.instanceId", "spanner-instance");
            connectorProperties.putIfAbsent("spanner.projectId", "spanner-project");
            connectorProperties.putIfAbsent("spanner.database", "spanner-database");
            connectorProperties.putIfAbsent("spanner.emulated", "true");
            connectorProperties.putIfAbsent("spanner.emulated.host", "localhost:9010");*/
            queryRunner.installPlugin(new SpannerPlugin());
            queryRunner.createCatalog("spanner", "spanner", connectorProperties);
            if (addTpcDsTables) {
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
                MaterializedResult execute = queryRunner.execute("SHOW TABLES FROM spanner.default");
                System.out.println(execute);
            }

/*
            MaterializedResult rows = queryRunner.execute("SELECT * FROM spanner.default.customer");
            System.out.println(rows);
*/
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, instance);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("spanner")
                .setSchema("default")
                .setCatalogSessionProperty("spanner",
                        SpannerSessionProperties.WRITE_MODE,
                        SpannerSessionProperties.Mode.UPSERT.name())
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = getSpannerQueryRunner();

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");

        Logger log = Logger.get(SpannerQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    @NotNull
    public static DistributedQueryRunner getSpannerQueryRunner()
            throws Exception
    {
        return createSpannerQueryRunner(
                new TestingSpannerInstance(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables(), false);
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
        String primaryKey = table.getColumns().get(0).getSimplifiedColumnName();
        String tableProperties = String.format("WITH (PRIMARY_KEYS = ARRAY['%s'])", primaryKey);
        @Language("SQL")
        String sql = format("CREATE TABLE IF NOT EXISTS %s %s AS SELECT * FROM %s",
                target, tableProperties, source, primaryKey, primaryKey, source);
        System.out.println(sql);
        LOG.debug("Running import for %s %s", target, sql);
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();

        LOG.debug("%s rows loaded into %s", rows, target);
    }
}
