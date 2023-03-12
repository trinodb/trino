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
package io.trino.plugin.raptor.legacy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.raptor.legacy.storage.StorageManagerConfig;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class RaptorQueryRunner
{
    private static final Logger log = Logger.get(RaptorQueryRunner.class);

    private RaptorQueryRunner() {}

    public static DistributedQueryRunner createRaptorQueryRunner(
            Map<String, String> extraProperties,
            List<TpchTable<?>> tablesToLoad,
            boolean bucketed,
            Map<String, String> extraRaptorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession("tpch"))
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new RaptorPlugin());
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();
        Map<String, String> raptorProperties = ImmutableMap.<String, String>builder()
                .putAll(extraRaptorProperties)
                .put("metadata.db.type", "h2")
                .put("metadata.db.filename", new File(baseDir, "db").getAbsolutePath())
                .put("storage.data-directory", new File(baseDir, "data").getAbsolutePath())
                .put("storage.max-shard-rows", "2000")
                .put("backup.provider", "file")
                .put("backup.directory", new File(baseDir, "backup").getAbsolutePath())
                .buildOrThrow();

        queryRunner.createCatalog("raptor", "raptor_legacy", raptorProperties);

        copyTables(queryRunner, "tpch", createSession(), bucketed, tablesToLoad);

        return queryRunner;
    }

    public static void copyTables(QueryRunner queryRunner, String catalog, Session session, boolean bucketed, List<TpchTable<?>> tables)
    {
        String schema = TINY_SCHEMA_NAME;
        if (!bucketed) {
            copyTpchTables(queryRunner, catalog, schema, session, tables);
            return;
        }

        ImmutableMap.Builder<TpchTable<?>, String> tablesMapBuilder = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            if (table.equals(TpchTable.ORDERS)) {
                tablesMapBuilder.put(TpchTable.ORDERS, "bucket_count = 25, bucketed_on = ARRAY['orderkey'], distribution_name = 'order'");
            }
            else if (table.equals(TpchTable.LINE_ITEM)) {
                tablesMapBuilder.put(TpchTable.LINE_ITEM, "bucket_count = 25, bucketed_on = ARRAY['orderkey'], distribution_name = 'order'");
            }
            else if (table.equals(TpchTable.PART)) {
                tablesMapBuilder.put(TpchTable.PART, "bucket_count = 20, bucketed_on = ARRAY['partkey'], distribution_name = 'part'");
            }
            else if (table.equals(TpchTable.PART_SUPPLIER)) {
                tablesMapBuilder.put(TpchTable.PART_SUPPLIER, "bucket_count = 20, bucketed_on = ARRAY['partkey'], distribution_name = 'part'");
            }
            else if (table.equals(TpchTable.SUPPLIER)) {
                tablesMapBuilder.put(TpchTable.SUPPLIER, "bucket_count = 10, bucketed_on = ARRAY['suppkey']");
            }
            else if (table.equals(TpchTable.CUSTOMER)) {
                tablesMapBuilder.put(TpchTable.CUSTOMER, "bucket_count = 10, bucketed_on = ARRAY['custkey']");
            }
            else if (table.equals(TpchTable.NATION)) {
                tablesMapBuilder.put(TpchTable.NATION, "");
            }
            else if (table.equals(TpchTable.REGION)) {
                tablesMapBuilder.put(TpchTable.REGION, "");
            }
            else {
                throw new IllegalArgumentException("Unsupported table: " + table);
            }
        }
        Map<TpchTable<?>, String> tablesMap = tablesMapBuilder.buildOrThrow();

        log.info("Loading data from %s.%s...", catalog, schema);
        long startTime = System.nanoTime();
        for (Entry<TpchTable<?>, String> entry : tablesMap.entrySet()) {
            copyTable(queryRunner, catalog, session, schema, entry.getKey(), entry.getValue());
        }
        log.info("Loading from %s.%s complete in %s", catalog, schema, nanosSince(startTime));
    }

    private static void copyTable(QueryRunner queryRunner, String catalog, Session session, String schema, TpchTable<?> table, String properties)
    {
        QualifiedObjectName source = new QualifiedObjectName(catalog, schema, table.getTableName());
        String target = table.getTableName();

        String with = properties.isEmpty() ? "" : format(" WITH (%s)", properties);
        @Language("SQL") String sql = format("CREATE TABLE %s%s AS SELECT * FROM %s", target, with, source);

        log.info("Running import for %s", target);
        long start = System.nanoTime();
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        log.info("Imported %s rows for %s in %s", rows, target, nanosSince(start));
    }

    public static Session createSession()
    {
        return createSession("tpch");
    }

    public static Session createSession(String schema)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(
                ImmutableSet.of(new SystemSessionProperties()),
                CatalogServiceProvider.singleton(
                        createTestCatalogHandle("raptor"),
                        Maps.uniqueIndex(new RaptorSessionProperties(new StorageManagerConfig()).getSessionProperties(), PropertyMetadata::getName)));
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog("raptor")
                .setSchema(schema)
                .setSystemProperty("enable_intermediate_aggregations", "true")
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = createRaptorQueryRunner(properties, ImmutableList.of(), false, ImmutableMap.of());
        Thread.sleep(10);
        Logger log = Logger.get(RaptorQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
