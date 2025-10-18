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
package io.trino.plugin.lance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.lance.catalog.BaseTable.LANCE_SUFFIX;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LanceQueryRunner
{
    public static final String LANCE_CATALOG = "lance";

    private static final Logger log = Logger.get(LanceQueryRunner.class);

    private LanceQueryRunner() {}

    public static Builder builder(String warehousePath)
    {
        return new Builder(warehousePath);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final String warehousePath;
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        protected Builder(String warehousePath)
        {
            super(testSessionBuilder().setCatalog(LANCE_CATALOG).build());
            this.warehousePath = requireNonNull(warehousePath, "warehousePath is null");
        }

        public Builder setInitialTables(List<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(initialTables);
            return this;
        }

        private static void loadTpchTable(TestingTrinoClient trinoClient, TpchTable<?> table, String warehousePath)
        {
            long start = System.nanoTime();
            Path tablePath = Paths.get(warehousePath, table.getTableName() + LANCE_SUFFIX);
            log.info("Running import for %s", table.getTableName());
            LanceLoader tpchLoader = new LanceLoader(trinoClient.getServer(), trinoClient.getDefaultSession(), tablePath.toString());
            tpchLoader.execute("SELECT * FROM tpch.tiny." + table.getTableName().toLowerCase(ENGLISH));
            log.info("Imported %s to %s in %s", table.getTableName(), tablePath, nanosSince(start).convertToMostSuccinctTimeUnit());
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();

            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");
                queryRunner.installPlugin(new LancePlugin());
                queryRunner.createCatalog(LANCE_CATALOG, "lance",
                        ImmutableMap.of("lance.namespace.type", "directory",
                                "lance.namespace.directory.warehouse.location", "local:///",
                                "fs.native-local.enabled", "true",
                                "local.location", warehousePath));

                TestingTrinoClient trinoClient = queryRunner.getClient();
                log.info("Loading data...");
                long startTime = System.nanoTime();
                for (TpchTable<?> table : initialTables) {
                    loadTpchTable(trinoClient, table, warehousePath);
                }
                log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    // LanceQueryRunner requires the following additional JVM options:
    //   - --add-opens=java.base/java.nio=ALL-UNNAMED
    //   - --sun-misc-unsafe-memory-access=allow
    public static void main(String[] args)
            throws Exception
    {
        Path warehousePath = createTempDirectory(null);
        QueryRunner queryRunner = LanceQueryRunner.builder(warehousePath.toString())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(LanceQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
