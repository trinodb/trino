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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;

public final class MemoryQueryRunner
{
    private static final String CATALOG = "memory";

    private MemoryQueryRunner() {}

    public static DistributedQueryRunner createMemoryQueryRunner(
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return builder()
                .setExtraProperties(extraProperties)
                .setInitialTables(tables)
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private ImmutableMap.Builder<String, String> memoryProperties = ImmutableMap.builder();

        protected Builder()
        {
            super(createSession());
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return self();
        }

        public Builder setMemoryProperties(Map<String, String> memoryProperties)
        {
            this.memoryProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(memoryProperties, "memoryProperties is null"));
            return self();
        }

        public Builder addMemoryProperty(String key, String value)
        {
            this.memoryProperties.put(key, value);
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();

            try {
                queryRunner.installPlugin(new MemoryPlugin());
                queryRunner.createCatalog(CATALOG, "memory", memoryProperties.buildOrThrow());

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), initialTables);

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }

        private static Session createSession()
        {
            return testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema("default")
                    .build();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createMemoryQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(MemoryQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static final class MemoryQueryRunnerWithTaskRetries
    {
        private MemoryQueryRunnerWithTaskRetries() {}

        public static void main(String[] args)
                throws Exception
        {
            Path exchangeManagerDirectory = createTempDirectory(null);
            ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                    .put("exchange.base-directories", exchangeManagerDirectory.toAbsolutePath().toString())
                    .buildOrThrow();

            DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                    .setExtraProperties(ImmutableMap.<String, String>builder()
                            .put("http-server.http.port", "8080")
                            .put("retry-policy", "TASK")
                            .put("fault-tolerant-execution-task-memory", "1GB")
                            .buildOrThrow())
                    .setAdditionalSetup(runner -> {
                        runner.installPlugin(new FileSystemExchangePlugin());
                        runner.loadExchangeManager("filesystem", exchangeManagerProperties);
                    })
                    .setInitialTables(TpchTable.getTables())
                    .build();
            Thread.sleep(10);
            Logger log = Logger.get(MemoryQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
