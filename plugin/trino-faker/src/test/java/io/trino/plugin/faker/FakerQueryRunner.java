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
package io.trino.plugin.faker;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.File;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNullElse;

public class FakerQueryRunner
{
    private static final String CATALOG = "faker";

    private FakerQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private Map<String, String> properties = ImmutableMap.of();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema("default")
                    .build());
        }

        public Builder setFakerProperties(Map<String, String> properties)
        {
            this.properties = ImmutableMap.copyOf(properties);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            setWorkerCount(0);
            DistributedQueryRunner queryRunner = super.build();

            try {
                queryRunner.installPlugin(new FakerPlugin());
                queryRunner.createCatalog(CATALOG, "faker", properties);

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("io.trino", Level.INFO);

        QueryRunner queryRunner = builder()
                .setExtraProperties(Map.of(
                        "http-server.http.port", requireNonNullElse(System.getenv("TRINO_PORT"), "8080")))
                .build();

        Logger log = Logger.get(FakerQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static final class FakerQueryRunnerWithTaskRetries
    {
        private FakerQueryRunnerWithTaskRetries() {}

        public static void main(String[] args)
                throws Exception
        {
            Logger log = Logger.get(FakerQueryRunnerWithTaskRetries.class);

            File exchangeManagerDirectory = createTempDirectory("exchange_manager").toFile();
            Map<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                    .put("exchange.base-directories", exchangeManagerDirectory.getAbsolutePath())
                    .buildOrThrow();
            exchangeManagerDirectory.deleteOnExit();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = builder()
                    .setExtraProperties(ImmutableMap.<String, String>builder()
                            .put("http-server.http.port", requireNonNullElse(System.getenv("TRINO_PORT"), "8080"))
                            .put("retry-policy", "TASK")
                            .put("fault-tolerant-execution-task-memory", "1GB")
                            .buildOrThrow())
                    .setAdditionalSetup(runner -> {
                        runner.installPlugin(new FileSystemExchangePlugin());
                        runner.loadExchangeManager("filesystem", exchangeManagerProperties);
                    })
                    .build();

            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
