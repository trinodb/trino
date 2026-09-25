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
package io.trino.faulttolerant;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;

public final class FileSystemFteTpchQueryRunner
{
    private static final Logger log = Logger.get(FileSystemFteTpchQueryRunner.class);

    private FileSystemFteTpchQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private int workerCount = 2;

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build());
        }

        public Builder withWorkerCount(int workerCount)
        {
            this.workerCount = workerCount;
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            File spoolingDir = createTempDirectory("filesystem_fte_spooling").toFile();
            log.info("Exchange spooling directory: %s", spoolingDir.getAbsolutePath());

            Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());

            setExtraProperties(extraProperties);
            addCoordinatorProperty("node-scheduler.include-coordinator", "true");
            setWorkerCount(workerCount);
            withExchange("filesystem", ImmutableMap.of("exchange.base-directories", "file://" + spoolingDir.getAbsolutePath()));

            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
                return queryRunner;
            }
            catch (Exception e) {
                queryRunner.close();
                throw e;
            }
        }
    }

    static void main()
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("io.trino.plugin.exchange.filesystem", Level.DEBUG);
        logging.setLevel("io.trino.execution.scheduler.faulttolerant", Level.DEBUG);
        DistributedQueryRunner queryRunner = builder()
                .withWorkerCount(1)
                .build();
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
