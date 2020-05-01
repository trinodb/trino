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
package io.prestosql.plugin.noop;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.testing.DistributedQueryRunner;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class NoOpQueryRunner
{
    private static final Logger log = Logger.get(NoOpQueryRunner.class);

    private NoOpQueryRunner()
    {
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = DistributedQueryRunner
                .builder(testSessionBuilder().build())
                .build();

        try {
            queryRunner.installPlugin(new NoOpPlugin());
            queryRunner.createCatalog("noop", "noop", ImmutableMap.of());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createQueryRunner();
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
