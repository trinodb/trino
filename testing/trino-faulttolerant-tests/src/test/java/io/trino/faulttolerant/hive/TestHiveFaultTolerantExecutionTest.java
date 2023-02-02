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
package io.trino.faulttolerant.hive;

import io.trino.Session;
import io.trino.faulttolerant.BaseFaultTolerantExecutionTest;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestHiveFaultTolerantExecutionTest
        extends BaseFaultTolerantExecutionTest
{
    public TestHiveFaultTolerantExecutionTest()
    {
        super("partitioned_by");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MinioStorage minioStorage = closeAfterClass(new MinioStorage("test-exchange-spooling-" + randomNameSuffix()));
        minioStorage.start();

        return HiveQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                })
                .build();
    }

    @Override
    protected Session getSession()
    {
        Session session = super.getSession();
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "non_transactional_optimize_enabled", "true")
                .build();
    }
}
