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
package io.trino.plugin.iceberg;

import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.FileSystemExchangePlugin;
import io.trino.plugin.exchange.containers.MinioStorage;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.exchange.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.sql.TestTable.randomTableSuffix;

public class TestIcebergQueryFailureRecoveryTest
        extends BaseIcebergFailureRecoveryTest
{
    private MinioStorage minioStorage;

    protected TestIcebergQueryFailureRecoveryTest()
    {
        super(RetryPolicy.QUERY);
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomTableSuffix());
        minioStorage.start();

        return IcebergQueryRunner.builder()
                .setInitialTables(requiredTpchTables)
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(configProperties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                })
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        if (minioStorage != null) {
            minioStorage.close();
            minioStorage = null;
        }
    }
}
