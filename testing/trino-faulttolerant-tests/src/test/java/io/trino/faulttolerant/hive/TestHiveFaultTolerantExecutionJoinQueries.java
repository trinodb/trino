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

import io.trino.execution.DynamicFilterConfig;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestFaultTolerantExecutionJoinQueries;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.getTables;

public class TestHiveFaultTolerantExecutionJoinQueries
        extends AbstractTestFaultTolerantExecutionJoinQueries
{
    private MinioStorage minioStorage;

    @Override
    protected QueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomNameSuffix());
        minioStorage.start();

        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");
        return HiveQueryRunner.builder()
                .setExtraProperties(extraProperties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                })
                .setInitialTables(getTables())
                .addHiveProperty("hive.dynamic-filtering.wait-timeout", "1h")
                .build();
    }

    @Test
    public void verifyDynamicFilteringEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'enable_dynamic_filtering'",
                "VALUES ('enable_dynamic_filtering', 'true', 'true', 'boolean', 'Enable dynamic filtering')");
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
