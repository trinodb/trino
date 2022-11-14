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

import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestFaultTolerantExecutionOrderByQueries;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.getTables;

public class TestHiveFaultTolerantExecutionOrderByQueries
        extends AbstractTestFaultTolerantExecutionOrderByQueries
{
    private MinioStorage minioStorage;

    @Override
    protected QueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomNameSuffix());
        minioStorage.start();

        return HiveQueryRunner.builder()
                .setExtraProperties(extraProperties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                })
                .setInitialTables(getTables())
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

    @Test
    public void testLargeOrderBy()
    {
        // The file system exchange can break ordering only when a single writer generates more than a single file for a single output partition.
        // Order large dataset to cross the exchange.sink-max-file-size boundary and generate more than a single file.
        assertQueryOrdered("" +
                "SELECT " +
                "   *, " +
                "   comment || comment c0, " +
                "   comment || comment || comment c1, " +
                "   comment || comment || comment || comment c2, " +
                "   comment || comment || comment || comment c3, " +
                "   comment || comment || comment || comment c4 " +
                " FROM lineitem ORDER BY orderkey, suppkey, linenumber");
    }
}
