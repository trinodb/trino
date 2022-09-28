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

import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.BaseHiveConnectorTest;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_PARTITION_COUNT;
import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.FaultTolerantExecutionConnectorTestHelper.getExtraProperties;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveFaultTolerantExecutionConnectorTest
        extends BaseHiveConnectorTest
{
    private MinioStorage minioStorage;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomTableSuffix());
        minioStorage.start();

        return BaseHiveConnectorTest.createHiveQueryRunner(
                getExtraProperties(),
                runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                });
    }

    @Override
    public void testScaleWriters()
    {
        testWithAllStorageFormats(this::testSingleWriter);
    }

    // We need to override this method because in the case of pipeline execution,
    // the default number of writers are equal to worker count. Whereas, in the
    // fault-tolerant execution, it starts with 1.
    @Override
    public void testTaskWritersDoesNotScaleWithLargeMinWriterSize()
    {
        testTaskScaleWriters(getSession(), DataSize.of(2, GIGABYTE), 4, false).isEqualTo(1);
    }

    @Override
    public void testWritersAcrossMultipleWorkersWhenScaleWritersIsEnabled()
    {
        // Not applicable for fault-tolerant mode.
    }

    @Override
    public void testTargetMaxFileSize()
    {
        testTargetMaxFileSize(9);
    }

    @Override
    public void testTargetMaxFileSizePartitioned()
    {
        testTargetMaxFileSizePartitioned(9);
    }

    @Override
    public void testOptimize()
    {
        assertThatThrownBy(super::testOptimize)
                .hasMessageContaining("OPTIMIZE procedure is not supported with query retries enabled");
    }

    @Override
    public void testOptimizeWithWriterScaling()
    {
        assertThatThrownBy(super::testOptimizeWithWriterScaling)
                .hasMessageContaining("OPTIMIZE procedure is not supported with query retries enabled");
    }

    @Override
    public void testOptimizeWithPartitioning()
    {
        assertThatThrownBy(super::testOptimizeWithPartitioning)
                .hasMessageContaining("OPTIMIZE procedure is not supported with query retries enabled");
    }

    @Override
    public void testOptimizeWithBucketing()
    {
        assertThatThrownBy(super::testOptimizeWithBucketing)
                .hasMessageContaining("OPTIMIZE procedure is not supported with query retries enabled");
    }

    @Test
    public void testMaxOutputPartitionCountCheck()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_PARTITION_COUNT, "51")
                .build();
        assertQueryFails(session, "SELECT nationkey, count(*) FROM nation GROUP BY nationkey", "Max number of output partitions exceeded for exchange.*");
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
