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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.BaseHiveConnectorTest;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT;
import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Isolated
@TestInstance(PER_CLASS)
public class TestHiveRuntimeAdaptivePartitioningFaultTolerantExecutionConnectorTest
        extends BaseHiveConnectorTest
{
    private MinioStorage minioStorage;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomNameSuffix());
        minioStorage.start();

        ImmutableMap.Builder<String, String> extraPropertiesWithRuntimeAdaptivePartitioning = ImmutableMap.builder();
        extraPropertiesWithRuntimeAdaptivePartitioning.putAll(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        extraPropertiesWithRuntimeAdaptivePartitioning.putAll(FaultTolerantExecutionConnectorTestHelper.enforceRuntimeAdaptivePartitioningProperties());

        return createHiveQueryRunner(HiveQueryRunner.builder()
                .setExtraProperties(extraPropertiesWithRuntimeAdaptivePartitioning.buildOrThrow())
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                }));
    }

    @Test
    @Override
    public void testMultipleWriters()
    {
        // Not applicable for fault-tolerant mode.
    }

    @Test
    @Override
    public void testMultipleWritersWithSkewedData()
    {
        // Not applicable for fault-tolerant mode.
    }

    // We need to override this method because in the case of pipeline execution,
    // the default number of writers are equal to worker count. Whereas, in the
    // fault-tolerant execution, it starts with 1.
    @Test
    @Override
    public void testTaskWritersDoesNotScaleWithLargeMinWriterSize()
    {
        testTaskScaleWriters(getSession(), DataSize.of(2, GIGABYTE), 4, DataSize.of(64, GIGABYTE))
                .isEqualTo(1);
    }

    @Test
    @Override
    public void testWriterTaskCountLimitUnpartitioned()
    {
        // Not applicable for fault-tolerant mode.
    }

    @Test
    @Override
    public void testWriterTaskCountLimitPartitionedScaleWritersDisabled()
    {
        // Not applicable for fault-tolerant mode.
    }

    @Test
    @Override
    public void testWriterTaskCountLimitPartitionedScaleWritersEnabled()
    {
        // Not applicable for fault-tolerant mode.
    }

    @Test
    public void testMaxOutputPartitionCountCheck()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "51")
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "51")
                .build();
        assertQueryFails(session, "SELECT nationkey, count(*) FROM nation GROUP BY nationkey", "Max number of output partitions exceeded for exchange.*");
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        if (minioStorage != null) {
            minioStorage.close();
            minioStorage = null;
        }
    }
}
