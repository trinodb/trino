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
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Runtime.getRuntime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated
public class TestTaskSplitting
        extends AbstractTestQueryFramework
{
    public static final long HEAP_SIZE = getRuntime().maxMemory();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());

        // limit size of memory assigned to worker nodes to 100MB
        long workerMemory = 100 * DataSize.Unit.MEGABYTE.inBytes();
        extraProperties.put("memory.heap-headroom-per-node", HEAP_SIZE - workerMemory + "B");
        extraProperties.put("query.max-memory-per-node", workerMemory + "B");

        // ensure we start with just one task for hash partitioned stage
        extraProperties.put("fault-tolerant-execution-hash-distribution-compute-task-target-size", "100GB");
        extraProperties.put("fault-tolerant-execution-hash-distribution-compute-task-to-node-min-ratio", "0");

        // set task splitting threshold low
        extraProperties.put("fault-tolerant-execution-task-split-memory-threshold", "50MB");

        // allow more partitions
        extraProperties.put("fault-tolerant-execution-max-partition-count", "50");

        return TpchQueryRunnerBuilder.builder()
                .setExtraProperties(extraProperties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", ImmutableMap.of("exchange.base-directories",
                                    System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
                })
                .withPartitioningEnabled(false)
                .build();
    }

    @Test
    public void testTaskSplitting()
    {
        String testQuery = """
           SELECT * FROM
             tpch.sf1.lineitem l1,
             tpch.sf1.lineitem l2
           WHERE
             l1.orderkey = l2.orderkey and l1.linenumber = l2.linenumber
             AND l1.partkey * l2.partkey = 0""";

        assertThat(query(testQuery))
                .returnsEmptyResult();

        Session taskSplittingDisabled = Session.builder(getSession())
                .setSystemProperty("fault_tolerant_execution_task_split_memory_threshold", "100GB")
                .build();
        assertThatThrownBy(() -> query(taskSplittingDisabled, testQuery))
                .hasMessageContaining("Cannot allocate enough memory for task");
    }
}
