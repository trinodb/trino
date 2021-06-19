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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.memory.MemoryQueryRunner.createMemoryQueryRunner;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test(singleThreaded = true)
public class TestMemoryWorkerCrash
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMemoryQueryRunner(
                ImmutableMap.of(),
                ImmutableList.of(NATION));
    }

    @Test
    public void tableAccessAfterWorkerCrash()
            throws Exception
    {
        getQueryRunner().execute("CREATE TABLE test_nation as SELECT * FROM nation");
        assertQuery("SELECT * FROM test_nation ORDER BY nationkey", "SELECT * FROM nation ORDER BY nationkey");
        closeWorker();
        assertQueryFails("SELECT * FROM test_nation ORDER BY nationkey", "No nodes available to run query");
        getQueryRunner().execute("INSERT INTO test_nation SELECT * FROM tpch.tiny.nation");

        assertQueryFails("SELECT * FROM test_nation ORDER BY nationkey", "No nodes available to run query");

        getQueryRunner().execute("CREATE TABLE test_region as SELECT * FROM tpch.tiny.region");
        assertQuery("SELECT * FROM test_region ORDER BY regionkey", "SELECT * FROM region ORDER BY regionkey");
    }

    private void closeWorker()
            throws Exception
    {
        int nodeCount = getNodeCount();
        TestingTrinoServer worker = getDistributedQueryRunner().getServers().stream()
                .filter(server -> !server.isCoordinator())
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No worker nodes"));
        worker.close();
        waitForNodes(nodeCount - 1);
    }

    private void waitForNodes(int numberOfNodes)
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (getDistributedQueryRunner().getCoordinator().refreshNodes().getActiveNodes().size() < numberOfNodes) {
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
    }
}
