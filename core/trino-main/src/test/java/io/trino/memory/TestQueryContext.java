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
package io.trino.memory;

import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.LocalMemoryManager.GENERAL_POOL;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertTrue;

public class TestQueryContext
{
    private static final ScheduledExecutorService TEST_EXECUTOR = newScheduledThreadPool(1, threadsNamed("test-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        TEST_EXECUTOR.shutdownNow();
    }

    @Test
    public void testSetMemoryPool()
    {
        try (LocalQueryRunner localQueryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            QueryContext queryContext = new QueryContext(
                    new QueryId("query"),
                    DataSize.ofBytes(10),
                    Optional.empty(),
                    new MemoryPool(GENERAL_POOL, DataSize.ofBytes(10)),
                    new TestingGcMonitor(),
                    localQueryRunner.getExecutor(),
                    localQueryRunner.getScheduler(),
                    DataSize.ofBytes(0),
                    new SpillSpaceTracker(DataSize.ofBytes(0)));

            // Use memory
            queryContext.getQueryMemoryContext().initializeLocalMemoryContexts("test");
            LocalMemoryContext userMemoryContext = queryContext.getQueryMemoryContext().localUserMemoryContext();
            LocalMemoryContext revocableMemoryContext = queryContext.getQueryMemoryContext().localRevocableMemoryContext();
            assertTrue(userMemoryContext.setBytes(3).isDone());
            assertTrue(revocableMemoryContext.setBytes(5).isDone());

            // Free memory
            userMemoryContext.close();
            revocableMemoryContext.close();
        }
    }
}
