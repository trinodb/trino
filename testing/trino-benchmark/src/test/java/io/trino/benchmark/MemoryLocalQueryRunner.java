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
package io.trino.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStateMachine;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.Driver;
import io.trino.operator.TaskContext;
import io.trino.plugin.memory.MemoryConnectorFactory;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.PageConsumerOperator;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertTrue;

public class MemoryLocalQueryRunner
        implements AutoCloseable
{
    protected final LocalQueryRunner localQueryRunner;

    public MemoryLocalQueryRunner()
    {
        this(ImmutableMap.of());
    }

    public MemoryLocalQueryRunner(Map<String, String> properties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default");
        properties.forEach(sessionBuilder::setSystemProperty);

        localQueryRunner = createMemoryLocalQueryRunner(sessionBuilder.build());
    }

    public List<Page> execute(@Language("SQL") String query)
    {
        MemoryPool memoryPool = new MemoryPool(DataSize.of(2, GIGABYTE));
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(DataSize.of(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(
                new QueryId("test"),
                DataSize.of(1, GIGABYTE),
                Optional.empty(),
                memoryPool,
                new TestingGcMonitor(),
                localQueryRunner.getExecutor(),
                localQueryRunner.getScheduler(),
                DataSize.of(4, GIGABYTE),
                spillSpaceTracker);

        TaskContext taskContext = queryContext
                .addTaskContext(new TaskStateMachine(new TaskId(new StageId("query", 0), 0, 0), localQueryRunner.getExecutor()),
                        localQueryRunner.getDefaultSession(),
                        () -> {},
                        false,
                        false);

        // Use NullOutputFactory to avoid coping out results to avoid affecting benchmark results
        ImmutableList.Builder<Page> output = ImmutableList.builder();
        List<Driver> drivers = localQueryRunner.createDrivers(
                query,
                new PageConsumerOperator.PageConsumerOutputFactory(types -> output::add),
                taskContext);

        boolean done = false;
        while (!done) {
            boolean processed = false;
            for (Driver driver : drivers) {
                if (!driver.isFinished()) {
                    driver.process();
                    processed = true;
                }
            }
            done = !processed;
        }

        return output.build();
    }

    private static LocalQueryRunner createMemoryLocalQueryRunner(Session session)
    {
        LocalQueryRunner localQueryRunner = LocalQueryRunner.builder(session)
                .withInitialTransaction()
                .build();

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        localQueryRunner.createCatalog(
                "memory",
                new MemoryConnectorFactory(),
                ImmutableMap.of("memory.max-data-per-node", "4GB"));

        return localQueryRunner;
    }

    public void dropTable(String tableName)
    {
        Session session = localQueryRunner.getDefaultSession();
        Metadata metadata = localQueryRunner.getMetadata();
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, QualifiedObjectName.valueOf(tableName));
        assertTrue(tableHandle.isPresent(), "Table " + tableName + " does not exist");
        metadata.dropTable(session, tableHandle.get());
    }

    @Override
    public void close()
    {
        localQueryRunner.close();
    }
}
