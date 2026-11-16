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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.Split;
import io.trino.operator.LeafTableFunctionOperator.LeafTableFunctionOperatorFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingSplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLeafTableFunctionOperator
{
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testProcessorClosedOnceWhenNextProcessorCreationFails()
            throws Exception
    {
        AtomicInteger created = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        TableFunctionProcessorProvider provider = new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionSplitProcessor getSplitProcessor(
                    ConnectorSession session,
                    ConnectorTableFunctionHandle handle,
                    Optional<ConnectorTableCredentials> tableCredentials,
                    ConnectorSplit split)
            {
                if (created.getAndIncrement() > 0) {
                    throw new IllegalStateException("cannot create processor");
                }
                return new TableFunctionSplitProcessor()
                {
                    @Override
                    public TableFunctionProcessorState process()
                    {
                        return FINISHED;
                    }

                    @Override
                    public void close()
                    {
                        closed.incrementAndGet();
                    }
                };
            }
        };

        SourceOperator operator = new LeafTableFunctionOperatorFactory(
                0,
                new PlanNodeId("test"),
                TEST_CATALOG_HANDLE,
                provider,
                new ConnectorTableFunctionHandle() {},
                Optional.empty(),
                ImmutableList.of(BIGINT))
                .createOperator(newDriverContext());
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        assertThat(operator.getOutput()).isNull();
        assertThat(closed.get()).isEqualTo(0);

        assertThatThrownBy(operator::getOutput)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("cannot create processor");
        assertThat(closed.get()).isEqualTo(1);

        operator.close();
        assertThat(closed.get()).isEqualTo(1);
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
