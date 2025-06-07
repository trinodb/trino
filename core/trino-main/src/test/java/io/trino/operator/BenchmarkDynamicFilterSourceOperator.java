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
import io.airlift.units.DataSize;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.DynamicFilterSourceConsumer;
import io.trino.sql.planner.DynamicFilterTupleDomain;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemGenerator;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(3)
@Warmup(iterations = 15, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDynamicFilterSourceOperator
{
    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        @Param("4096")
        private int positionsPerPage = 4096;

        @Param("600572")
        private int maxDistinctValuesCount = 600572;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            pages = createInputPages(positionsPerPage);

            TypeOperators typeOperators = new TypeOperators();
            operatorFactory = new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                    1,
                    new PlanNodeId("joinNodeId"),
                    new DynamicFilterSourceConsumer() {
                        @Override
                        public void addPartition(DynamicFilterTupleDomain<DynamicFilterId> tupleDomain) {}

                        @Override
                        public void setPartitionCount(int partitionCount)
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public boolean isDomainCollectionComplete()
                        {
                            return false;
                        }
                    },
                    ImmutableList.of(new DynamicFilterSourceOperator.Channel(new DynamicFilterId("0"), BIGINT, 0)),
                    maxDistinctValuesCount,
                    maxDistinctValuesCount * 2,
                    DataSize.ofBytes(Long.MAX_VALUE),
                    typeOperators);
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.of(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        private static List<Page> createInputPages(int positionsPerPage)
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
            LineItemGenerator lineItemGenerator = new LineItemGenerator(0.1, 1, 1);
            for (LineItem lineItem : lineItemGenerator) {
                pageBuilder.declarePosition();
                BIGINT.writeLong(pageBuilder.getBlockBuilder(0), lineItem.orderKey());

                if (pageBuilder.getPositionCount() == positionsPerPage) {
                    pages.add(pageBuilder.build());
                    pageBuilder.reset();
                }
            }

            if (pageBuilder.getPositionCount() > 0) {
                pages.add(pageBuilder.build());
            }
            return pages.build();
        }
    }

    @Benchmark
    public List<Page> dynamicFilterCollect(BenchmarkContext context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator operator = context.getOperatorFactory().createOperator(driverContext);

        Iterator<Page> input = context.getPages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !operator.isFinished() && loops < 1_000_000; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    operator.addInput(inputPage);
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }

    @Test
    public void testBenchmark()
    {
        BenchmarkContext context = new BenchmarkContext();
        context.setup();

        List<Page> outputPages = dynamicFilterCollect(context);
        assertThat(600572).isEqualTo(outputPages.stream().mapToInt(Page::getPositionCount).sum());

        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkDynamicFilterSourceOperator.class).run();
    }
}
