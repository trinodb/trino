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
package io.trino.operator.unnest;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.block.BlockAssertions;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.unnest.UnnestOperator.UnnestOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.unnest.BenchmarkUnnestOperator.BenchmarkContext.produceBlock;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(3)
@Warmup(iterations = 8, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkUnnestOperator
{
    private static final int TOTAL_POSITIONS = 10000;

    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        @Param("varchar")
        private String replicateType = "bigint";

        @Param({
                "array(varchar)",
                "map(varchar,varchar)",
                "array(row(varchar,varchar,varchar))",
                "array(array(varchar))",
                "array(varchar)|array(varchar)"
        })
        private String nestedType = "array(varchar)";

        @Param({"0.0", "0.05"})
        private float nullsRatio;

        @Param("1000")
        private int positionsPerPage = 1000;

        @Param("300")
        private int nestedLengths = 300;   // max entries in one nested structure (array, map)

        @Param("true")
        private boolean withOrdinality;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> replicatedTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> unnestTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Integer> replicatedChannelsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Integer> unnestChannelsBuilder = ImmutableList.builder();

            String[] replicatedTypes = replicateType.split("\\|");
            for (int i = 0; i < replicatedTypes.length; i++) {
                Type replicateType = getType(replicatedTypes[i]);
                typesBuilder.add(replicateType);
                replicatedTypesBuilder.add(replicateType);
                replicatedChannelsBuilder.add(i);
            }

            String[] unnestTypes = nestedType.split("\\|");
            for (int i = 0; i < unnestTypes.length; i++) {
                Type unnestType = getType(unnestTypes[i]);
                typesBuilder.add(unnestType);
                unnestTypesBuilder.add(unnestType);
                unnestChannelsBuilder.add(i + replicatedTypes.length);
            }

            this.pages = createInputPages(positionsPerPage, typesBuilder.build(), nullsRatio, nestedLengths);

            operatorFactory = new UnnestOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    replicatedChannelsBuilder.build(),
                    replicatedTypesBuilder.build(),
                    unnestChannelsBuilder.build(),
                    unnestTypesBuilder.build(),
                    withOrdinality,
                    false);
        }

        public static Type getType(String typeString)
        {
            return TESTING_TYPE_MANAGER.fromSqlType(typeString);
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

        static List<Page> createInputPages(
                int positionsPerPage,
                List<Type> types,
                float nullsRatio,
                int maxNestedCardinality)
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();
            int pageCount = TOTAL_POSITIONS / positionsPerPage;

            for (int i = 0; i < pageCount; i++) {
                Block[] blocks = new Block[types.size()];
                // Replicate block
                blocks[0] = produceBlock(types.get(0), positionsPerPage, 0.0f, maxNestedCardinality);
                // nested blocks
                for (int nestedColumn = 1; nestedColumn < blocks.length; nestedColumn++) {
                    blocks[nestedColumn] = produceBlock(types.get(nestedColumn), positionsPerPage, nullsRatio, maxNestedCardinality);
                }

                pages.add(new Page(blocks));
            }

            return pages.build();
        }

        static Block produceBlock(
                Type type,
                int positionsCount,
                float nullsRatio,
                int maxNestedCardinality)
        {
            if (type instanceof ArrayType || type instanceof MapType) {
                return BlockAssertions.createRandomBlockForNestedType(type, positionsCount, nullsRatio, maxNestedCardinality);
            }

            return BlockAssertions.createRandomBlockForType(type, positionsCount, nullsRatio);
        }
    }

    @Benchmark
    public List<Page> unnest(BenchmarkContext context)
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
    public void testBlocks()
    {
        Block block = produceBlock(new ArrayType(VARCHAR), 100, 0.1f, 50);
        assertEquals(block.getPositionCount(), 100);

        block = produceBlock(mapType(VARCHAR, INTEGER), 100, 0.1f, 50);
        assertEquals(block.getPositionCount(), 100);

        block = produceBlock(RowType.anonymous(Arrays.asList(VARCHAR, VARCHAR)), 100, 0.1f, 50);
        assertEquals(block.getPositionCount(), 100);

        block = produceBlock(new ArrayType(RowType.anonymous(Arrays.asList(VARCHAR, VARCHAR, VARCHAR))), 100, 0.1f, 50);
        assertEquals(block.getPositionCount(), 100);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkUnnestOperator.class)
                .withOptions(optionsBuilder -> optionsBuilder.addProfiler(GCProfiler.class))
                .run();
    }
}
