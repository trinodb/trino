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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDecimalAggregation
{
    private static final Random RANDOM = new Random(633969769);
    private static final int ELEMENT_COUNT = 1_000_000;

    @Benchmark
    @OperationsPerInvocation(ELEMENT_COUNT)
    public GroupedAggregator benchmark(BenchmarkData data)
    {
        GroupedAggregator aggregator = data.getPartialAggregatorFactory().createGroupedAggregator();
        aggregator.processPage(data.getGroupCount(), data.getGroupIds(), data.getValues());
        return aggregator;
    }

    @Benchmark
    @OperationsPerInvocation(ELEMENT_COUNT)
    public Block benchmarkEvaluateIntermediate(BenchmarkData data)
    {
        GroupedAggregator aggregator = data.getPartialAggregatorFactory().createGroupedAggregator();
        aggregator.processPage(data.getGroupCount(), data.getGroupIds(), data.getValues());
        BlockBuilder builder = aggregator.getType().createBlockBuilder(null, data.getGroupCount());
        for (int groupId = 0; groupId < data.getGroupCount(); groupId++) {
            aggregator.evaluate(groupId, builder);
        }
        return builder.build();
    }

    @Benchmark
    public Block benchmarkEvaluateFinal(BenchmarkData data)
    {
        GroupedAggregator aggregator = data.getFinalAggregatorFactory().createGroupedAggregator();
        // Add the intermediate input multiple times to invoke the combine behavior
        aggregator.processPage(data.getGroupCount(), data.getGroupIds(), data.getIntermediateValues());
        aggregator.processPage(data.getGroupCount(), data.getGroupIds(), data.getIntermediateValues());
        BlockBuilder builder = aggregator.getType().createBlockBuilder(null, data.getGroupCount());
        for (int groupId = 0; groupId < data.getGroupCount(); groupId++) {
            aggregator.evaluate(groupId, builder);
        }
        return builder.build();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"SHORT", "LONG"})
        private String type = "SHORT";

        @Param({"avg", "sum"})
        private String function = "avg";

        @Param({"10", "1000"})
        private int groupCount = 10;

        @Param({"0.0", "0.05"})
        private float nullRate;

        private AggregatorFactory partialAggregatorFactory;
        private AggregatorFactory finalAggregatorFactory;
        private int[] groupIds;
        private Page values;
        private Page intermediateValues;

        @Setup
        public void setup()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution();

            switch (type) {
                case "SHORT": {
                    DecimalType type = createDecimalType(14, 3);
                    values = createValues(functionResolution, type);
                    break;
                }
                case "LONG": {
                    DecimalType type = createDecimalType(30, 10);
                    values = createValues(functionResolution, type);
                    break;
                }
            }

            int[] ids = new int[ELEMENT_COUNT];
            for (int i = 0; i < ELEMENT_COUNT; i++) {
                ids[i] = RANDOM.nextInt(groupCount);
            }
            groupIds = ids;
            intermediateValues = new Page(createIntermediateValues(partialAggregatorFactory.createGroupedAggregator(), groupIds, values));
        }

        private Block createIntermediateValues(GroupedAggregator aggregator, int[] groupIds, Page inputPage)
        {
            aggregator.processPage(groupCount, groupIds, inputPage);
            BlockBuilder builder = aggregator.getType().createBlockBuilder(null, groupCount);
            for (int groupId = 0; groupId < groupCount; groupId++) {
                aggregator.evaluate(groupId, builder);
            }
            return builder.build();
        }

        private Page createValues(TestingFunctionResolution functionResolution, Type type)
        {
            TestingAggregationFunction implementation = functionResolution.getAggregateFunction(QualifiedName.of(function), fromTypes(type));
            partialAggregatorFactory = implementation.createAggregatorFactory(PARTIAL, ImmutableList.of(0), OptionalInt.empty());
            finalAggregatorFactory = implementation.createAggregatorFactory(FINAL, ImmutableList.of(0), OptionalInt.empty());

            return new Page(createRandomBlockForType(type, ELEMENT_COUNT, nullRate));
        }

        public AggregatorFactory getPartialAggregatorFactory()
        {
            return partialAggregatorFactory;
        }

        public AggregatorFactory getFinalAggregatorFactory()
        {
            return finalAggregatorFactory;
        }

        public Page getValues()
        {
            return values;
        }

        public int[] getGroupIds()
        {
            return groupIds;
        }

        public int getGroupCount()
        {
            return groupCount;
        }

        public Page getIntermediateValues()
        {
            return intermediateValues;
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();

        assertEquals(data.getGroupIds().length, data.getValues().getPositionCount());

        new BenchmarkDecimalAggregation().benchmark(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // ensure the benchmarks are valid before running
        new BenchmarkDecimalAggregation().verify();

        Benchmarks.benchmark(BenchmarkDecimalAggregation.class, WarmupMode.BULK).run();
    }
}
