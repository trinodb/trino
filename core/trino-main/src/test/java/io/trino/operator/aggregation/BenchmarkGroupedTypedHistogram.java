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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.sql.tree.QualifiedName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

@OutputTimeUnit(TimeUnit.SECONDS)
//@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 7)
@Measurement(iterations = 20)
public class BenchmarkGroupedTypedHistogram
{
    @State(Scope.Thread)
    public static class Data
    {
        @Param("10000") // larger groups => worse perf for NEW as it's more costly to track a group than with LEGACY. Tweak based on what you want to measure
        private int numGroups;
        @Param("5000") // makes sure legacy impl isn't doing trivial work
        private int rowCount;
        //        @Param({"0.0", "0.1", ".25", "0.50", ".75", "1.0"})
        @Param("0.1") // somewhat arbitrary guess, we don't know this
        private float distinctFraction;
        //        @Param({"1", "5", "50"})
        @Param("32") // size of entries--we have no idea here, could be 8 long (common in anecdotal) or longer strings
        private int rowSize;
        // these must be manually set in each class now; the mechanism to change and test was removed; the enum was kept in case we want to revisit. Retesting showed linear was superior
        //        //        @Param({"LINEAR", "SUM_OF_COUNT", "SUM_OF_SQUARE"})
//        @Param({"LINEAR"}) // found to be best, by about 10-15%
//        private ProbeType mainProbeTyepe;
//        //        @Param({"LINEAR", "SUM_OF_COUNT", "SUM_OF_SQUARE"})
//        @Param({"LINEAR"}) // found to best
//        private ProbeType valueStoreProbeType;

        private final Random random = new Random();
        private Page[] pages;
        private int[] groupCounts;
        private int[][] groupByIdBlocks;
        private GroupedAggregator groupedAggregator;

        @Setup
        public void setUp()
        {
            pages = new Page[numGroups];
            groupCounts = new int[numGroups];
            groupByIdBlocks = new int[numGroups][];

            for (int j = 0; j < numGroups; j++) {
                List<String> valueList = new ArrayList<>();

                for (int i = 0; i < rowCount; i++) {
                    // makes sure rows don't exceed rowSize
                    String str = String.valueOf(i % 10);
                    String item = IntStream.range(0, rowSize).mapToObj(x -> str).collect(Collectors.joining());
                    boolean distinctValue = random.nextDouble() < distinctFraction;

                    if (distinctValue) {
                        // produce a unique value for the histogram
                        valueList.add(j + "-" + item);
                    }
                    else {
                        valueList.add(item);
                    }
                }

                Block block = createStringsBlock(valueList);
                Page page = new Page(block);
                int[] groupByIdBlock = AggregationTestUtils.createGroupByIdBlock(j, page.getPositionCount());

                pages[j] = page;
                groupCounts[j] = j;
                groupByIdBlocks[j] = groupByIdBlock;
            }

            TestingAggregationFunction aggregationFunction = getInternalAggregationFunctionVarChar();
            groupedAggregator = aggregationFunction.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())
                    .createGroupedAggregator();
        }
    }

    @Benchmark
    public GroupedAggregator testSharedGroupWithLargeBlocksRunner(Data data)
    {
        GroupedAggregator groupedAggregator = data.groupedAggregator;

        for (int i = 0; i < data.numGroups; i++) {
            int groupCount = data.groupCounts[i];
            int[] groupByIdBlock = data.groupByIdBlocks[i];
            Page page = data.pages[i];
            groupedAggregator.processPage(groupCount, groupByIdBlock, page);
        }

        return groupedAggregator;
    }

    private static TestingAggregationFunction getInternalAggregationFunctionVarChar()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        return functionResolution.getAggregateFunction(QualifiedName.of("histogram"), fromTypes(VARCHAR));
    }

    @Test
    public void test()
    {
        Data data = new Data();
        data.setUp();
        testSharedGroupWithLargeBlocksRunner(data);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkGroupedTypedHistogram.class)
                .withOptions(optionsBuilder -> optionsBuilder.addProfiler(GCProfiler.class))
                .run();
    }

    public enum ProbeType
    {
        LINEAR, SUM_OF_COUNT, SUM_OF_SQUARE
    }
}
