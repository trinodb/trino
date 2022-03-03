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
import io.trino.operator.GroupByIdBlock;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
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
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.profile.DTraceAsmProfiler;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 6)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDecimalAggregation
{
    private static final int ELEMENT_COUNT = 1_000_000;

    @Benchmark
    @OperationsPerInvocation(ELEMENT_COUNT)
    public GroupedAggregator benchmark(BenchmarkData data)
    {
        GroupedAggregator aggregator = data.getPartialAggregatorFactory().createGroupedAggregator();
        aggregator.processPage(data.getGroupIds(), data.getValues());
        return aggregator;
    }

    @Benchmark
    @OperationsPerInvocation(ELEMENT_COUNT)
    public Block benchmarkEvaluateIntermediate(BenchmarkData data)
    {
        GroupedAggregator aggregator = data.getPartialAggregatorFactory().createGroupedAggregator();
        aggregator.processPage(data.getGroupIds(), data.getValues());
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
        aggregator.processPage(data.getGroupIds(), data.getIntermediateValues());
        aggregator.processPage(data.getGroupIds(), data.getIntermediateValues());
        BlockBuilder builder = aggregator.getType().createBlockBuilder(null, data.getGroupCount());
        for (int groupId = 0; groupId < data.getGroupCount(); groupId++) {
            aggregator.evaluate(groupId, builder);
        }
        return builder.build();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"LONG"})
        private String type = "SHORT";

        @Param({"avg"})
        private String function = "avg";

        @Param({"1000"})
        private int groupCount = 10;

        private AggregatorFactory partialAggregatorFactory;
        private AggregatorFactory finalAggregatorFactory;
        private GroupByIdBlock groupIds;
        private Page values;
        private Page intermediateValues;

        @Setup
        public void setup()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution();

            switch (type) {
                case "SHORT": {
                    DecimalType type = createDecimalType(14, 3);
                    values = createValues(functionResolution, type, type::writeLong);
                    break;
                }
                case "LONG": {
                    DecimalType type = createDecimalType(30, 10);
                    values = createValues(functionResolution, type, (builder, value) -> type.writeObject(builder, Int128.valueOf(value)));
                    break;
                }
            }

            BlockBuilder ids = BIGINT.createBlockBuilder(null, ELEMENT_COUNT);
            for (int i = 0; i < ELEMENT_COUNT; i++) {
                BIGINT.writeLong(ids, ThreadLocalRandom.current().nextLong(groupCount));
            }
            groupIds = new GroupByIdBlock(groupCount, ids.build());
            intermediateValues = new Page(createIntermediateValues(partialAggregatorFactory.createGroupedAggregator(), groupIds, values));
        }

        private Block createIntermediateValues(GroupedAggregator aggregator, GroupByIdBlock groupIds, Page inputPage)
        {
            aggregator.processPage(groupIds, inputPage);
            BlockBuilder builder = aggregator.getType().createBlockBuilder(null, toIntExact(groupIds.getGroupCount()));
            for (int groupId = 0; groupId < groupIds.getGroupCount(); groupId++) {
                aggregator.evaluate(groupId, builder);
            }
            return builder.build();
        }

        private Page createValues(TestingFunctionResolution functionResolution, DecimalType type, ValueWriter writer)
        {
            TestingAggregationFunction implementation = functionResolution.getAggregateFunction(QualifiedName.of(function), fromTypes(type));
            partialAggregatorFactory = implementation.createAggregatorFactory(PARTIAL, ImmutableList.of(0), OptionalInt.empty());
            finalAggregatorFactory = implementation.createAggregatorFactory(FINAL, ImmutableList.of(0), OptionalInt.empty());

            BlockBuilder builder = type.createBlockBuilder(null, ELEMENT_COUNT);
            for (int i = 0; i < ELEMENT_COUNT; i++) {
                writer.write(builder, i);
            }
            return new Page(builder.build());
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

        public GroupByIdBlock getGroupIds()
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

        interface ValueWriter
        {
            void write(BlockBuilder valuesBuilder, int value);
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();

        assertEquals(data.groupIds.getPositionCount(), data.getValues().getPositionCount());

        new BenchmarkDecimalAggregation().benchmark(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // ensure the benchmarks are valid before running
        new BenchmarkDecimalAggregation().verify();

        String profilerOutputDir;
        try {
            String jmhDir = "jmh";
            new File(jmhDir).mkdirs();
            String outDir = jmhDir + "/";
            String end = String.valueOf(Files.list(Paths.get(jmhDir))
                    .map(path -> path.getFileName().toString())
                    .filter(path -> path.matches("\\d+"))
                    .map(path -> Integer.parseInt(path) + 1)
                    .sorted(Comparator.reverseOrder())
                    .findFirst().orElse(0));
            outDir = outDir + System.getProperty("outputDirectory", end);
            new File(outDir).mkdirs();
            profilerOutputDir = outDir;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        String finalProfilerOutputDir = profilerOutputDir;


        Benchmarks.benchmark(BenchmarkDecimalAggregation.class)
                .withOptions(options -> options
                        .jvmArgs("-Xmx32g")
                        .include("benchmarkEvaluateFinal")
                        .addProfiler(AsyncProfiler.class, String.format("dir=%s;output=flamegraph;event=cpu", finalProfilerOutputDir))
                        .addProfiler(DTraceAsmProfiler.class, String.format("hotThreshold=0.05;tooBigThreshold=3000;saveLog=true;saveLogTo=%s", profilerOutputDir))
                        .output(String.format("%s/%s", finalProfilerOutputDir, "stdout.log"))
                        .jvmArgsAppend(
                                "-XX:+UnlockDiagnosticVMOptions",
                                "-XX:+PrintAssembly",
                                "-XX:+LogCompilation",
                                "-XX:+TraceClassLoading")
                                )

                .run();
    }
}
