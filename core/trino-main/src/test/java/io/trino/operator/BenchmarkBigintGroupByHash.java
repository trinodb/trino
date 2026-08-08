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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
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
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBigintGroupByHash
{
    private static final int POSITIONS = 10_000_000;
    private static final int EXPECTED_SIZE = 10_000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPages(BenchmarkData data)
    {
        GroupByHash groupByHash = data.createGroupByHash();
        addInputPagesToHash(groupByHash, data.getPages());
        return groupByHash;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object writeData(WriteBenchmarkData data)
    {
        GroupByHash groupByHash = data.getPrefilledHash();
        int groupCount = groupByHash.getGroupCount();
        PageBuilder pageBuilder = new PageBuilder(groupCount, data.getOutputTypes());
        for (int groupId = 0; groupId < groupCount; groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder);
            if (pageBuilder.isFull()) {
                pageBuilder.reset();
            }
        }
        return pageBuilder.build();
    }

    private static void addInputPagesToHash(GroupByHash groupByHash, List<Page> pages)
    {
        for (Page page : pages) {
            Work<?> work = groupByHash.addPage(page);
            boolean finished;
            do {
                finished = work.process();
            }
            while (!finished);
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"100000", "3000000", "10000000"})
        private int groupCount = 3_000_000;

        @Param({"ADAPTIVE", "MURMUR"})
        private String hashing = "ADAPTIVE";

        @Param({"DENSE", "SCATTERED", "SHIFTED"})
        private String distribution = "DENSE";

        private List<Page> pages;

        @Setup
        public void setup()
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();
            PageBuilder pageBuilder = new PageBuilder(List.of(BIGINT));
            for (int position = 0; position < POSITIONS; position++) {
                pageBuilder.declarePosition();
                long value = ThreadLocalRandom.current().nextInt(groupCount);
                switch (distribution) {
                    case "DENSE" -> {
                        /* small dense integers as generated */
                    }
                    // same cardinality spread over the long range via an odd multiplier (a bijection)
                    case "SCATTERED" -> value *= 0x6A5D39EAE116586BL;
                    // adversarial for identity bucketing: all keys share their low 12 bits
                    case "SHIFTED" -> value <<= 12;
                    default -> throw new IllegalArgumentException("Unknown distribution: " + distribution);
                }
                BIGINT.writeLong(pageBuilder.getBlockBuilder(0), value);
                if (pageBuilder.isFull()) {
                    pages.add(pageBuilder.build());
                    pageBuilder.reset();
                }
            }
            pages.add(pageBuilder.build());
            this.pages = pages.build();
        }

        public GroupByHash createGroupByHash()
        {
            return switch (hashing) {
                case "ADAPTIVE" -> new BigintGroupByHash(EXPECTED_SIZE, NOOP);
                case "MURMUR" -> new BigintGroupByHash(EXPECTED_SIZE, false, NOOP);
                default -> throw new IllegalArgumentException("Unknown hashing: " + hashing);
            };
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class WriteBenchmarkData
    {
        private GroupByHash prefilledHash;
        private List<Type> outputTypes;

        @Setup
        public void setup(BenchmarkData data)
        {
            prefilledHash = data.createGroupByHash();
            addInputPagesToHash(prefilledHash, data.getPages());
            // per the GroupByHash contract, output draining starts here; values are materialized
            // into groupId order at this point (one-time cost paid in setup)
            prefilledHash.startReleasingOutput();
            outputTypes = List.of(BIGINT);
        }

        public GroupByHash getPrefilledHash()
        {
            return prefilledHash;
        }

        public List<Type> getOutputTypes()
        {
            return outputTypes;
        }
    }

    static void main()
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkBigintGroupByHash().addPages(data);

        WriteBenchmarkData writeData = new WriteBenchmarkData();
        writeData.setup(data);
        new BenchmarkBigintGroupByHash().writeData(writeData);

        benchmark(BenchmarkBigintGroupByHash.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgs("-Xmx8g"))
                .run();
    }
}
