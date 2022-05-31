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
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.array.LongBigArray;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGroupByHash
{
    private static final int POSITIONS = 10_000_000;
    private static final String GROUP_COUNT_STRING = "3000000";
    private static final int GROUP_COUNT = Integer.parseInt(GROUP_COUNT_STRING);
    private static final int EXPECTED_SIZE = 10_000;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(TYPE_OPERATORS);

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object groupByHashPreCompute(BenchmarkData data)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), EXPECTED_SIZE, false, getJoinCompiler(), TYPE_OPERATOR_FACTORY, NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public List<Page> benchmarkHashPosition(BenchmarkData data)
    {
        InterpretedHashGenerator hashGenerator = new InterpretedHashGenerator(data.getTypes(), data.getChannels(), TYPE_OPERATOR_FACTORY);
        ImmutableList.Builder<Page> results = ImmutableList.builderWithExpectedSize(data.getPages().size());
        for (Page page : data.getPages()) {
            long[] hashes = new long[page.getPositionCount()];
            for (int position = 0; position < page.getPositionCount(); position++) {
                hashes[position] = hashGenerator.hashPosition(position, page);
            }
            results.add(page.appendColumn(new LongArrayBlock(page.getPositionCount(), Optional.empty(), hashes)));
        }
        return results.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPagePreCompute(BenchmarkData data)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), EXPECTED_SIZE, false, getJoinCompiler(), TYPE_OPERATOR_FACTORY, NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object bigintGroupByHash(SingleChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new BigintGroupByHash(0, data.getHashEnabled(), EXPECTED_SIZE, NOOP);
        addInputPagesToHash(groupByHash, data.getPages());

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long baseline(BaselinePagesData data)
    {
        int hashSize = arraySize(GROUP_COUNT, 0.9f);
        int mask = hashSize - 1;
        long[] table = new long[hashSize];
        Arrays.fill(table, -1);

        long groupIds = 0;
        for (Page page : data.getPages()) {
            Block block = page.getBlock(0);
            int positionCount = block.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                long value = block.getLong(position, 0);

                int tablePosition = (int) (value & mask);
                while (table[tablePosition] != -1 && table[tablePosition] != value) {
                    tablePosition++;
                }
                if (table[tablePosition] == -1) {
                    table[tablePosition] = value;
                    groupIds++;
                }
            }
        }
        return groupIds;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long baselineBigArray(BaselinePagesData data)
    {
        int hashSize = arraySize(GROUP_COUNT, 0.9f);
        int mask = hashSize - 1;
        LongBigArray table = new LongBigArray(-1);
        table.ensureCapacity(hashSize);

        long groupIds = 0;
        for (Page page : data.getPages()) {
            Block block = page.getBlock(0);
            int positionCount = block.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                long value = BIGINT.getLong(block, position);

                int tablePosition = (int) XxHash64.hash(value) & mask;
                while (table.get(tablePosition) != -1 && table.get(tablePosition) != value) {
                    tablePosition++;
                }
                if (table.get(tablePosition) == -1) {
                    table.set(tablePosition, value);
                    groupIds++;
                }
            }
        }
        return groupIds;
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

    private static List<Page> createBigintPages(int positionCount, int groupCount, int channelCount, boolean hashEnabled, boolean useMixedBlockTypes)
    {
        List<Type> types = Collections.nCopies(channelCount, BIGINT);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        int pageCount = 0;
        for (int position = 0; position < positionCount; position++) {
            int rand = ThreadLocalRandom.current().nextInt(groupCount);
            pageBuilder.declarePosition();
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(numChannel), rand);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), AbstractLongType.hash(rand));
            }
            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                if (useMixedBlockTypes) {
                    if (pageCount % 3 == 0) {
                        pages.add(page);
                    }
                    else if (pageCount % 3 == 1) {
                        // rle page
                        Block[] blocks = new Block[page.getChannelCount()];
                        for (int channel = 0; channel < blocks.length; ++channel) {
                            blocks[channel] = new RunLengthEncodedBlock(page.getBlock(channel).getSingleValueBlock(0), page.getPositionCount());
                        }
                        pages.add(new Page(blocks));
                    }
                    else {
                        // dictionary page
                        int[] positions = IntStream.range(0, page.getPositionCount()).toArray();
                        Block[] blocks = new Block[page.getChannelCount()];
                        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                            blocks[channel] = new DictionaryBlock(page.getBlock(channel), positions);
                        }
                        pages.add(new Page(blocks));
                    }
                }
                else {
                    pages.add(page);
                }
                pageCount++;
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    private static List<Page> createVarcharPages(int positionCount, int groupCount, int channelCount, boolean hashEnabled)
    {
        List<Type> types = Collections.nCopies(channelCount, VARCHAR);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < positionCount; position++) {
            int rand = ThreadLocalRandom.current().nextInt(groupCount);
            Slice value = Slices.wrappedBuffer(ByteBuffer.allocate(4).putInt(rand).flip());
            pageBuilder.declarePosition();
            for (int channel = 0; channel < channelCount; channel++) {
                VARCHAR.writeSlice(pageBuilder.getBlockBuilder(channel), value);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), XxHash64.hash(value));
            }
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BaselinePagesData
    {
        @Param("1")
        private int channelCount = 1;

        @Param("false")
        private boolean hashEnabled;

        @Param(GROUP_COUNT_STRING)
        private int groupCount;

        private List<Page> pages;

        @Setup
        public void setup()
        {
            pages = createBigintPages(POSITIONS, groupCount, channelCount, hashEnabled, false);
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class SingleChannelBenchmarkData
    {
        @Param("1")
        private int channelCount = 1;

        @Param({"true", "false"})
        private boolean hashEnabled = true;

        private List<Page> pages;
        private List<Type> types;
        private int[] channels;

        @Setup
        public void setup()
        {
            setup(false);
        }

        public void setup(boolean useMixedBlockTypes)
        {
            pages = createBigintPages(POSITIONS, GROUP_COUNT, channelCount, hashEnabled, useMixedBlockTypes);
            types = Collections.nCopies(1, BIGINT);
            channels = new int[1];
            for (int i = 0; i < 1; i++) {
                channels[i] = i;
            }
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public boolean getHashEnabled()
        {
            return hashEnabled;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "5", "10", "15", "20"})
        private int channelCount = 1;

        // todo add more group counts when JMH support programmatic ability to set OperationsPerInvocation
        @Param(GROUP_COUNT_STRING)
        private int groupCount = GROUP_COUNT;

        @Param({"true", "false"})
        private boolean hashEnabled;

        @Param({"VARCHAR", "BIGINT"})
        private String dataType = "VARCHAR";

        private List<Page> pages;
        private Optional<Integer> hashChannel;
        private List<Type> types;
        private int[] channels;

        @Setup
        public void setup()
        {
            switch (dataType) {
                case "VARCHAR":
                    types = Collections.nCopies(channelCount, VARCHAR);
                    pages = createVarcharPages(POSITIONS, groupCount, channelCount, hashEnabled);
                    break;
                case "BIGINT":
                    types = Collections.nCopies(channelCount, BIGINT);
                    pages = createBigintPages(POSITIONS, groupCount, channelCount, hashEnabled, false);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported dataType");
            }
            hashChannel = hashEnabled ? Optional.of(channelCount) : Optional.empty();
            channels = new int[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channels[i] = i;
            }
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Optional<Integer> getHashChannel()
        {
            return hashChannel;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public int[] getChannels()
        {
            return channels;
        }
    }

    private static JoinCompiler getJoinCompiler()
    {
        return new JoinCompiler(TYPE_OPERATORS);
    }

    static {
        // pollute BigintGroupByHash profile by different block types
        SingleChannelBenchmarkData singleChannelBenchmarkData = new SingleChannelBenchmarkData();
        singleChannelBenchmarkData.setup(true);
        BenchmarkGroupByHash hash = new BenchmarkGroupByHash();
        for (int i = 0; i < 5; ++i) {
            hash.bigintGroupByHash(singleChannelBenchmarkData);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkGroupByHash().groupByHashPreCompute(data);
        new BenchmarkGroupByHash().addPagePreCompute(data);

        SingleChannelBenchmarkData singleChannelBenchmarkData = new SingleChannelBenchmarkData();
        singleChannelBenchmarkData.setup();
        new BenchmarkGroupByHash().bigintGroupByHash(singleChannelBenchmarkData);

        benchmark(BenchmarkGroupByHash.class)
                .withOptions(optionsBuilder -> optionsBuilder
                        .addProfiler(GCProfiler.class)
                        .jvmArgs("-Xmx10g"))
                .run();
    }
}
