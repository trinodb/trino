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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGroupByHash
{
    private static final int POSITIONS = 10_000_000;
    private static final String GROUP_COUNT_STRING = "3000000";
    private static final int GROUP_COUNT = Integer.parseInt(GROUP_COUNT_STRING);
    private static final int EXPECTED_SIZE = 10_000;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(TYPE_OPERATORS);

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPages(MultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash;
        if (data.isFlat()) {
            groupByHash = new FlatGroupByHash(data.getTypes(), data.isHashEnabled(), EXPECTED_SIZE, false, JOIN_COMPILER, NOOP);
        }
        else if (data.getChannelCount() == 1 && data.getTypes().get(0) == BIGINT) {
            groupByHash = new BigintGroupByHash(data.isHashEnabled(), EXPECTED_SIZE, NOOP);
        }
        else {
            groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.isHashEnabled(), EXPECTED_SIZE, false, JOIN_COMPILER, TYPE_OPERATORS, NOOP);
        }
        addInputPagesToHash(groupByHash, data.getPages());
        return groupByHash;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object writeData(WriteMultiChannelBenchmarkData data)
    {
        GroupByHash groupByHash = data.getPrefilledHash();
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(POSITIONS, data.getOutputTypes());
        int[] groupIdsByPhysicalOrder = data.getGroupIdsByPhysicalOrder();
        for (int groupId : groupIdsByPhysicalOrder) {
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
                            blocks[channel] = RunLengthEncodedBlock.create(page.getBlock(channel).getSingleValueBlock(0), page.getPositionCount());
                        }
                        pages.add(new Page(blocks));
                    }
                    else {
                        // dictionary page
                        int[] positions = IntStream.range(0, page.getPositionCount()).toArray();
                        Block[] blocks = new Block[page.getChannelCount()];
                        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                            blocks[channel] = DictionaryBlock.create(positions.length, page.getBlock(channel), positions);
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
            Slice value = Slices.wrappedHeapBuffer(ByteBuffer.allocate(4).putInt(rand).flip());
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
    public static class MultiChannelBenchmarkData
    {
        @Param({"true", "false"})
        private boolean flat = true;

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
        private List<Type> types;

        @Setup
        public void setup()
        {
            switch (dataType) {
                case "VARCHAR" -> {
                    types = Collections.nCopies(channelCount, VARCHAR);
                    pages = createVarcharPages(POSITIONS, groupCount, channelCount, hashEnabled);
                }
                case "BIGINT" -> {
                    types = Collections.nCopies(channelCount, BIGINT);
                    pages = createBigintPages(POSITIONS, groupCount, channelCount, hashEnabled, false);
                }
                default -> throw new UnsupportedOperationException("Unsupported dataType");
            }
        }

        public boolean isFlat()
        {
            return flat;
        }

        public int getChannelCount()
        {
            return channelCount;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public boolean isHashEnabled()
        {
            return hashEnabled;
        }

        public List<Type> getTypes()
        {
            return types;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class WriteMultiChannelBenchmarkData
    {
        private GroupByHash prefilledHash;
        private int[] groupIdsByPhysicalOrder;
        private List<Type> outputTypes;

        @Setup
        public void setup(MultiChannelBenchmarkData data)
        {
            if (data.isFlat()) {
                prefilledHash = new FlatGroupByHash(data.getTypes(), data.isHashEnabled(), EXPECTED_SIZE, false, JOIN_COMPILER, NOOP);
            }
            else if (data.getChannelCount() == 1 && data.getTypes().get(0) == BIGINT) {
                prefilledHash = new BigintGroupByHash(data.isHashEnabled(), EXPECTED_SIZE, NOOP);
            }
            else {
                prefilledHash = new MultiChannelGroupByHash(data.getTypes(), data.isHashEnabled(), EXPECTED_SIZE, false, JOIN_COMPILER, TYPE_OPERATORS, NOOP);
            }
            addInputPagesToHash(prefilledHash, data.getPages());

            Integer[] groupIds = new Integer[prefilledHash.getGroupCount()];
            for (int i = 0; i < groupIds.length; i++) {
                groupIds[i] = i;
            }
            if (prefilledHash instanceof FlatGroupByHash flatGroupByHash) {
                Arrays.sort(groupIds, Comparator.comparing(flatGroupByHash::getPhysicalPosition));
            }
            groupIdsByPhysicalOrder = Arrays.stream(groupIds).mapToInt(Integer::intValue).toArray();

            outputTypes = new ArrayList<>(data.getTypes());
            if (data.isHashEnabled()) {
                outputTypes.add(BIGINT);
            }
        }

        public GroupByHash getPrefilledHash()
        {
            return prefilledHash;
        }

        public int[] getGroupIdsByPhysicalOrder()
        {
            return groupIdsByPhysicalOrder;
        }

        public List<Type> getOutputTypes()
        {
            return outputTypes;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        MultiChannelBenchmarkData data = new MultiChannelBenchmarkData();
        data.setup();
        new BenchmarkGroupByHash().addPages(data);

        WriteMultiChannelBenchmarkData writeData = new WriteMultiChannelBenchmarkData();
        writeData.setup(data);
        new BenchmarkGroupByHash().writeData(writeData);

        benchmark(BenchmarkGroupByHash.class)
                .withOptions(optionsBuilder -> optionsBuilder
                        .addProfiler(GCProfiler.class)
                        .jvmArgs("-Xmx10g"))
                .run();
    }
}
