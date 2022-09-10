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
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
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
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.BenchmarkGroupByHashOnSimulatedData.AggregationDefinition.BIGINT_1K_GROUPS;
import static io.trino.operator.BenchmarkGroupByHashOnSimulatedData.AggregationDefinition.BIGINT_1M_GROUPS;
import static io.trino.operator.BenchmarkGroupByHashOnSimulatedData.AggregationDefinition.BIGINT_2_GROUPS;
import static io.trino.operator.BenchmarkGroupByHashOnSimulatedData.WorkType.GET_GROUPS;
import static io.trino.operator.UpdateMemory.NOOP;
import static java.util.Objects.requireNonNull;

/**
 * This class attempts to emulate aggregations done while running real-life queries.
 * Some of the numbers here has been inspired by tpch benchmarks, however,
 * there is no guarantee that results correlate with the benchmark itself.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGroupByHashOnSimulatedData
{
    private static final int DEFAULT_POSITIONS = 10_000_000;
    private static final int EXPECTED_GROUP_COUNT = 10_000;
    private static final int DEFAULT_PAGE_SIZE = 8192;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(TYPE_OPERATORS);

    private final JoinCompiler joinCompiler = new JoinCompiler(TYPE_OPERATORS);

    @Benchmark
    @OperationsPerInvocation(DEFAULT_POSITIONS)
    public Object groupBy(BenchmarkContext data)
    {
        GroupByHash groupByHash = GroupByHash.createGroupByHash(
                data.getTypes(),
                data.getChannels(),
                Optional.empty(),
                EXPECTED_GROUP_COUNT,
                false,
                joinCompiler,
                TYPE_OPERATOR_FACTORY,
                NOOP);
        List<GroupByIdBlock> results = addInputPages(groupByHash, data.getPages(), data.getWorkType());

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
        return ImmutableList.of(pages, results); // all the things that might get erased by the compiler
    }

    @Test
    public void testGroupBy()
    {
        BenchmarkGroupByHashOnSimulatedData benchmark = new BenchmarkGroupByHashOnSimulatedData();
        for (double nullChance : new double[] {0, .1, .5, .9}) {
            for (AggregationDefinition query : AggregationDefinition.values()) {
                BenchmarkContext data = new BenchmarkContext(GET_GROUPS, query, nullChance, 10_000);
                data.setup();
                benchmark.groupBy(data);
            }
        }
    }

    private List<GroupByIdBlock> addInputPages(GroupByHash groupByHash, List<Page> pages, WorkType workType)
    {
        List<GroupByIdBlock> results = new ArrayList<>();
        for (Page page : pages) {
            if (workType == GET_GROUPS) {
                Work<GroupByIdBlock> work = groupByHash.getGroupIds(page);
                boolean finished;
                do {
                    finished = work.process();
                    results.add(work.getResult());
                }
                while (!finished);
            }
            else {
                Work<?> work = groupByHash.addPage(page);
                boolean finished;
                do {
                    finished = work.process();
                }
                while (!finished);
            }
        }

        return results;
    }

    public interface BlockWriter
    {
        void write(BlockBuilder blockBuilder, int positionCount, long randomSeed);
    }

    public enum ColumnType
    {
        BIGINT(BigintType.BIGINT, (blockBuilder, positionCount, seed) -> {
            Random r = new Random(seed);
            for (int i = 0; i < positionCount; i++) {
                blockBuilder.writeLong((r.nextLong() >>> 1)); // Only positives
            }
        }),
        INT(IntegerType.INTEGER, (blockBuilder, positionCount, seed) -> {
            Random r = new Random(seed);
            for (int i = 0; i < positionCount; i++) {
                blockBuilder.writeInt(r.nextInt());
            }
        }),
        DOUBLE(DoubleType.DOUBLE, (blockBuilder, positionCount, seed) -> {
            Random r = new Random(seed);
            for (int i = 0; i < positionCount; i++) {
                blockBuilder.writeLong((r.nextLong() >>> 1)); // Only positives
            }
        }),
        VARCHAR_25(VarcharType.VARCHAR, (blockBuilder, positionCount, seed) -> {
            writeVarchar(blockBuilder, positionCount, seed, 25);
        }),
        VARCHAR_117(VarcharType.VARCHAR, (blockBuilder, positionCount, seed) -> {
            writeVarchar(blockBuilder, positionCount, seed, 117);
        }),
        CHAR_1(CharType.createCharType(1), (blockBuilder, positionCount, seed) -> {
            Random r = new Random(seed);
            for (int i = 0; i < positionCount; i++) {
                byte value = (byte) r.nextInt();
                while (value == ' ') {
                    value = (byte) r.nextInt();
                }
                CharType.createCharType(1).writeSlice(blockBuilder, Slices.wrappedBuffer(value));
            }
        }),
        /**/;

        private static void writeVarchar(BlockBuilder blockBuilder, int positionCount, long seed, int maxLength)
        {
            Random r = new Random(seed);

            for (int i = 0; i < positionCount; i++) {
                int length = 1 + r.nextInt(maxLength - 1);
                byte[] bytes = new byte[length];
                r.nextBytes(bytes);
                VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.wrappedBuffer(bytes));
            }
        }

        final Type type;
        final BlockWriter blockWriter;

        ColumnType(Type type, BlockWriter blockWriter)
        {
            this.type = requireNonNull(type, "type is null");
            this.blockWriter = requireNonNull(blockWriter, "blockWriter is null");
        }

        public Type getType()
        {
            return type;
        }

        public BlockWriter getBlockWriter()
        {
            return blockWriter;
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        @Param
        private WorkType workType;

        @Param
        private AggregationDefinition query;

        @Param({"0", ".1", ".5", ".9"})
        private double nullChance;

        private final int positions;
        private List<Page> pages;
        private List<Type> types;
        private int[] channels;

        public BenchmarkContext()
        {
            this.positions = DEFAULT_POSITIONS;
        }

        public BenchmarkContext(WorkType workType, AggregationDefinition query, double nullChance, int positions)
        {
            this.workType = requireNonNull(workType, "workType is null");
            this.query = requireNonNull(query, "query is null");
            this.positions = positions;
            this.nullChance = nullChance;
        }

        @Setup
        public void setup()
        {
            types = query.getChannels().stream()
                    .map(channel -> channel.columnType.type)
                    .collect(toImmutableList());
            channels = IntStream.range(0, query.getChannels().size()).toArray();
            pages = createPages(query);
        }

        private List<Page> createPages(AggregationDefinition definition)
        {
            List<Page> result = new ArrayList<>();
            int channelCount = definition.getChannels().size();
            int pageSize = definition.pageSize;
            int pageCount = positions / pageSize;

            Block[][] blocks = new Block[channelCount][];
            for (int i = 0; i < definition.getChannels().size(); i++) {
                ChannelDefinition channel = definition.getChannels().get(i);
                blocks[i] = channel.createBlocks(pageCount, pageSize, i, nullChance);
            }

            for (int i = 0; i < pageCount; i++) {
                int pageIndex = i;
                Block[] pageBlocks = IntStream.range(0, channelCount)
                        .mapToObj(channel -> blocks[channel][pageIndex])
                        .toArray(Block[]::new);
                result.add(new Page(pageBlocks));
            }

            return result;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public int[] getChannels()
        {
            return channels;
        }

        public WorkType getWorkType()
        {
            return workType;
        }
    }

    public enum WorkType
    {
        // Only create groups without actually returning the group ids
        ADD,
        // Create the groups and return group ids
        GET_GROUPS,
    }

    public enum AggregationDefinition
    {
        // Single bigint column
        BIGINT_2_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 2)),
        BIGINT_10_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 10)),
        BIGINT_1K_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 1000)),
        BIGINT_10K_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 10_000)),
        BIGINT_100K_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 100_000)),
        BIGINT_1M_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 1_000_000)),
        BIGINT_10M_GROUPS(new ChannelDefinition(ColumnType.BIGINT, 10_000_000)),
        // Single bigint dictionary column
        BIGINT_2_GROUPS_1_SMALL_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 2, 1, 50)),
        BIGINT_2_GROUPS_1_BIG_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 2, 1, 10000)),
        BIGINT_2_GROUPS_MULTIPLE_SMALL_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 2, 10, 50)),
        BIGINT_2_GROUPS_MULTIPLE_BIG_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 2, 10, 10000)),
        BIGINT_10K_GROUPS_1_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 10000, 1, 20000)),
        BIGINT_10K_GROUPS_MULTIPLE_DICTIONARY(new ChannelDefinition(ColumnType.BIGINT, 10000, 20, 20000)),
        // Single double column
        DOUBLE_10_GROUPS(new ChannelDefinition(ColumnType.DOUBLE, 10)),
        // Multiple dictionary varchar column
        TWO_TINY_VARCHAR_DICTIONARIES(
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10)),
        FIVE_TINY_VARCHAR_DICTIONARIES(
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 2, 10)),
        TWO_SMALL_VARCHAR_DICTIONARIES(
                new ChannelDefinition(ColumnType.CHAR_1, 30, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 30, 10)),
        TWO_SMALL_VARCHAR_DICTIONARIES_WITH_SMALL_PAGE_SIZE(// low cardinality optimisation will not kick in here
                1000,
                new ChannelDefinition(ColumnType.CHAR_1, 30, 10),
                new ChannelDefinition(ColumnType.CHAR_1, 30, 10)),
        // Single varchar column
        VARCHAR_2_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 2)),
        VARCHAR_10_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 10)),
        VARCHAR_1K_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 1000)),
        VARCHAR_10K_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 10_000)),
        VARCHAR_100K_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 100_000)),
        VARCHAR_1M_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 1_000_000)),
        VARCHAR_10M_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_25, 10_000_000)),
        // Single dictionary varchar column
        VARCHAR_2_GROUPS_1_SMALL_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 2, 1, 50)),
        VARCHAR_2_GROUPS_1_BIG_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 2, 1, 10000)),
        VARCHAR_2_GROUPS_MULTIPLE_SMALL_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 2, 10, 50)),
        VARCHAR_2_GROUPS_MULTIPLE_BIG_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 2, 10, 10000)),
        VARCHAR_10K_GROUPS_1_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 10000, 1, 20000)),
        VARCHAR_10K_GROUPS_MULTIPLE_DICTIONARY(new ChannelDefinition(ColumnType.VARCHAR_25, 10000, 20, 20000)),
        TINY_CHAR_10_GROUPS(new ChannelDefinition(ColumnType.CHAR_1, 10)),
        BIG_VARCHAR_10_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_117, 10)),
        BIG_VARCHAR_1M_GROUPS(new ChannelDefinition(ColumnType.VARCHAR_117, 1_000_000)),
        // Multiple columns
        DOUBLE_BIGINT_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 10),
                new ChannelDefinition(ColumnType.BIGINT, 10)),
        BIGINT_AND_TWO_INTS_5K(
                new ChannelDefinition(ColumnType.BIGINT, 500),
                new ChannelDefinition(ColumnType.INT, 10),
                new ChannelDefinition(ColumnType.INT, 10)),
        FIVE_MIXED_SHORT_COLUMNS_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.INT, 1),
                new ChannelDefinition(ColumnType.DOUBLE, 2)),
        FIVE_MIXED_SHORT_COLUMNS_100K_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 20),
                new ChannelDefinition(ColumnType.INT, 10),
                new ChannelDefinition(ColumnType.DOUBLE, 20)),
        FIVE_MIXED_LONG_COLUMNS_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 1),
                new ChannelDefinition(ColumnType.VARCHAR_117, 2)),
        FIVE_MIXED_LONG_COLUMNS_100K_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 20),
                new ChannelDefinition(ColumnType.VARCHAR_25, 10),
                new ChannelDefinition(ColumnType.VARCHAR_117, 20)),
        TEN_MIXED_SHORT_COLUMNS_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 1),
                new ChannelDefinition(ColumnType.INT, 2),
                new ChannelDefinition(ColumnType.BIGINT, 1),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 1),
                new ChannelDefinition(ColumnType.BIGINT, 2),
                new ChannelDefinition(ColumnType.INT, 1),
                new ChannelDefinition(ColumnType.VARCHAR_25, 5),
                new ChannelDefinition(ColumnType.INT, 1),
                new ChannelDefinition(ColumnType.DOUBLE, 1)),
        TEN_MIXED_SHORT_COLUMNS_100K_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.INT, 2),
                new ChannelDefinition(ColumnType.BIGINT, 2),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 5),
                new ChannelDefinition(ColumnType.BIGINT, 2),
                new ChannelDefinition(ColumnType.INT, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 5),
                new ChannelDefinition(ColumnType.INT, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 2)),
        TEN_MIXED_LONG_COLUMNS_100_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 1),
                new ChannelDefinition(ColumnType.VARCHAR_117, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 1),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 1),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 1),
                new ChannelDefinition(ColumnType.VARCHAR_25, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 1),
                new ChannelDefinition(ColumnType.DOUBLE, 1)),
        TEN_MIXED_LONG_COLUMNS_100K_GROUPS(
                new ChannelDefinition(ColumnType.BIGINT, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 5),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 2),
                new ChannelDefinition(ColumnType.VARCHAR_25, 5),
                new ChannelDefinition(ColumnType.VARCHAR_117, 5),
                new ChannelDefinition(ColumnType.DOUBLE, 2)),
        /**/;

        private final int pageSize;
        private final List<ChannelDefinition> channels;

        AggregationDefinition(ChannelDefinition... channels)
        {
            this(DEFAULT_PAGE_SIZE, channels);
        }

        AggregationDefinition(int pageSize, ChannelDefinition... channels)
        {
            this.pageSize = pageSize;
            this.channels = Arrays.stream(requireNonNull(channels, "channels is null")).collect(toImmutableList());
        }

        public int getPageSize()
        {
            return pageSize;
        }

        public List<ChannelDefinition> getChannels()
        {
            return channels;
        }
    }

    public static class ChannelDefinition
    {
        private final ColumnType columnType;
        private final int distinctValuesCountInColumn;
        private final int dictionaryPositionsCount;
        private final int numberOfDistinctDictionaries;

        public ChannelDefinition(ColumnType columnType, int distinctValuesCountInColumn)
        {
            this(columnType, distinctValuesCountInColumn, -1, -1);
        }

        public ChannelDefinition(ColumnType columnType, int distinctValuesCountInColumn, int numberOfDistinctDictionaries)
        {
            this(columnType, distinctValuesCountInColumn, numberOfDistinctDictionaries, distinctValuesCountInColumn);
        }

        public ChannelDefinition(ColumnType columnType, int distinctValuesCountInColumn, int numberOfDistinctDictionaries, int dictionaryPositionsCount)
        {
            this.columnType = requireNonNull(columnType, "columnType is null");
            this.distinctValuesCountInColumn = distinctValuesCountInColumn;
            this.dictionaryPositionsCount = dictionaryPositionsCount;
            this.numberOfDistinctDictionaries = numberOfDistinctDictionaries;
            checkArgument(dictionaryPositionsCount == -1 || dictionaryPositionsCount >= distinctValuesCountInColumn);
        }

        public ColumnType getColumnType()
        {
            return columnType;
        }

        public Block[] createBlocks(int blockCount, int positionsPerBlock, int channel, double nullChance)
        {
            Block[] blocks = new Block[blockCount];
            if (dictionaryPositionsCount == -1) { // No dictionaries
                createNonDictionaryBlock(blockCount, positionsPerBlock, channel, nullChance, blocks);
            }
            else {
                createDictionaryBlock(blockCount, positionsPerBlock, channel, nullChance, blocks);
            }
            return blocks;
        }

        private void createDictionaryBlock(int blockCount, int positionsPerBlock, int channel, double nullChance, Block[] blocks)
        {
            Random r = new Random(channel);

            // All values that will be stored in dictionaries. Not all of them need to be used in blocks
            BlockBuilder allValues = generateValues(channel, dictionaryPositionsCount);
            if (nullChance > 0) {
                allValues.appendNull();
            }

            Block[] dictionaries = new Block[numberOfDistinctDictionaries];
            // Generate 'numberOfDistinctDictionaries' dictionaries that are equal, but are not the same object.
            // This way the optimization that caches dictionary results will not work
            for (int i = 0; i < numberOfDistinctDictionaries; i++) {
                dictionaries[i] = allValues.build();
            }

            int[] usedValues = nOutOfM(r, distinctValuesCountInColumn, dictionaryPositionsCount).stream()
                    .mapToInt(x -> x)
                    .toArray();

            // Generate output blocks
            for (int i = 0; i < blockCount; i++) {
                int[] indexes = new int[positionsPerBlock];
                int dictionaryId = r.nextInt(numberOfDistinctDictionaries);
                Block dictionary = dictionaries[dictionaryId];
                for (int j = 0; j < positionsPerBlock; j++) {
                    if (isNull(r, nullChance)) {
                        indexes[j] = dictionaryPositionsCount; // Last value in dictionary is null
                    }
                    else {
                        indexes[j] = usedValues[r.nextInt(usedValues.length)];
                    }
                }

                blocks[i] = DictionaryBlock.create(indexes.length, dictionary, indexes);
            }
        }

        private void createNonDictionaryBlock(int blockCount, int positionsPerBlock, int channel, double nullChance, Block[] blocks)
        {
            BlockBuilder allValues = generateValues(channel, distinctValuesCountInColumn);
            Random r = new Random(channel);
            for (int i = 0; i < blockCount; i++) {
                BlockBuilder block = columnType.getType().createBlockBuilder(null, positionsPerBlock);
                for (int j = 0; j < positionsPerBlock; j++) {
                    if (isNull(r, nullChance)) {
                        block.appendNull();
                    }
                    else {
                        int position = r.nextInt(distinctValuesCountInColumn);
                        columnType.getType().appendTo(allValues, position, block);
                    }
                }
                blocks[i] = block.build();
            }
        }

        private BlockBuilder generateValues(int channel, int distinctValueCount)
        {
            BlockBuilder allValues = columnType.getType().createBlockBuilder(null, distinctValueCount);
            columnType.getBlockWriter().write(allValues, distinctValueCount, channel);
            return allValues;
        }

        private static boolean isNull(Random random, double nullChance)
        {
            double value = 0;
            // null chance has to be 0 to 1 exclusive.
            while (value == 0) {
                value = random.nextDouble();
            }
            return value < nullChance;
        }

        private Set<Integer> nOutOfM(Random r, int n, int m)
        {
            Set<Integer> usedValues = new HashSet<>();
            // Double loop for performance reasons
            while (usedValues.size() < n) {
                int left = n - usedValues.size();
                for (int i = 0; i < left; i++) {
                    usedValues.add(r.nextInt(m));
                }
            }
            return usedValues;
        }
    }

    static {
        // Pollute JVM profile
        BenchmarkGroupByHashOnSimulatedData benchmark = new BenchmarkGroupByHashOnSimulatedData();
        for (WorkType workType : WorkType.values()) {
            for (double nullChance : new double[] {0, .1, .5, .9}) {
                for (AggregationDefinition query : new AggregationDefinition[] {BIGINT_2_GROUPS, BIGINT_1K_GROUPS, BIGINT_1M_GROUPS}) {
                    BenchmarkContext context = new BenchmarkContext(workType, query, nullChance, 8000);
                    context.setup();
                    benchmark.groupBy(context);
                }
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkGroupByHashOnSimulatedData.class)
                .withOptions(optionsBuilder -> optionsBuilder
                        .jvmArgs("-Xmx8g"))
                .run();
    }
}
