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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.block.BlockAssertions;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.BlockTypeOperators;
import io.trino.type.UnknownType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongDecimalsBlock;
import static io.trino.block.BlockAssertions.createLongTimestampBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRandomDictionaryBlock;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.block.BlockAssertions.createSmallintsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTinyintsBlock;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPositionsAppender
{
    private static final PositionsAppenderFactory POSITIONS_APPENDER_FACTORY = new PositionsAppenderFactory(new BlockTypeOperators());

    @Test(dataProvider = "types")
    public void testMixedBlockTypes(TestType type)
    {
        List<BlockView> input = ImmutableList.of(
                input(emptyBlock(type)),
                input(nullBlock(type, 3), 0, 2),
                input(notNullBlock(type, 3), 1, 2),
                input(partiallyNullBlock(type, 4), 0, 1, 2, 3),
                input(partiallyNullBlock(type, 4)), // empty position list
                input(rleBlock(type, 4), 0, 2),
                input(rleBlock(type, 2), 0, 1), // rle all positions
                input(nullRleBlock(type, 4), 1, 2),
                input(dictionaryBlock(type, 4, 2, 0), 0, 3), // dict not null
                input(dictionaryBlock(type, 8, 4, 0.5F), 1, 3, 5), // dict mixed
                input(dictionaryBlock(type, 8, 4, 1), 1, 3, 5), // dict null
                input(rleBlock(dictionaryBlock(type, 1, 2, 0), 3), 2), // rle -> dict
                input(rleBlock(dictionaryBlock(notNullBlock(type, 2), new int[] {1}), 3), 2), // rle -> dict with position 0 mapped to > 0
                input(rleBlock(dictionaryBlock(rleBlock(type, 4), 1), 3), 1), // rle -> dict -> rle
                input(dictionaryBlock(dictionaryBlock(type, 5, 4, 0.5F), 3), 2), // dict -> dict
                input(dictionaryBlock(dictionaryBlock(dictionaryBlock(type, 5, 4, 0.5F), 3), 3), 2), // dict -> dict -> dict
                input(dictionaryBlock(rleBlock(type, 4), 3), 0, 2), // dict -> rle
                input(notNullBlock(type, 4).getRegion(2, 2), 0, 1), // not null block with offset
                input(partiallyNullBlock(type, 4).getRegion(2, 2), 0, 1), // nullable block with offset
                input(rleBlock(notNullBlock(type, 4).getRegion(2, 1), 3), 1)); // rle block with offset

        testAppend(type, input);
    }

    @Test(dataProvider = "types")
    public void testNullRle(TestType type)
    {
        testNullRle(type.getType(), nullBlock(type, 2));
        testNullRle(type.getType(), nullRleBlock(type, 2));
        testNullRle(type.getType(), createRandomBlockForType(type, 4, 0.5f));
    }

    @Test(dataProvider = "types")
    public void testRleSwitchToFlat(TestType type)
    {
        List<BlockView> inputs = ImmutableList.of(
                input(rleBlock(type, 3), 0, 1),
                input(notNullBlock(type, 2), 0, 1));
        testAppend(type, inputs);

        List<BlockView> dictionaryInputs = ImmutableList.of(
                input(rleBlock(type, 3), 0, 1),
                input(dictionaryBlock(type, 2, 4, 0), 0, 1));
        testAppend(type, dictionaryInputs);
    }

    @Test(dataProvider = "types")
    public void testFlatAppendRle(TestType type)
    {
        List<BlockView> inputs = ImmutableList.of(
                input(notNullBlock(type, 2), 0, 1),
                input(rleBlock(type, 3), 0, 1));
        testAppend(type, inputs);

        List<BlockView> dictionaryInputs = ImmutableList.of(
                input(dictionaryBlock(type, 2, 4, 0), 0, 1),
                input(rleBlock(type, 3), 0, 1));
        testAppend(type, dictionaryInputs);
    }

    @Test(dataProvider = "differentValues")
    public void testMultipleRleBlocksWithDifferentValues(TestType type, Block value1, Block value2)
    {
        List<BlockView> input = ImmutableList.of(
                input(rleBlock(value1, 3), 0, 1),
                input(rleBlock(value2, 3), 0, 1));
        testAppend(type, input);
    }

    @DataProvider(name = "differentValues")
    public static Object[][] differentValues()
    {
        return new Object[][]
                {
                        {TestType.BIGINT, createLongsBlock(0), createLongsBlock(1)},
                        {TestType.BOOLEAN, createBooleansBlock(true), createBooleansBlock(false)},
                        {TestType.INTEGER, createIntsBlock(0), createIntsBlock(1)},
                        {TestType.CHAR_10, createStringsBlock("0"), createStringsBlock("1")},
                        {TestType.VARCHAR, createStringsBlock("0"), createStringsBlock("1")},
                        {TestType.DOUBLE, createDoublesBlock(0.0), createDoublesBlock(1.0)},
                        {TestType.SMALLINT, createSmallintsBlock(0), createSmallintsBlock(1)},
                        {TestType.TINYINT, createTinyintsBlock(0), createTinyintsBlock(1)},
                        {TestType.VARBINARY, createSlicesBlock(Slices.allocate(Long.BYTES)), createSlicesBlock(Slices.allocate(Long.BYTES).getOutput().appendLong(1).slice())},
                        {TestType.LONG_DECIMAL, createLongDecimalsBlock("0"), createLongDecimalsBlock("1")},
                        {TestType.ARRAY_BIGINT, createArrayBigintBlock(ImmutableList.of(ImmutableList.of(0L))), createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L)))},
                        {TestType.LONG_TIMESTAMP, createLongTimestampBlock(createTimestampType(9), new LongTimestamp(0, 0)),
                                createLongTimestampBlock(createTimestampType(9), new LongTimestamp(1, 0))},
                        {TestType.VARCHAR_WITH_TEST_BLOCK, adapt(createStringsBlock("0")), adapt(createStringsBlock("1"))}
                };
    }

    @Test(dataProvider = "types")
    public void testMultipleRleWithTheSameValueProduceRle(TestType type)
    {
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        Block value = notNullBlock(type, 1);
        positionsAppender.append(allPositions(3), rleBlock(value, 3));
        positionsAppender.append(allPositions(2), rleBlock(value, 2));

        Block actual = positionsAppender.build();
        assertThat(actual.getPositionCount()).isEqualTo(5);
        assertInstanceOf(actual, RunLengthEncodedBlock.class);
    }

    @Test(dataProvider = "complexTypesWithNullElementBlock")
    public void testRleAppendForComplexTypeWithNullElement(TestType type, Block value)
    {
        checkArgument(value.getPositionCount() == 1);
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        positionsAppender.append(allPositions(3), rleBlock(value, 3));
        positionsAppender.append(allPositions(2), rleBlock(value, 2));

        Block actual = positionsAppender.build();
        assertThat(actual.getPositionCount()).isEqualTo(5);
        assertInstanceOf(actual, RunLengthEncodedBlock.class);
        assertBlockEquals(type.getType(), actual, RunLengthEncodedBlock.create(value, 5));
    }

    @Test(dataProvider = "types")
    public void testRleAppendedWithSinglePositionDoesNotProduceRle(TestType type)
    {
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        Block value = notNullBlock(type, 1);
        positionsAppender.append(allPositions(3), rleBlock(value, 3));
        positionsAppender.append(allPositions(2), rleBlock(value, 2));
        positionsAppender.append(0, rleBlock(value, 2));

        Block actual = positionsAppender.build();
        assertThat(actual.getPositionCount()).isEqualTo(6);
        assertThat(actual instanceof RunLengthEncodedBlock)
                .describedAs(actual.getClass().getSimpleName())
                .isFalse();
    }

    @Test(dataProvider = "types")
    public static void testMultipleTheSameDictionariesProduceDictionary(TestType type)
    {
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        testMultipleTheSameDictionariesProduceDictionary(type, positionsAppender);
        // test if appender can accept different dictionary after a build
        testMultipleTheSameDictionariesProduceDictionary(type, positionsAppender);
    }

    private static void testMultipleTheSameDictionariesProduceDictionary(TestType type, UnnestingPositionsAppender positionsAppender)
    {
        Block dictionary = createRandomBlockForType(type, 4, 0);
        positionsAppender.append(allPositions(3), createRandomDictionaryBlock(dictionary, 3));
        positionsAppender.append(allPositions(2), createRandomDictionaryBlock(dictionary, 2));

        Block actual = positionsAppender.build();
        assertThat(actual.getPositionCount()).isEqualTo(5);
        assertInstanceOf(actual, DictionaryBlock.class);
        assertThat(((DictionaryBlock) actual).getDictionary()).isEqualTo(dictionary);
    }

    @Test(dataProvider = "types")
    public void testDictionarySwitchToFlat(TestType type)
    {
        List<BlockView> inputs = ImmutableList.of(
                input(dictionaryBlock(type, 3, 4, 0), 0, 1),
                input(notNullBlock(type, 2), 0, 1));
        testAppend(type, inputs);
    }

    @Test(dataProvider = "types")
    public void testFlatAppendDictionary(TestType type)
    {
        List<BlockView> inputs = ImmutableList.of(
                input(notNullBlock(type, 2), 0, 1),
                input(dictionaryBlock(type, 3, 4, 0), 0, 1));
        testAppend(type, inputs);
    }

    @Test(dataProvider = "types")
    public void testDictionaryAppendDifferentDictionary(TestType type)
    {
        List<BlockView> dictionaryInputs = ImmutableList.of(
                input(dictionaryBlock(type, 3, 4, 0), 0, 1),
                input(dictionaryBlock(type, 2, 4, 0), 0, 1));
        testAppend(type, dictionaryInputs);
    }

    @Test(dataProvider = "types")
    public void testDictionarySingleThenFlat(TestType type)
    {
        BlockView firstInput = input(dictionaryBlock(type, 1, 4, 0), 0);
        BlockView secondInput = input(dictionaryBlock(type, 2, 4, 0), 0, 1);
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        long initialRetainedSize = positionsAppender.getRetainedSizeInBytes();

        firstInput.positions().forEach((int position) -> positionsAppender.append(position, firstInput.block()));
        positionsAppender.append(secondInput.positions(), secondInput.block());

        assertBuildResult(type, ImmutableList.of(firstInput, secondInput), positionsAppender, initialRetainedSize);
    }

    @Test(dataProvider = "types")
    public void testConsecutiveBuilds(TestType type)
    {
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        // empty block
        positionsAppender.append(positions(), emptyBlock(type));
        assertThat(positionsAppender.build().getPositionCount()).isEqualTo(0);

        Block block = createRandomBlockForType(type, 2, 0.5f);
        // append only null position
        int nullPosition = block.isNull(0) ? 0 : 1;
        positionsAppender.append(positions(nullPosition), block);
        Block actualNullBlock = positionsAppender.build();
        assertThat(actualNullBlock.getPositionCount()).isEqualTo(1);
        assertThat(actualNullBlock.isNull(0)).isTrue();

        // append null and not null position
        positionsAppender.append(allPositions(2), block);
        assertBlockEquals(type.getType(), positionsAppender.build(), block);

        // append not null rle
        Block rleBlock = rleBlock(type, 10);
        positionsAppender.append(allPositions(10), rleBlock);
        assertBlockEquals(type.getType(), positionsAppender.build(), rleBlock);

        // append null rle
        Block nullRleBlock = nullRleBlock(type, 10);
        positionsAppender.append(allPositions(10), nullRleBlock);
        assertBlockEquals(type.getType(), positionsAppender.build(), nullRleBlock);

        // append dictionary
        Block dictionaryBlock = dictionaryBlock(type, 10, 5, 0);
        positionsAppender.append(allPositions(10), dictionaryBlock);
        assertBlockEquals(type.getType(), positionsAppender.build(), dictionaryBlock);

        // just build to confirm appender was reset
        assertThat(positionsAppender.build().getPositionCount()).isEqualTo(0);
    }

    // testcase for jit bug described https://github.com/trinodb/trino/issues/12821.
    // this test needs to be run first (hence the lowest priority) as the test order
    // influences jit compilation, making this problem to not occur if other tests are run first.
    @Test(priority = Integer.MIN_VALUE)
    public void testSliceRle()
    {
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(VARCHAR, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        // first append some not empty value to avoid RleAwarePositionsAppender for the empty value
        positionsAppender.appendRle(singleValueBlock("some value"), 1);
        // append empty value multiple times to trigger jit compilation
        ValueBlock emptyStringBlock = singleValueBlock("");
        for (int i = 0; i < 1000; i++) {
            positionsAppender.appendRle(emptyStringBlock, 2000);
        }
    }

    @Test
    public void testRowWithNestedFields()
    {
        RowType type = anonymousRow(BIGINT, BIGINT, VARCHAR);
        Block rowBLock = RowBlock.fromFieldBlocks(2, new Block[] {
                notNullBlock(TestType.BIGINT, 2),
                dictionaryBlock(TestType.BIGINT, 2, 2, 0.5F),
                rleBlock(TestType.VARCHAR, 2)
        });

        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        positionsAppender.append(allPositions(2), rowBLock);
        Block actual = positionsAppender.build();

        assertBlockEquals(type, actual, rowBLock);
    }

    @DataProvider(name = "complexTypesWithNullElementBlock")
    public static Object[][] complexTypesWithNullElementBlock()
    {
        return new Object[][] {
                {TestType.ROW_BIGINT_VARCHAR, RowBlock.fromFieldBlocks(1, new Block[] {nullBlock(BIGINT, 1), nullBlock(VARCHAR, 1)})},
                {TestType.ARRAY_BIGINT, ArrayBlock.fromElementBlock(1, Optional.empty(), new int[] {0, 1}, nullBlock(BIGINT, 1))}};
    }

    @DataProvider(name = "types")
    public static Object[][] types()
    {
        return Arrays.stream(TestType.values())
                .filter(testType -> testType != TestType.UNKNOWN)
                .map(type -> new Object[] {type})
                .toArray(Object[][]::new);
    }

    private static ValueBlock singleValueBlock(String value)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(value));
        return blockBuilder.buildValueBlock();
    }

    private static IntArrayList allPositions(int count)
    {
        return new IntArrayList(IntStream.range(0, count).toArray());
    }

    private static BlockView input(Block block, int... positions)
    {
        return new BlockView(block, new IntArrayList(positions));
    }

    private static IntArrayList positions(int... positions)
    {
        return new IntArrayList(positions);
    }

    private static Block dictionaryBlock(Block dictionary, int positionCount)
    {
        return createRandomDictionaryBlock(dictionary, positionCount);
    }

    private static Block dictionaryBlock(Block dictionary, int[] ids)
    {
        return DictionaryBlock.create(ids.length, dictionary, ids);
    }

    private static Block dictionaryBlock(TestType type, int positionCount, int dictionarySize, float nullRate)
    {
        Block dictionary = createRandomBlockForType(type, dictionarySize, nullRate);
        return createRandomDictionaryBlock(dictionary, positionCount);
    }

    private static RunLengthEncodedBlock rleBlock(Block value, int positionCount)
    {
        checkArgument(positionCount >= 2);
        return (RunLengthEncodedBlock) RunLengthEncodedBlock.create(value, positionCount);
    }

    private static RunLengthEncodedBlock rleBlock(TestType type, int positionCount)
    {
        checkArgument(positionCount >= 2);
        Block rleValue = createRandomBlockForType(type, 1, 0);
        return (RunLengthEncodedBlock) RunLengthEncodedBlock.create(rleValue, positionCount);
    }

    private static RunLengthEncodedBlock nullRleBlock(TestType type, int positionCount)
    {
        checkArgument(positionCount >= 2);
        Block rleValue = nullBlock(type, 1);
        return (RunLengthEncodedBlock) RunLengthEncodedBlock.create(rleValue, positionCount);
    }

    private static Block partiallyNullBlock(TestType type, int positionCount)
    {
        return createRandomBlockForType(type, positionCount, 0.5F);
    }

    private static Block notNullBlock(TestType type, int positionCount)
    {
        return createRandomBlockForType(type, positionCount, 0);
    }

    private static Block nullBlock(TestType type, int positionCount)
    {
        BlockBuilder blockBuilder = type.getType().createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.appendNull();
        }
        return type.adapt(blockBuilder.build());
    }

    private static Block nullBlock(Type type, int positionCount)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.build();
    }

    private static Block emptyBlock(TestType type)
    {
        return type.adapt(type.getType().createBlockBuilder(null, 0).build());
    }

    private static Block createRandomBlockForType(TestType type, int positionCount, float nullRate)
    {
        return type.adapt(BlockAssertions.createRandomBlockForType(type.getType(), positionCount, nullRate));
    }

    private static void testNullRle(Type type, Block source)
    {
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        // extract null positions
        IntArrayList positions = new IntArrayList(source.getPositionCount());
        for (int i = 0; i < source.getPositionCount(); i++) {
            if (source.isNull(i)) {
                positions.add(i);
            }
        }
        // append twice to trigger RleAwarePositionsAppender.equalOperator call
        positionsAppender.append(positions, source);
        positionsAppender.append(positions, source);
        Block actual = positionsAppender.build();
        assertThat(actual.isNull(0)).isTrue();
        assertThat(actual.getPositionCount()).isEqualTo(positions.size() * 2);
        assertInstanceOf(actual, RunLengthEncodedBlock.class);
    }

    private static void testAppend(TestType type, List<BlockView> inputs)
    {
        testAppendBatch(type, inputs);
        testAppendSingle(type, inputs);
    }

    private static void testAppendBatch(TestType type, List<BlockView> inputs)
    {
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        long initialRetainedSize = positionsAppender.getRetainedSizeInBytes();

        inputs.forEach(input -> positionsAppender.append(input.positions(), input.block()));
        assertBuildResult(type, inputs, positionsAppender, initialRetainedSize);
    }

    private static void assertBuildResult(TestType type, List<BlockView> inputs, UnnestingPositionsAppender positionsAppender, long initialRetainedSize)
    {
        long sizeInBytes = positionsAppender.getSizeInBytes();
        assertGreaterThanOrEqual(positionsAppender.getRetainedSizeInBytes(), sizeInBytes);
        Block actual = positionsAppender.build();

        assertBlockIsValid(actual, sizeInBytes, type.getType(), inputs);
        // verify positionsAppender reset
        assertThat(positionsAppender.getSizeInBytes()).isEqualTo(0);
        assertThat(positionsAppender.getRetainedSizeInBytes()).isEqualTo(initialRetainedSize);
        Block secondBlock = positionsAppender.build();
        assertThat(secondBlock.getPositionCount()).isEqualTo(0);
    }

    private static void testAppendSingle(TestType type, List<BlockView> inputs)
    {
        UnnestingPositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        long initialRetainedSize = positionsAppender.getRetainedSizeInBytes();

        inputs.forEach(input -> input.positions().forEach((int position) -> positionsAppender.append(position, input.block())));
        long sizeInBytes = positionsAppender.getSizeInBytes();
        assertGreaterThanOrEqual(positionsAppender.getRetainedSizeInBytes(), sizeInBytes);
        Block actual = positionsAppender.build();

        assertBlockIsValid(actual, sizeInBytes, type.getType(), inputs);
        // verify positionsAppender reset
        assertThat(positionsAppender.getSizeInBytes()).isEqualTo(0);
        assertThat(positionsAppender.getRetainedSizeInBytes()).isEqualTo(initialRetainedSize);
        Block secondBlock = positionsAppender.build();
        assertThat(secondBlock.getPositionCount()).isEqualTo(0);
    }

    private static void assertBlockIsValid(Block actual, long sizeInBytes, Type type, List<BlockView> inputs)
    {
        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus();
        BlockBuilderStatus blockBuilderStatus = pageBuilderStatus.createBlockBuilderStatus();
        Block expected = buildBlock(type, inputs, blockBuilderStatus);

        assertBlockEquals(type, actual, expected);
        assertThat(sizeInBytes).isEqualTo(pageBuilderStatus.getSizeInBytes());
    }

    private static Block buildBlock(Type type, List<BlockView> inputs, BlockBuilderStatus blockBuilderStatus)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(blockBuilderStatus, 10);
        for (BlockView input : inputs) {
            for (int position : input.positions()) {
                type.appendTo(input.block(), position, blockBuilder);
            }
        }
        return blockBuilder.build();
    }

    private enum TestType
    {
        BIGINT(BigintType.BIGINT),
        BOOLEAN(BooleanType.BOOLEAN),
        INTEGER(IntegerType.INTEGER),
        CHAR_10(createCharType(10)),
        VARCHAR(createUnboundedVarcharType()),
        DOUBLE(DoubleType.DOUBLE),
        SMALLINT(SmallintType.SMALLINT),
        TINYINT(TinyintType.TINYINT),
        VARBINARY(VarbinaryType.VARBINARY),
        LONG_DECIMAL(createDecimalType(Decimals.MAX_SHORT_PRECISION + 1)),
        LONG_TIMESTAMP(createTimestampType(9)),
        ROW_BIGINT_VARCHAR(anonymousRow(BigintType.BIGINT, VarcharType.VARCHAR)),
        ARRAY_BIGINT(new ArrayType(BigintType.BIGINT)),
        VARCHAR_WITH_TEST_BLOCK(VarcharType.VARCHAR, adaptation()),
        UNKNOWN(UnknownType.UNKNOWN);

        private final Type type;
        private final Function<Block, Block> blockAdaptation;

        TestType(Type type)
        {
            this(type, Function.identity());
        }

        TestType(Type type, Function<Block, Block> blockAdaptation)
        {
            this.type = requireNonNull(type, "type is null");
            this.blockAdaptation = requireNonNull(blockAdaptation, "blockAdaptation is null");
        }

        public Block adapt(Block block)
        {
            return blockAdaptation.apply(block);
        }

        public Type getType()
        {
            return type;
        }
    }

    private record BlockView(Block block, IntArrayList positions)
    {
        private BlockView
        {
            requireNonNull(block, "block is null");
            requireNonNull(positions, "positions is null");
        }
    }

    private static Function<Block, Block> adaptation()
    {
        return TestPositionsAppender::adapt;
    }

    private static Block adapt(Block block)
    {
        if (block instanceof RunLengthEncodedBlock) {
            checkArgument(block.getPositionCount() == 0 || block.isNull(0));
            return RunLengthEncodedBlock.create(new VariableWidthBlock(1, EMPTY_SLICE, new int[] {0, 0}, Optional.of(new boolean[] {true})), block.getPositionCount());
        }

        int[] offsets = new int[block.getPositionCount() + 1];
        boolean[] valueIsNull = new boolean[block.getPositionCount()];
        boolean hasNullValue = false;
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                valueIsNull[i] = true;
                hasNullValue = true;
                offsets[i + 1] = offsets[i];
            }
            else {
                offsets[i + 1] = offsets[i] + block.getSliceLength(i);
            }
        }

        return new VariableWidthBlock(block.getPositionCount(), ((VariableWidthBlock) block).getRawSlice(), offsets, hasNullValue ? Optional.of(valueIsNull) : Optional.empty());
    }
}
