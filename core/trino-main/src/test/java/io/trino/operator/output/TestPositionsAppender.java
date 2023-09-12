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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.block.BlockAssertions;
import io.trino.spi.block.AbstractVariableWidthBlock;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
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
import jakarta.annotation.Nullable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPositionsAppender
{
    private static final PositionsAppenderFactory POSITIONS_APPENDER_FACTORY = new PositionsAppenderFactory(new BlockTypeOperators());

    @Test(dataProvider = "types")
    public void testMixedBlockTypes(TestType type)
    {
        List<BlockView> input = ImmutableList.of(
                input(emptyBlock(type)),
                input(nullBlock(type, 3), 0, 2),
                input(nullBlock(TestType.UNKNOWN, 3), 0, 2), // a := null projections are handled by UnknownType null block
                input(nullBlock(TestType.UNKNOWN, 1), 0), // a := null projections are handled by UnknownType null block, 1 position uses non RLE block
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
                        {TestType.DOUBLE, createDoublesBlock(0D), createDoublesBlock(1D)},
                        {TestType.SMALLINT, createSmallintsBlock(0), createSmallintsBlock(1)},
                        {TestType.TINYINT, createTinyintsBlock(0), createTinyintsBlock(1)},
                        {TestType.VARBINARY, createSlicesBlock(Slices.allocate(Long.BYTES)), createSlicesBlock(Slices.allocate(Long.BYTES).getOutput().appendLong(1).slice())},
                        {TestType.LONG_DECIMAL, createLongDecimalsBlock("0"), createLongDecimalsBlock("1")},
                        {TestType.ARRAY_BIGINT, createArrayBigintBlock(ImmutableList.of(ImmutableList.of(0L))), createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L)))},
                        {TestType.LONG_TIMESTAMP, createLongTimestampBlock(createTimestampType(9), new LongTimestamp(0, 0)),
                                createLongTimestampBlock(createTimestampType(9), new LongTimestamp(1, 0))},
                        {TestType.VARCHAR_WITH_TEST_BLOCK, TestVariableWidthBlock.adapt(createStringsBlock("0")), TestVariableWidthBlock.adapt(createStringsBlock("1"))}
                };
    }

    @Test(dataProvider = "types")
    public void testMultipleRleWithTheSameValueProduceRle(TestType type)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        Block value = notNullBlock(type, 1);
        positionsAppender.append(allPositions(3), rleBlock(value, 3));
        positionsAppender.append(allPositions(2), rleBlock(value, 2));

        Block actual = positionsAppender.build();
        assertEquals(actual.getPositionCount(), 5);
        assertInstanceOf(actual, RunLengthEncodedBlock.class);
    }

    @Test(dataProvider = "complexTypesWithNullElementBlock")
    public void testRleAppendForComplexTypeWithNullElement(TestType type, Block value)
    {
        checkArgument(value.getPositionCount() == 1);
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        positionsAppender.append(allPositions(3), rleBlock(value, 3));
        positionsAppender.append(allPositions(2), rleBlock(value, 2));

        Block actual = positionsAppender.build();
        assertEquals(actual.getPositionCount(), 5);
        assertInstanceOf(actual, RunLengthEncodedBlock.class);
        assertBlockEquals(type.getType(), actual, RunLengthEncodedBlock.create(value, 5));
    }

    @Test(dataProvider = "types")
    public void testRleAppendedWithSinglePositionDoesNotProduceRle(TestType type)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        Block value = notNullBlock(type, 1);
        positionsAppender.append(allPositions(3), rleBlock(value, 3));
        positionsAppender.append(allPositions(2), rleBlock(value, 2));
        positionsAppender.append(0, rleBlock(value, 2));

        Block actual = positionsAppender.build();
        assertEquals(actual.getPositionCount(), 6);
        assertFalse(actual instanceof RunLengthEncodedBlock, actual.getClass().getSimpleName());
    }

    @Test(dataProvider = "types")
    public void testMultipleTheSameDictionariesProduceDictionary(TestType type)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        testMultipleTheSameDictionariesProduceDictionary(type, positionsAppender);
        // test if appender can accept different dictionary after a build
        testMultipleTheSameDictionariesProduceDictionary(type, positionsAppender);
    }

    private void testMultipleTheSameDictionariesProduceDictionary(TestType type, PositionsAppender positionsAppender)
    {
        Block dictionary = createRandomBlockForType(type, 4, 0);
        positionsAppender.append(allPositions(3), createRandomDictionaryBlock(dictionary, 3));
        positionsAppender.append(allPositions(2), createRandomDictionaryBlock(dictionary, 2));

        Block actual = positionsAppender.build();
        assertEquals(actual.getPositionCount(), 5);
        assertInstanceOf(actual, DictionaryBlock.class);
        assertEquals(((DictionaryBlock) actual).getDictionary(), dictionary);
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
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        long initialRetainedSize = positionsAppender.getRetainedSizeInBytes();

        firstInput.getPositions().forEach((int position) -> positionsAppender.append(position, firstInput.getBlock()));
        positionsAppender.append(secondInput.getPositions(), secondInput.getBlock());

        assertBuildResult(type, ImmutableList.of(firstInput, secondInput), positionsAppender, initialRetainedSize);
    }

    @Test(dataProvider = "types")
    public void testConsecutiveBuilds(TestType type)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        // empty block
        positionsAppender.append(positions(), emptyBlock(type));
        assertEquals(positionsAppender.build().getPositionCount(), 0);

        Block block = createRandomBlockForType(type, 2, 0.5f);
        // append only null position
        int nullPosition = block.isNull(0) ? 0 : 1;
        positionsAppender.append(positions(nullPosition), block);
        Block actualNullBlock = positionsAppender.build();
        assertEquals(actualNullBlock.getPositionCount(), 1);
        assertTrue(actualNullBlock.isNull(0));

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
        assertEquals(positionsAppender.build().getPositionCount(), 0);
    }

    // testcase for jit bug described https://github.com/trinodb/trino/issues/12821.
    // this test needs to be run first (hence lowest priority) as order of tests
    // influence jit compilation making this problem to not occur if other tests are run first.
    @Test(priority = Integer.MIN_VALUE)
    public void testSliceRle()
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(VARCHAR, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        // first append some not empty value to avoid RleAwarePositionsAppender for the empty value
        positionsAppender.appendRle(singleValueBlock("some value"), 1);
        // append empty value multiple times to trigger jit compilation
        Block emptyStringBlock = singleValueBlock("");
        for (int i = 0; i < 1000; i++) {
            positionsAppender.appendRle(emptyStringBlock, 2000);
        }
    }

    @Test
    public void testRowWithNestedFields()
    {
        RowType type = anonymousRow(BIGINT, BIGINT, VARCHAR);
        Block rowBLock = RowBlock.fromFieldBlocks(2, Optional.empty(), new Block[] {
                notNullBlock(TestType.BIGINT, 2),
                dictionaryBlock(TestType.BIGINT, 2, 2, 0.5F),
                rleBlock(TestType.VARCHAR, 2)
        });

        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        positionsAppender.append(allPositions(2), rowBLock);
        Block actual = positionsAppender.build();

        assertBlockEquals(type, actual, rowBLock);
    }

    @DataProvider(name = "complexTypesWithNullElementBlock")
    public static Object[][] complexTypesWithNullElementBlock()
    {
        return new Object[][] {
                {TestType.ROW_BIGINT_VARCHAR, RowBlock.fromFieldBlocks(1, Optional.empty(), new Block[] {nullBlock(BIGINT, 1), nullBlock(VARCHAR, 1)})},
                {TestType.ARRAY_BIGINT, ArrayBlock.fromElementBlock(1, Optional.empty(), new int[] {0, 1}, nullBlock(BIGINT, 1))}};
    }

    @DataProvider(name = "types")
    public static Object[][] types()
    {
        return Arrays.stream(TestType.values())
                .filter(testType -> !testType.equals(TestType.UNKNOWN))
                .map(type -> new Object[] {type})
                .toArray(Object[][]::new);
    }

    private static Block singleValueBlock(String value)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(value));
        return blockBuilder.build();
    }

    private IntArrayList allPositions(int count)
    {
        return new IntArrayList(IntStream.range(0, count).toArray());
    }

    private BlockView input(Block block, int... positions)
    {
        return new BlockView(block, new IntArrayList(positions));
    }

    private static IntArrayList positions(int... positions)
    {
        return new IntArrayList(positions);
    }

    private Block dictionaryBlock(Block dictionary, int positionCount)
    {
        return createRandomDictionaryBlock(dictionary, positionCount);
    }

    private Block dictionaryBlock(Block dictionary, int[] ids)
    {
        return DictionaryBlock.create(ids.length, dictionary, ids);
    }

    private Block dictionaryBlock(TestType type, int positionCount, int dictionarySize, float nullRate)
    {
        Block dictionary = createRandomBlockForType(type, dictionarySize, nullRate);
        return createRandomDictionaryBlock(dictionary, positionCount);
    }

    private RunLengthEncodedBlock rleBlock(Block value, int positionCount)
    {
        checkArgument(positionCount >= 2);
        return (RunLengthEncodedBlock) RunLengthEncodedBlock.create(value, positionCount);
    }

    private RunLengthEncodedBlock rleBlock(TestType type, int positionCount)
    {
        checkArgument(positionCount >= 2);
        Block rleValue = createRandomBlockForType(type, 1, 0);
        return (RunLengthEncodedBlock) RunLengthEncodedBlock.create(rleValue, positionCount);
    }

    private RunLengthEncodedBlock nullRleBlock(TestType type, int positionCount)
    {
        checkArgument(positionCount >= 2);
        Block rleValue = nullBlock(type, 1);
        return (RunLengthEncodedBlock) RunLengthEncodedBlock.create(rleValue, positionCount);
    }

    private Block partiallyNullBlock(TestType type, int positionCount)
    {
        return createRandomBlockForType(type, positionCount, 0.5F);
    }

    private Block notNullBlock(TestType type, int positionCount)
    {
        return createRandomBlockForType(type, positionCount, 0);
    }

    private Block nullBlock(TestType type, int positionCount)
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

    private Block emptyBlock(TestType type)
    {
        return type.adapt(type.getType().createBlockBuilder(null, 0).build());
    }

    private Block createRandomBlockForType(TestType type, int positionCount, float nullRate)
    {
        return type.adapt(BlockAssertions.createRandomBlockForType(type.getType(), positionCount, nullRate));
    }

    private void testNullRle(Type type, Block source)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
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
        assertTrue(actual.isNull(0));
        assertEquals(actual.getPositionCount(), positions.size() * 2);
        assertInstanceOf(actual, RunLengthEncodedBlock.class);
    }

    private void testAppend(TestType type, List<BlockView> inputs)
    {
        testAppendBatch(type, inputs);
        testAppendSingle(type, inputs);
    }

    private void testAppendBatch(TestType type, List<BlockView> inputs)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        long initialRetainedSize = positionsAppender.getRetainedSizeInBytes();

        inputs.forEach(input -> positionsAppender.append(input.getPositions(), input.getBlock()));
        assertBuildResult(type, inputs, positionsAppender, initialRetainedSize);
    }

    private void assertBuildResult(TestType type, List<BlockView> inputs, PositionsAppender positionsAppender, long initialRetainedSize)
    {
        long sizeInBytes = positionsAppender.getSizeInBytes();
        assertGreaterThanOrEqual(positionsAppender.getRetainedSizeInBytes(), sizeInBytes);
        Block actual = positionsAppender.build();

        assertBlockIsValid(actual, sizeInBytes, type.getType(), inputs);
        // verify positionsAppender reset
        assertEquals(positionsAppender.getSizeInBytes(), 0);
        assertEquals(positionsAppender.getRetainedSizeInBytes(), initialRetainedSize);
        Block secondBlock = positionsAppender.build();
        assertEquals(secondBlock.getPositionCount(), 0);
    }

    private void testAppendSingle(TestType type, List<BlockView> inputs)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type.getType(), 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        long initialRetainedSize = positionsAppender.getRetainedSizeInBytes();

        inputs.forEach(input -> input.getPositions().forEach((int position) -> positionsAppender.append(position, input.getBlock())));
        long sizeInBytes = positionsAppender.getSizeInBytes();
        assertGreaterThanOrEqual(positionsAppender.getRetainedSizeInBytes(), sizeInBytes);
        Block actual = positionsAppender.build();

        assertBlockIsValid(actual, sizeInBytes, type.getType(), inputs);
        // verify positionsAppender reset
        assertEquals(positionsAppender.getSizeInBytes(), 0);
        assertEquals(positionsAppender.getRetainedSizeInBytes(), initialRetainedSize);
        Block secondBlock = positionsAppender.build();
        assertEquals(secondBlock.getPositionCount(), 0);
    }

    private void assertBlockIsValid(Block actual, long sizeInBytes, Type type, List<BlockView> inputs)
    {
        PageBuilderStatus pageBuilderStatus = new PageBuilderStatus();
        BlockBuilderStatus blockBuilderStatus = pageBuilderStatus.createBlockBuilderStatus();
        Block expected = buildBlock(type, inputs, blockBuilderStatus);

        assertBlockEquals(type, actual, expected);
        assertEquals(sizeInBytes, pageBuilderStatus.getSizeInBytes());
    }

    private Block buildBlock(Type type, List<BlockView> inputs, BlockBuilderStatus blockBuilderStatus)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(blockBuilderStatus, 10);
        for (BlockView input : inputs) {
            for (int position : input.getPositions()) {
                type.appendTo(input.getBlock(), position, blockBuilder);
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
        VARCHAR_WITH_TEST_BLOCK(VarcharType.VARCHAR, TestVariableWidthBlock.adaptation()),
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

    private static class BlockView
    {
        private final Block block;
        private final IntArrayList positions;

        private BlockView(Block block, IntArrayList positions)
        {
            this.block = requireNonNull(block, "block is null");
            this.positions = requireNonNull(positions, "positions is null");
        }

        public Block getBlock()
        {
            return block;
        }

        public IntArrayList getPositions()
        {
            return positions;
        }

        public void appendTo(PositionsAppender positionsAppender)
        {
            positionsAppender.append(getPositions(), getBlock());
        }
    }

    private static class TestVariableWidthBlock
            extends AbstractVariableWidthBlock
    {
        private static final int INSTANCE_SIZE = instanceSize(VariableWidthBlock.class);
        private final int arrayOffset;
        private final int positionCount;
        private final Slice slice;
        private final int[] offsets;
        @Nullable
        private final boolean[] valueIsNull;

        private static Function<Block, Block> adaptation()
        {
            return TestVariableWidthBlock::adapt;
        }

        private static Block adapt(Block block)
        {
            if (block instanceof RunLengthEncodedBlock) {
                checkArgument(block.getPositionCount() == 0 || block.isNull(0));
                return RunLengthEncodedBlock.create(new TestVariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new boolean[] {true}), block.getPositionCount());
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

            return new TestVariableWidthBlock(0, block.getPositionCount(), ((VariableWidthBlock) block).getRawSlice(), offsets, hasNullValue ? valueIsNull : null);
        }

        private TestVariableWidthBlock(int arrayOffset, int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull)
        {
            checkArgument(arrayOffset >= 0);
            this.arrayOffset = arrayOffset;
            checkArgument(positionCount >= 0);
            this.positionCount = positionCount;
            this.slice = requireNonNull(slice, "slice is null");
            this.offsets = offsets;
            this.valueIsNull = valueIsNull;
        }

        @Override
        protected Slice getRawSlice(int position)
        {
            return slice;
        }

        @Override
        protected int getPositionOffset(int position)
        {
            return offsets[position + arrayOffset];
        }

        @Override
        public int getSliceLength(int position)
        {
            return getPositionOffset(position + 1) - getPositionOffset(position);
        }

        @Override
        protected boolean isEntryNull(int position)
        {
            return valueIsNull != null && valueIsNull[position + arrayOffset];
        }

        @Override
        public int getPositionCount()
        {
            return positionCount;
        }

        @Override
        public Block getRegion(int positionOffset, int length)
        {
            return new TestVariableWidthBlock(positionOffset + arrayOffset, length, slice, offsets, valueIsNull);
        }

        @Override
        public Block getSingleValueBlock(int position)
        {
            if (isNull(position)) {
                return new TestVariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new boolean[] {true});
            }

            int offset = getPositionOffset(position);
            int entrySize = getSliceLength(position);

            Slice copy = getRawSlice(position).copy(offset, entrySize);

            return new TestVariableWidthBlock(0, 1, copy, new int[] {0, copy.length()}, null);
        }

        @Override
        public long getSizeInBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getRegionSizeInBytes(int position, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OptionalInt fixedSizeInBytesPerPosition()
        {
            return OptionalInt.empty();
        }

        @Override
        public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(valueIsNull) + sizeOf(offsets);
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Block copyPositions(int[] positions, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Block copyRegion(int position, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Block copyWithAppendedNull()
        {
            throw new UnsupportedOperationException();
        }
    }
}
