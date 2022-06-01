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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;

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
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRandomDictionaryBlock;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.block.BlockAssertions.createSmallintsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTinyintsBlock;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPositionsAppender
{
    private static final PositionsAppenderFactory POSITIONS_APPENDER_FACTORY = new PositionsAppenderFactory(new BlockTypeOperators());

    @Test(dataProvider = "types")
    public void testMixedBlockTypes(Type type)
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
                input(dictionaryBlock(rleBlock(type, 4), 3), 0, 2)); // dict -> rle

        testAppend(type, input);
    }

    @Test(dataProvider = "nullRleTypes")
    public void testNullRle(Type type)
    {
        testNullRle(type, nullBlock(type, 2));
        testNullRle(type, nullRleBlock(type, 2));
    }

    @Test(dataProvider = "types")
    public void testRleSwitchToFlat(Type type)
    {
        List<BlockView> inputs = ImmutableList.of(
                input(rleBlock(type, 3), 0, 1),
                input(notNullBlock(type, 2), 0, 1));
        testAppend(type, inputs);

        List<BlockView> dictionaryInputs = ImmutableList.of(
                input(rleBlock(type, 3), 0, 1),
                input(dictionaryBlock(type, 2, 4, 0.5F), 0, 1));
        testAppend(type, dictionaryInputs);
    }

    @Test(dataProvider = "types")
    public void testFlatAppendRle(Type type)
    {
        List<BlockView> inputs = ImmutableList.of(
                input(notNullBlock(type, 2), 0, 1),
                input(rleBlock(type, 3), 0, 1));
        testAppend(type, inputs);

        List<BlockView> dictionaryInputs = ImmutableList.of(
                input(dictionaryBlock(type, 2, 4, 0.5F), 0, 1),
                input(rleBlock(type, 3), 0, 1));
        testAppend(type, dictionaryInputs);
    }

    @Test(dataProvider = "differentValues")
    public void testMultipleRleBlocksWithDifferentValues(Type type, Block value1, Block value2)
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
                        {BIGINT, createLongsBlock(0), createLongsBlock(1)},
                        {BOOLEAN, createBooleansBlock(true), createBooleansBlock(false)},
                        {INTEGER, createIntsBlock(0), createIntsBlock(1)},
                        {createCharType(10), createStringsBlock("0"), createStringsBlock("1")},
                        {createUnboundedVarcharType(), createStringsBlock("0"), createStringsBlock("1")},
                        {DOUBLE, createDoublesBlock(0D), createDoublesBlock(1D)},
                        {SMALLINT, createSmallintsBlock(0), createSmallintsBlock(1)},
                        {TINYINT, createTinyintsBlock(0), createTinyintsBlock(1)},
                        {VARBINARY, createSlicesBlock(Slices.wrappedLongArray(0)), createSlicesBlock(Slices.wrappedLongArray(1))},
                        {createDecimalType(Decimals.MAX_SHORT_PRECISION + 1), createLongDecimalsBlock("0"), createLongDecimalsBlock("1")},
                        {new ArrayType(BIGINT), createArrayBigintBlock(ImmutableList.of(ImmutableList.of(0L))), createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L)))},
                        {
                                createTimestampType(9),
                                createLongTimestampBlock(createTimestampType(9), new LongTimestamp(0, 0)),
                                createLongTimestampBlock(createTimestampType(9), new LongTimestamp(1, 0))}
                };
    }

    @Test(dataProvider = "types")
    public void testMultipleRleWithTheSameValueProduceRle(Type type)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        Block value = notNullBlock(type, 1);
        positionsAppender.append(allPositions(3), rleBlock(value, 3));
        positionsAppender.append(allPositions(2), rleBlock(value, 2));

        Block actual = positionsAppender.build();
        assertEquals(actual.getPositionCount(), 5);
        assertInstanceOf(actual, RunLengthEncodedBlock.class);
    }

    @DataProvider(name = "nullRleTypes")
    public static Object[][] nullRleTypes()
    {
        return new Object[][]
                {
                        {BIGINT},
                        {BOOLEAN},
                        {INTEGER},
                        {createCharType(10)},
                        {createUnboundedVarcharType()},
                        {DOUBLE},
                        {SMALLINT},
                        {TINYINT},
                        {VARBINARY},
                        {createDecimalType(Decimals.MAX_SHORT_PRECISION + 1)},
                        {createTimestampType(9)}
                };
    }

    @DataProvider(name = "types")
    public static Object[][] types()
    {
        return new Object[][]
                {
                        {BIGINT},
                        {BOOLEAN},
                        {INTEGER},
                        {createCharType(10)},
                        {createUnboundedVarcharType()},
                        {DOUBLE},
                        {SMALLINT},
                        {TINYINT},
                        {VARBINARY},
                        {createDecimalType(Decimals.MAX_SHORT_PRECISION + 1)},
                        {new ArrayType(BIGINT)},
                        {createTimestampType(9)}
                };
    }

    private IntArrayList allPositions(int count)
    {
        return new IntArrayList(IntStream.range(0, count).toArray());
    }

    private BlockView input(Block block, int... positions)
    {
        return new BlockView(block, new IntArrayList(positions));
    }

    private DictionaryBlock dictionaryBlock(Block dictionary, int positionCount)
    {
        return createRandomDictionaryBlock(dictionary, positionCount);
    }

    private DictionaryBlock dictionaryBlock(Block dictionary, int[] ids)
    {
        return new DictionaryBlock(0, ids.length, dictionary, ids, false, randomDictionaryId());
    }

    private DictionaryBlock dictionaryBlock(Type type, int positionCount, int dictionarySize, float nullRate)
    {
        Block dictionary = createRandomBlockForType(type, dictionarySize, nullRate);
        return createRandomDictionaryBlock(dictionary, positionCount);
    }

    private RunLengthEncodedBlock rleBlock(Block value, int positionCount)
    {
        return new RunLengthEncodedBlock(value, positionCount);
    }

    private RunLengthEncodedBlock rleBlock(Type type, int positionCount)
    {
        Block rleValue = createRandomBlockForType(type, 1, 0);
        return new RunLengthEncodedBlock(rleValue, positionCount);
    }

    private RunLengthEncodedBlock nullRleBlock(Type type, int positionCount)
    {
        Block rleValue = nullBlock(type, 1);
        return new RunLengthEncodedBlock(rleValue, positionCount);
    }

    private Block partiallyNullBlock(Type type, int positionCount)
    {
        return createRandomBlockForType(type, positionCount, 0.5F);
    }

    private Block notNullBlock(Type type, int positionCount)
    {
        return createRandomBlockForType(type, positionCount, 0);
    }

    private Block nullBlock(Type type, int positionCount)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.build();
    }

    private Block emptyBlock(Type type)
    {
        return type.createBlockBuilder(null, 0).build();
    }

    private void testNullRle(Type type, Block source)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        // append twice to trigger RleAwarePositionsAppender.equalOperator call
        positionsAppender.append(new IntArrayList(IntStream.range(0, source.getPositionCount()).toArray()), source);
        positionsAppender.append(new IntArrayList(IntStream.range(0, source.getPositionCount()).toArray()), source);
        Block actual = positionsAppender.build();
        assertTrue(actual.isNull(0));
        assertEquals(actual.getPositionCount(), source.getPositionCount() * 2);
        assertInstanceOf(actual, RunLengthEncodedBlock.class);
    }

    private void testAppend(Type type, List<BlockView> inputs)
    {
        PositionsAppender positionsAppender = POSITIONS_APPENDER_FACTORY.create(type, 10, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        long initialRetainedSize = positionsAppender.getRetainedSizeInBytes();

        inputs.forEach(input -> positionsAppender.append(input.getPositions(), input.getBlock()));
        long sizeInBytes = positionsAppender.getSizeInBytes();
        assertGreaterThanOrEqual(positionsAppender.getRetainedSizeInBytes(), sizeInBytes);
        Block actual = positionsAppender.build();

        assertBlockIsValid(actual, sizeInBytes, type, inputs);
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
}
