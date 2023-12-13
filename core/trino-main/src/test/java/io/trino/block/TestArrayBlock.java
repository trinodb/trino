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
package io.trino.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestArrayBlock
        extends AbstractTestBlock
{
    private static final int[] ARRAY_SIZES = new int[] {16, 0, 13, 1, 2, 11, 4, 7};

    @Test
    public void testWithFixedWidthBlock()
    {
        long[][] expectedValues = new long[ARRAY_SIZES.length][];
        Random rand = new Random(47);
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = rand.longs(ARRAY_SIZES[i]).toArray();
        }

        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
        assertBlockFilteredPositions(expectedValues, block, 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, block, 2, 3, 5, 6);

        long[][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        Block blockWithNull = createBlockBuilderWithValues(expectedValuesWithNull).build();
        assertBlock(blockWithNull, expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 2, 3, 4, 9, 13, 14);
    }

    @Test
    public void testWithVariableWidthBlock()
    {
        Slice[][] expectedValues = new Slice[ARRAY_SIZES.length][];
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = new Slice[ARRAY_SIZES[i]];
            for (int j = 0; j < ARRAY_SIZES[i]; j++) {
                expectedValues[i][j] = Slices.utf8Slice(format("%d.%d", i, j));
            }
        }

        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
        assertBlockFilteredPositions(expectedValues, block, 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, block, 2, 3, 5, 6);

        Slice[][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        Block blockWithNull = createBlockBuilderWithValues(expectedValuesWithNull).build();
        assertBlock(blockWithNull, expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 2, 3, 4, 9, 13, 14);
    }

    @Test
    public void testWithArrayBlock()
    {
        long[][][] expectedValues = createExpectedValues();

        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
        assertBlockFilteredPositions(expectedValues, block, 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, block, 2, 3, 5, 6);

        long[][][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        Block blockWithNull = createBlockBuilderWithValues(expectedValuesWithNull).build();
        assertBlock(blockWithNull, expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockWithNull, 2, 3, 4, 9, 13, 14);
    }

    private static long[][][] createExpectedValues()
    {
        long[][][] expectedValues = new long[ARRAY_SIZES.length][][];
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = new long[ARRAY_SIZES[i]][];
            for (int j = 1; j < ARRAY_SIZES[i]; j++) {
                if ((i + j) % 5 == 0) {
                    expectedValues[i][j] = null;
                }
                else {
                    expectedValues[i][j] = new long[] {i, j, i + j};
                }
            }
        }
        return expectedValues;
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        long[][] expectedValues = new long[ARRAY_SIZES.length][];
        Random rand = new Random(47);
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = rand.longs(ARRAY_SIZES[i]).toArray();
        }
        ArrayBlockBuilder emptyBlockBuilder = new ArrayBlockBuilder(BIGINT, null, 0, 0);

        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, 100, 100);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertThat(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes()).isTrue();
        assertThat(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes()).isTrue();

        blockBuilder = blockBuilder.newBlockBuilderLike(null);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        long[][][] expectedValues = alternatingNullValues(createExpectedValues());
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertThat(block.getPositionCount()).isEqualTo(expectedValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = getExpectedEstimatedDataSize(expectedValues[i]);
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(expectedSize);
        }
    }

    private static int getExpectedEstimatedDataSize(long[][] values)
    {
        if (values == null) {
            return 0;
        }
        int size = 0;
        for (long[] value : values) {
            if (value != null) {
                size += Long.BYTES * value.length;
            }
        }
        return size;
    }

    @Test
    public void testCompactBlock()
    {
        Block emptyValueBlock = new ByteArrayBlock(0, Optional.empty(), new byte[0]);
        Block compactValueBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(16).getBytes());
        Block inCompactValueBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(17).getBytes());
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(fromElementBlock(0, Optional.empty(), new int[1], emptyValueBlock));
        testCompactBlock(fromElementBlock(valueIsNull.length, Optional.of(valueIsNull), offsets, compactValueBlock));
        testIncompactBlock(fromElementBlock(valueIsNull.length - 1, Optional.of(valueIsNull), offsets, compactValueBlock));
        // underlying value block is not compact
        testIncompactBlock(fromElementBlock(valueIsNull.length, Optional.of(valueIsNull), offsets, inCompactValueBlock));
    }

    private static BlockBuilder createBlockBuilderWithValues(long[][][] expectedValues)
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(new ArrayBlockBuilder(BIGINT, null, 100, 100), null, 100);
        for (long[][] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry(elementBuilder -> {
                    for (long[] values : expectedValue) {
                        if (values == null) {
                            elementBuilder.appendNull();
                        }
                        else {
                            ((ArrayBlockBuilder) elementBuilder).buildEntry(innerBuilder -> Arrays.stream(values).forEach(value -> BIGINT.writeLong(innerBuilder, value)));
                        }
                    }
                });
            }
        }
        return blockBuilder;
    }

    private static BlockBuilder createBlockBuilderWithValues(long[][] expectedValues)
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, 100, 100);
        return writeValues(expectedValues, blockBuilder);
    }

    private static BlockBuilder writeValues(long[][] expectedValues, BlockBuilder blockBuilder)
    {
        for (long[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                    for (long v : expectedValue) {
                        BIGINT.writeLong(elementBuilder, v);
                    }
                });
            }
        }
        return blockBuilder;
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[][] expectedValues)
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(VARCHAR, null, 100, 100);
        for (Slice[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry(elementBuilder -> {
                    for (Slice v : expectedValue) {
                        VARCHAR.writeSlice(elementBuilder, v);
                    }
                });
            }
        }
        return blockBuilder;
    }
}
