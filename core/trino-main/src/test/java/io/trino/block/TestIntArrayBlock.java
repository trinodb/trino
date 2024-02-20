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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.IntArrayBlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIntArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Integer[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
    {
        Integer[] expectedValues = alternatingNullValues(createTestValue(17));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Integer[] expectedValues = createTestValue(100);
        BlockBuilder emptyBlockBuilder = new IntArrayBlockBuilder(null, 0);

        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, expectedValues.length);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertThat(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes()).isTrue();
        assertThat(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes()).isTrue();

        blockBuilder = (IntArrayBlockBuilder) blockBuilder.newBlockBuilderLike(null);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(createTestValue(100));
        Block block = blockBuilder.build();
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(Integer.BYTES);
        }

        assertThat(new IntArrayBlockBuilder(null, 22).appendNull().build().getEstimatedDataSizeForStats(0)).isEqualTo(0);
    }

    @Test
    public void testCompactBlock()
    {
        int[] intArray = {0, 0, 1, 2, 3, 4};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new IntArrayBlock(0, Optional.empty(), new int[0]));
        testCompactBlock(new IntArrayBlock(intArray.length, Optional.of(valueIsNull), intArray));
        testNotCompactBlock(new IntArrayBlock(intArray.length - 1, Optional.of(valueIsNull), intArray));
    }

    private void assertFixedWithValues(Integer[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Integer[] expectedValues)
    {
        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Integer[] expectedValues, IntArrayBlockBuilder blockBuilder)
    {
        for (Integer expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeInt(expectedValue.intValue());
            }
        }
    }

    private static Integer[] createTestValue(int positionCount)
    {
        Integer[] expectedValues = new Integer[positionCount];
        Random random = new Random(0);
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = random.nextInt();
        }
        return expectedValues;
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertThat(block.isNull(position)).isTrue();
            return;
        }

        assertThat(block.isNull(position)).isFalse();
        assertThat(((IntArrayBlock) block).getInt(position)).isEqualTo(((Integer) expectedValue).intValue());
    }
}
