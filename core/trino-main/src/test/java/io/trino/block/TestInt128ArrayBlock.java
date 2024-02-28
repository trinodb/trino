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
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.Int128ArrayBlockBuilder;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.type.Int128;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestInt128ArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Int128[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
    {
        Int128[] expectedValues = alternatingNullValues(createTestValue(17));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Int128[] expectedValues = createTestValue(100);
        BlockBuilder emptyBlockBuilder = new Int128ArrayBlockBuilder(null, 0);

        BlockBuilder blockBuilder = new Int128ArrayBlockBuilder(null, expectedValues.length);
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
        BlockBuilder blockBuilder = createBlockBuilderWithValues(createTestValue(100));
        Block block = blockBuilder.build();
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(Int128.SIZE);
        }

        assertThat(new IntArrayBlockBuilder(null, 22).appendNull().build().getEstimatedDataSizeForStats(0)).isEqualTo(0);
    }

    @Test
    public void testCompactBlock()
    {
        long[] longArray = {0L, 0L, 0L, 0L, 0L, 1L, 0L, 2L, 0L, 3L, 0L, 4L};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new Int128ArrayBlock(0, Optional.empty(), new long[0]));
        testCompactBlock(new Int128ArrayBlock(valueIsNull.length, Optional.of(valueIsNull), longArray));
        testNotCompactBlock(new Int128ArrayBlock(valueIsNull.length - 2, Optional.of(valueIsNull), longArray));
    }

    private void assertFixedWithValues(Int128[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Int128[] expectedValues)
    {
        Int128ArrayBlockBuilder blockBuilder = new Int128ArrayBlockBuilder(null, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Int128[] expectedValues, BlockBuilder blockBuilder)
    {
        for (Int128 expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                ((Int128ArrayBlockBuilder) blockBuilder).writeInt128(expectedValue.getHigh(), expectedValue.getLow());
            }
        }
    }

    private static Int128[] createTestValue(int positionCount)
    {
        Int128[] expectedValues = new Int128[positionCount];
        Random random = new Random(0);
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = Int128.valueOf(random.nextLong(), random.nextLong());
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
        assertThat(((Int128ArrayBlock) block).getInt128(position)).isEqualTo(expectedValue);
    }
}
