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

import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.BitArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBitArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Boolean[] expectedValues = createTestValue(67);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
    {
        Boolean[] expectedValues = alternatingNullValues(createTestValue(67));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16, 63, 64, 66);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Boolean[] expectedValues = createTestValue(100);
        BlockBuilder emptyBlockBuilder = new BitArrayBlockBuilder(null, 0);

        BitArrayBlockBuilder blockBuilder = new BitArrayBlockBuilder(null, expectedValues.length);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertThat(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes()).isTrue();
        assertThat(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes()).isTrue();

        blockBuilder = (BitArrayBlockBuilder) blockBuilder.newBlockBuilderLike(null);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        Block block = createBlockBuilderWithValues(createTestValue(100)).build();
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(Byte.BYTES);
        }

        assertThat(new BitArrayBlockBuilder(null, 22).appendNull().build().getEstimatedDataSizeForStats(0)).isEqualTo(0);
    }

    @Test
    public void testCompactBlock()
    {
        long[] values = {0b101101};
        long[] valueIsValid = {0b111101};

        testCompactBlock(new BitArrayBlock(0, Optional.empty(), new long[0]));
        testCompactBlock(new BitArrayBlock(6, Optional.of(valueIsValid), values));
        testNotCompactBlock(new BitArrayBlock(5, Optional.of(new long[2]), new long[2]));
    }

    private void assertFixedWithValues(Boolean[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Boolean[] expectedValues)
    {
        BitArrayBlockBuilder blockBuilder = new BitArrayBlockBuilder(null, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Boolean[] expectedValues, BitArrayBlockBuilder blockBuilder)
    {
        for (Boolean expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeBoolean(expectedValue);
            }
        }
    }

    private static Boolean[] createTestValue(int positionCount)
    {
        Boolean[] expectedValues = new Boolean[positionCount];
        Random random = new Random(0);
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = random.nextBoolean();
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
        assertThat(((BitArrayBlock) block).getBoolean(position)).isEqualTo(expectedValue);
    }
}
