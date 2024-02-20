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
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLongArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Long[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
    {
        Long[] expectedValues = alternatingNullValues(createTestValue(17));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Long[] expectedValues = createTestValue(100);
        BlockBuilder emptyBlockBuilder = new LongArrayBlockBuilder(null, 0);

        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, expectedValues.length);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertThat(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes()).isTrue();
        assertThat(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes()).isTrue();

        blockBuilder = (LongArrayBlockBuilder) blockBuilder.newBlockBuilderLike(null);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(createTestValue(100));
        Block block = blockBuilder.build();
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(Long.BYTES);
        }

        assertThat(new LongArrayBlockBuilder(null, 22).appendNull().build().getEstimatedDataSizeForStats(0)).isEqualTo(0);
    }

    @Test
    public void testCompactBlock()
    {
        long[] longArray = {0L, 0L, 1L, 2L, 3L, 4L};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new LongArrayBlock(0, Optional.empty(), new long[0]));
        testCompactBlock(new LongArrayBlock(longArray.length, Optional.of(valueIsNull), longArray));
        testNotCompactBlock(new LongArrayBlock(longArray.length - 1, Optional.of(valueIsNull), longArray));
    }

    private void assertFixedWithValues(Long[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Long[] expectedValues)
    {
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Long[] expectedValues, LongArrayBlockBuilder blockBuilder)
    {
        for (Long expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeLong(expectedValue.longValue());
            }
        }
    }

    private static Long[] createTestValue(int positionCount)
    {
        Long[] expectedValues = new Long[positionCount];
        Random random = new Random(0);
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = random.nextLong();
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
        assertThat(((LongArrayBlock) block).getLong(position)).isEqualTo(((Long) expectedValue).longValue());
    }
}
