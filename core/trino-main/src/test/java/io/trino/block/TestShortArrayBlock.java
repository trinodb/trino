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
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.ShortArrayBlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestShortArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Short[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
    {
        Short[] expectedValues = alternatingNullValues(createTestValue(17));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Short[] expectedValues = createTestValue(100);
        ShortArrayBlockBuilder emptyBlockBuilder = new ShortArrayBlockBuilder(null, 0);

        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, expectedValues.length);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertThat(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes()).isTrue();
        assertThat(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes()).isTrue();

        blockBuilder = (ShortArrayBlockBuilder) blockBuilder.newBlockBuilderLike(null);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(createTestValue(100));
        Block block = blockBuilder.build();
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(Short.BYTES);
        }

        assertThat(new ShortArrayBlockBuilder(null, 22).appendNull().build().getEstimatedDataSizeForStats(0)).isEqualTo(0);
    }

    @Test
    public void testCompactBlock()
    {
        short[] shortArray = {(short) 0, (short) 0, (short) 1, (short) 2, (short) 3, (short) 4};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new ShortArrayBlock(0, Optional.empty(), new short[0]));
        testCompactBlock(new ShortArrayBlock(shortArray.length, Optional.of(valueIsNull), shortArray));
        testNotCompactBlock(new ShortArrayBlock(shortArray.length - 1, Optional.of(valueIsNull), shortArray));
    }

    private void assertFixedWithValues(Short[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Short[] expectedValues)
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Short[] expectedValues, ShortArrayBlockBuilder blockBuilder)
    {
        for (Short expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeShort(expectedValue);
            }
        }
    }

    private static Short[] createTestValue(int positionCount)
    {
        Short[] expectedValues = new Short[positionCount];
        Random random = new Random(0);
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = (short) random.nextInt();
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
        assertThat(((ShortArrayBlock) block).getShort(position)).isEqualTo(((Short) expectedValue).shortValue());
    }
}
