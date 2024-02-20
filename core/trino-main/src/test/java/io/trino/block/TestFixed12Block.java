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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.Fixed12BlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.block.Fixed12Block.FIXED12_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFixed12Block
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
    {
        Slice[] expectedValues = alternatingNullValues(createTestValue(17));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Slice[] expectedValues = createTestValue(100);
        Fixed12BlockBuilder emptyBlockBuilder = new Fixed12BlockBuilder(null, 0);

        Fixed12BlockBuilder blockBuilder = new Fixed12BlockBuilder(null, expectedValues.length);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertThat(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes()).isTrue();
        assertThat(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes()).isTrue();

        blockBuilder = (Fixed12BlockBuilder) blockBuilder.newBlockBuilderLike(null);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        Slice[] expectedValues = createTestValue(100);
        assertEstimatedDataSizeForStats(createBlockBuilderWithValues(expectedValues), expectedValues);
    }

    @Test
    public void testCompactBlock()
    {
        int[] intArray = {
                0, 0, 0,
                0, 0, 0,
                0, 0, 1,
                0, 0, 2,
                0, 0, 3,
                0, 0, 4};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new Fixed12Block(0, Optional.empty(), new int[0]));
        testCompactBlock(new Fixed12Block(valueIsNull.length, Optional.of(valueIsNull), intArray));
        testNotCompactBlock(new Fixed12Block(valueIsNull.length - 2, Optional.of(valueIsNull), intArray));
    }

    private void assertFixedWithValues(Slice[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        Fixed12BlockBuilder blockBuilder = new Fixed12BlockBuilder(null, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Slice[] expectedValues, Fixed12BlockBuilder blockBuilder)
    {
        for (Slice expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeFixed12(
                        expectedValue.getLong(0),
                        expectedValue.getInt(8));
            }
        }
    }

    private static Slice[] createTestValue(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(FIXED12_BYTES);
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

        Fixed12Block fixed12Block = (Fixed12Block) block;
        Slice expectedBytes = (Slice) expectedValue;
        assertThat(fixed12Block.getFixed12First(position)).isEqualTo(expectedBytes.getLong(0));
        assertThat(fixed12Block.getFixed12Second(position)).isEqualTo(expectedBytes.getInt(8));
    }
}
