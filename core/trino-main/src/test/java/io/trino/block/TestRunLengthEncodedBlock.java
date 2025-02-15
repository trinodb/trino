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
import io.trino.spi.block.ByteArrayBlockBuilder;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRunLengthEncodedBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        for (int positionCount = 0; positionCount < 10; positionCount++) {
            assertRleBlock(positionCount);
        }
    }

    private void assertRleBlock(int positionCount)
    {
        Slice expectedValue = createExpectedValue(0);
        Block block = RunLengthEncodedBlock.create(createSingleValueBlock(expectedValue), positionCount);
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = expectedValue;
        }
        assertBlock(block, expectedValues);
    }

    private static Block createSingleValueBlock(Slice expectedValue)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, expectedValue.length());
        blockBuilder.writeEntry(expectedValue);
        return blockBuilder.build();
    }

    private static BlockBuilder createBlockBuilder()
    {
        return new VariableWidthBlockBuilder(null, 1, 1);
    }

    @Test
    public void testPositionsSizeInBytes()
    {
        Block valueBlock = createSingleValueBlock(createExpectedValue(10));
        Block rleBlock = RunLengthEncodedBlock.create(valueBlock, 10);
        // Size in bytes is not fixed per position
        assertThat(rleBlock.fixedSizeInBytesPerPosition()).isEmpty();
        // Accepts specific position selection
        boolean[] positions = new boolean[rleBlock.getPositionCount()];
        positions[0] = true;
        positions[1] = true;
        assertThat(rleBlock.getPositionsSizeInBytes(positions, 2)).isEqualTo(valueBlock.getSizeInBytes());
        // Accepts null positions array with count only
        assertThat(rleBlock.getPositionsSizeInBytes(null, 2)).isEqualTo(valueBlock.getSizeInBytes());
        // Always reports the same size in bytes regardless of positions
        for (int positionCount = 0; positionCount < rleBlock.getPositionCount(); positionCount++) {
            assertThat(rleBlock.getPositionsSizeInBytes(null, positionCount)).isEqualTo(valueBlock.getSizeInBytes());
        }
    }

    @Test
    public void testBuildingFromLongArrayBlockBuilder()
    {
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
    }

    @Test
    public void testBuildingFromIntArrayBlockBuilder()
    {
        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
    }

    @Test
    public void testBuildingFromShortArrayBlockBuilder()
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
    }

    @Test
    public void testBuildingFromByteArrayBlockBuilder()
    {
        ByteArrayBlockBuilder blockBuilder = new ByteArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        int positionCount = 10;
        Slice expectedValue = createExpectedValue(5);
        Block block = RunLengthEncodedBlock.create(createSingleValueBlock(expectedValue), positionCount);
        for (int postition = 0; postition < positionCount; postition++) {
            assertThat(block.getEstimatedDataSizeForStats(postition)).isEqualTo(expectedValue.length());
        }
    }

    private void populateNullValues(BlockBuilder blockBuilder, int positionCount)
    {
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertThat(block.isNull(position)).isTrue();
            return;
        }

        assertThat(block.isNull(position)).isFalse();
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        assertThat(variableWidthBlock.getSlice(block.getUnderlyingValuePosition(position))).isEqualTo(expectedValue);
    }
}
