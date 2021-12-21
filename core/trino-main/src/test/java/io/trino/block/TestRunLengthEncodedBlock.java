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
import io.trino.spi.block.RunLengthBlockEncoding;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
        Block block = new RunLengthEncodedBlock(createSingleValueBlock(expectedValue), positionCount);
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = expectedValue;
        }
        assertBlock(block, TestRunLengthEncodedBlock::createBlockBuilder, expectedValues);
    }

    private static Block createSingleValueBlock(Slice expectedValue)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, expectedValue.length());
        blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
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
        Block rleBlock = new RunLengthEncodedBlock(valueBlock, 10);
        // Size in bytes is not fixed per position
        assertTrue(rleBlock.fixedSizeInBytesPerPosition().isEmpty());
        // Accepts specific position selection
        boolean[] positions = new boolean[rleBlock.getPositionCount()];
        positions[0] = true;
        positions[1] = true;
        assertEquals(rleBlock.getPositionsSizeInBytes(positions, 2), valueBlock.getSizeInBytes());
        // Accepts null positions array with count only
        assertEquals(rleBlock.getPositionsSizeInBytes(null, 2), valueBlock.getSizeInBytes());
        // Always reports the same size in bytes regardless of positions
        for (int positionCount = 0; positionCount < rleBlock.getPositionCount(); positionCount++) {
            assertEquals(rleBlock.getPositionsSizeInBytes(null, positionCount), valueBlock.getSizeInBytes());
        }
    }

    @Test
    public void testBuildingFromLongArrayBlockBuilder()
    {
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), RunLengthBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromIntArrayBlockBuilder()
    {
        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), RunLengthBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromShortArrayBlockBuilder()
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), RunLengthBlockEncoding.NAME);
    }

    @Test
    public void testBuildingFromByteArrayBlockBuilder()
    {
        ByteArrayBlockBuilder blockBuilder = new ByteArrayBlockBuilder(null, 100);
        populateNullValues(blockBuilder, 100);
        assertEquals(blockBuilder.build().getEncodingName(), RunLengthBlockEncoding.NAME);
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        int positionCount = 10;
        Slice expectedValue = createExpectedValue(5);
        Block block = new RunLengthEncodedBlock(createSingleValueBlock(expectedValue), positionCount);
        for (int postition = 0; postition < positionCount; postition++) {
            assertEquals(block.getEstimatedDataSizeForStats(postition), expectedValue.length());
        }
    }

    private void populateNullValues(BlockBuilder blockBuilder, int positionCount)
    {
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.appendNull();
        }
    }
}
