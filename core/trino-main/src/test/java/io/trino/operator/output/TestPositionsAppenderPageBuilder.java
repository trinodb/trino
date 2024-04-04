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

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPositionsAppenderPageBuilder
{
    @Test
    public void testFullOnPositionCountLimit()
    {
        int maxPageBytes = 1024 * 1024;
        int maxDirectSize = maxPageBytes * 10;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                maxDirectSize,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        Block rleBlock = RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("test"), 10);
        Page inputPage = new Page(rleBlock);

        IntArrayList positions = IntArrayList.wrap(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        // Append 32760 positions, just less than MAX_POSITION_COUNT
        assertEquals(32768, PositionsAppenderPageBuilder.MAX_POSITION_COUNT, "expected MAX_POSITION_COUNT to be 32768");
        for (int i = 0; i < 3276; i++) {
            pageBuilder.appendToOutputPartition(inputPage, positions);
        }
        assertFalse(pageBuilder.isFull(), "pageBuilder should still not be full");
        // Append 10 more positions, crossing the threshold on position count
        pageBuilder.appendToOutputPartition(inputPage, positions);
        assertTrue(pageBuilder.isFull(), "pageBuilder should be full");
        PositionsAppenderSizeAccumulator sizeAccumulator = pageBuilder.computeAppenderSizes();
        assertEquals(rleBlock.getSizeInBytes(), sizeAccumulator.getSizeInBytes());
        assertTrue(sizeAccumulator.getDirectSizeInBytes() < maxDirectSize, "direct size should still be below threshold");
        assertEquals(sizeAccumulator.getSizeInBytes(), pageBuilder.getSizeInBytes(), "pageBuilder sizeInBytes must match sizeAccumulator value");
    }

    @Test
    public void testFullOnDirectSizeInBytes()
    {
        int maxPageBytes = 100;
        int maxDirectSize = 1000;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                maxDirectSize,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        PositionsAppenderSizeAccumulator sizeAccumulator = pageBuilder.computeAppenderSizes();
        assertEquals(0L, sizeAccumulator.getSizeInBytes());
        assertEquals(0L, sizeAccumulator.getDirectSizeInBytes());
        assertFalse(pageBuilder.isFull());

        Block rleBlock = RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("test"), 10);
        Page inputPage = new Page(rleBlock);

        IntArrayList positions = IntArrayList.wrap(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        pageBuilder.appendToOutputPartition(inputPage, positions);
        // 10 positions inserted, size in bytes is still the same since we're in RLE mode but direct size is 10x
        sizeAccumulator = pageBuilder.computeAppenderSizes();
        assertEquals(rleBlock.getSizeInBytes(), sizeAccumulator.getSizeInBytes());
        assertEquals(sizeAccumulator.getSizeInBytes(), pageBuilder.getSizeInBytes(), "pageBuilder sizeInBytes must match sizeAccumulator value");
        assertEquals(rleBlock.getSizeInBytes() * 10, sizeAccumulator.getDirectSizeInBytes());
        assertFalse(pageBuilder.isFull());

        // Keep inserting until the direct size limit is reached
        while (pageBuilder.computeAppenderSizes().getDirectSizeInBytes() < maxDirectSize) {
            pageBuilder.appendToOutputPartition(inputPage, positions);
        }
        // size in bytes is unchanged
        sizeAccumulator = pageBuilder.computeAppenderSizes();
        assertEquals(rleBlock.getSizeInBytes(), sizeAccumulator.getSizeInBytes(), "sizeInBytes must still report the RLE block size only");
        assertEquals(sizeAccumulator.getSizeInBytes(), pageBuilder.getSizeInBytes(), "pageBuilder sizeInBytes must match sizeAccumulator value");
        // builder reports full due to maximum size in bytes reached
        assertTrue(pageBuilder.isFull());
        Page result = pageBuilder.build();
        assertEquals(120, result.getPositionCount(), "result positions should be below the 8192 maximum");
        assertTrue(result.getBlock(0) instanceof RunLengthEncodedBlock, "result block is RLE encoded");
    }
}
