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
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.predicate.Utils;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
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

        IntArrayList positions = IntArrayList.wrap(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
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

        IntArrayList positions = IntArrayList.wrap(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
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

    @Test
    public void testFlushUsefulDictionariesOnRelease()
    {
        int maxPageBytes = 100;
        int maxDirectSize = 1000;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                maxDirectSize,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        Block valueBlock = Utils.nativeValueToBlock(VARCHAR, Slices.utf8Slice("test"));
        Block dictionaryBlock = DictionaryBlock.create(10, valueBlock, new int[10]);
        Page inputPage = new Page(dictionaryBlock);

        pageBuilder.appendToOutputPartition(inputPage, IntArrayList.wrap(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
        // Dictionary mode appender should report the size of the ID's, but doesn't currently track
        // the per-position size at all because it would be inefficient
        assertEquals(Integer.BYTES * 10, pageBuilder.getSizeInBytes());
        assertFalse(pageBuilder.isFull());

        Optional<Page> flushedPage = pageBuilder.flushOrFlattenBeforeRelease();
        assertTrue(flushedPage.isPresent(), "pageBuilder should force flush the dictionary");
        assertTrue(flushedPage.get().getBlock(0) instanceof DictionaryBlock, "result should be dictionary encoded");
    }

    @Test
    public void testFlattenUnhelpfulDictionariesOnRelease()
    {
        // Create unhelpful dictionary wrapping
        Block valueBlock = createRandomBlockForType(VARCHAR, 10, 0.25f);
        Block dictionaryBlock = DictionaryBlock.create(10, valueBlock, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Page inputPage = new Page(dictionaryBlock);

        // Ensure the builder allows the entire value block to be inserted without being full
        int maxPageBytes = toIntExact(valueBlock.getSizeInBytes() * 10);
        int maxDirectSize = maxPageBytes * 10;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                maxDirectSize,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        pageBuilder.appendToOutputPartition(inputPage, IntArrayList.wrap(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
        assertEquals(Integer.BYTES * 10, pageBuilder.getSizeInBytes());
        assertFalse(pageBuilder.isFull());

        assertEquals(Optional.empty(), pageBuilder.flushOrFlattenBeforeRelease(), "pageBuilder should not force a flush");
        assertFalse(pageBuilder.isFull());
        assertEquals(valueBlock.getSizeInBytes(), pageBuilder.getSizeInBytes(), "pageBuilder should have transitioned to direct mode");

        Page result = pageBuilder.build();
        assertTrue(result.getBlock(0) instanceof ValueBlock, "result should not be a dictionary block");
    }
}
