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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DecimalAggregationAccumulatorBlock;
import io.trino.spi.block.DecimalAggregationAccumulatorBlockBuilder;
import org.testng.annotations.Test;

import static io.trino.spi.type.DecimalAggregationAccumulatorType.LONG_DECIMAL_WITH_OVERFLOW_AND_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDecimalAggregationAccumulatorType
{
    private static final Type TYPE = LONG_DECIMAL_WITH_OVERFLOW_AND_LONG;
    private static final int OVERFLOW_OFFSET = 3 * Long.BYTES;

    @Test
    public void testWriteRead()
    {
        BlockBuilder blockBuilder = TYPE.createBlockBuilder(null, 2);
        Slice entry = Slices.wrappedLongArray(1, 2, 3, 4);
        TYPE.writeSlice(blockBuilder, entry);
        blockBuilder.appendNull();
        Block block = blockBuilder.build();

        testWriteRead(entry, block);
        testWriteRead(entry, blockBuilder);
    }

    private void testWriteRead(Slice entry, Block block)
    {
        assertEquals(block.getPositionCount(), 2);
        assertEquals(block.getLong(0, 0), 1);
        assertEquals(block.getLong(0, Long.BYTES), 2);
        assertEquals(block.getLong(0, Long.BYTES * 2), 3);
        assertEquals(block.getLong(0, Long.BYTES * 3), 4);
        assertEquals(TYPE.getSlice(block, 0), entry);
        assertEquals(TYPE.getObjectValue(null, block, 0), new SqlVarbinary(entry.getBytes()));

        assertTrue(block.isNull(1));
        assertNull(TYPE.getObjectValue(null, block, 1));

        BlockBuilder newBlockBuilder = TYPE.createBlockBuilder(null, 2);
        TYPE.appendTo(block, 0, newBlockBuilder);
        TYPE.appendTo(block, 1, newBlockBuilder);
        Block newBlock = newBlockBuilder.build();
        assertEquals(TYPE.getSlice(block, 0), TYPE.getSlice(newBlock, 0));
        assertNull(TYPE.getObjectValue(null, newBlock, 1));
        assertTrue(newBlock.isNull(1));
    }

    @Test
    public void testRegion()
    {
        int entries = 4;
        BlockBuilder blockBuilder = TYPE.createBlockBuilder(null, entries);
        TYPE.writeSlice(blockBuilder, entry(1));
        blockBuilder.appendNull();
        TYPE.writeSlice(blockBuilder, entry(2));
        TYPE.writeSlice(blockBuilder, entry(3));
        Block block = blockBuilder.build();

        testRegion(block);
        testRegion(blockBuilder);
    }

    private void testRegion(Block block)
    {
        DecimalAggregationAccumulatorBlock region = (DecimalAggregationAccumulatorBlock) block.getRegion(1, 2);
        assertTrue(region.isNull(0));
        assertEquals(TYPE.getSlice(region, 1), entry(2));
        assertEquals(region.getValuesSlice(), Slices.wrappedLongArray(0, 0, 0, 2, 2, 2));
        assertEquals(region.getOverflowSlice(), Slices.wrappedLongArray(0, 2));

        DecimalAggregationAccumulatorBlock copiedRegion = (DecimalAggregationAccumulatorBlock) block.copyRegion(1, 2);
        assertTrue(copiedRegion.isNull(0));
        assertEquals(TYPE.getSlice(copiedRegion, 1), entry(2));
        assertEquals(copiedRegion.getValuesSlice(), Slices.wrappedLongArray(0, 0, 0, 2, 2, 2));
        assertEquals(copiedRegion.getOverflowSlice(), Slices.wrappedLongArray(0, 2));

        DecimalAggregationAccumulatorBlock copiedPositions = (DecimalAggregationAccumulatorBlock) block.copyPositions(new int[] {1, 2}, 0, 2);
        assertTrue(copiedPositions.isNull(0));
        assertEquals(TYPE.getSlice(copiedPositions, 1), entry(2));
        assertEquals(copiedPositions.getValuesSlice(), Slices.wrappedLongArray(0, 0, 0, 2, 2, 2));
        assertEquals(copiedPositions.getOverflowSlice(), Slices.wrappedLongArray(0, 2));
    }

    @Test
    public void testSingleValueBlock()
    {
        BlockBuilder blockBuilder = TYPE.createBlockBuilder(null, 2);
        TYPE.writeSlice(blockBuilder, entry(1));
        blockBuilder.appendNull();
        TYPE.writeSlice(blockBuilder, noOverflowEntry(1));
        Block block = blockBuilder.build();

        testSingleValueBlock(block);
        testSingleValueBlock(blockBuilder);
    }

    private void testSingleValueBlock(Block block)
    {
        Block notNullBlock = block.getSingleValueBlock(0);
        assertEquals(notNullBlock.getPositionCount(), 1);
        assertEquals(TYPE.getSlice(notNullBlock, 0), entry(1));

        Block nullBlock = block.getSingleValueBlock(1);
        assertEquals(nullBlock.getPositionCount(), 1);
        assertTrue(nullBlock.isNull(0));

        Block noOverflowBlock = block.getSingleValueBlock(2);
        assertEquals(noOverflowBlock.getPositionCount(), 1);
        assertEquals(TYPE.getSlice(noOverflowBlock, 0), noOverflowEntry(1));
        assertEquals(noOverflowBlock.getLong(0, OVERFLOW_OFFSET), 0);
    }

    @Test
    public void testOverflowSliceNull()
    {
        DecimalAggregationAccumulatorBlockBuilder blockBuilder = (DecimalAggregationAccumulatorBlockBuilder) TYPE.createBlockBuilder(null, 1);
        TYPE.writeSlice(blockBuilder, noOverflowEntry(1));
        DecimalAggregationAccumulatorBlock block = (DecimalAggregationAccumulatorBlock) blockBuilder.build();

        assertNull(block.getOverflowSlice());
        assertNull(blockBuilder.getOverflowSlice());
    }

    private Slice entry(int value)
    {
        return Slices.wrappedLongArray(value, value, value, value);
    }

    private Slice noOverflowEntry(int value)
    {
        return Slices.wrappedLongArray(value, value, value, 0);
    }
}
