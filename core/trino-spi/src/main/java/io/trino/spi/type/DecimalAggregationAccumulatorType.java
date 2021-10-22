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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DecimalAggregationAccumulatorBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorSession;

import static java.util.Collections.singletonList;

public class DecimalAggregationAccumulatorType
        extends AbstractType
        implements FixedWidthType
{
    public static final String NAME = "DecimalAggregationAccumulator";
    public static final Type LONG_DECIMAL_WITH_OVERFLOW = createDecimalAggregationAccumulatorType(2);
    public static final Type LONG_DECIMAL_WITH_OVERFLOW_AND_LONG = createDecimalAggregationAccumulatorType(3);

    private final int numberOfNoOverflowValues;

    private DecimalAggregationAccumulatorType(int numberOfNoOverflowValues)
    {
        super(
                new TypeSignature(
                        NAME,
                        singletonList(TypeSignatureParameter.numericParameter((long) numberOfNoOverflowValues))),
                Slice.class);

        if (numberOfNoOverflowValues < 0) {
            throw new IllegalArgumentException("Invalid DecimalAggregationAccumulatorType numberOfNoOverflowValues " + numberOfNoOverflowValues);
        }
        this.numberOfNoOverflowValues = numberOfNoOverflowValues;
    }

    private static Type createDecimalAggregationAccumulatorType(int numberOfNoOverflowValues)
    {
        return new DecimalAggregationAccumulatorType(numberOfNoOverflowValues);
    }

    @Override
    public int getFixedSize()
    {
        return numberOfNoOverflowValues * Long.BYTES + Long.BYTES;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new DecimalAggregationAccumulatorBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / getFixedSize()), numberOfNoOverflowValues);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlVarbinary(getSlice(block, position).getBytes());
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new DecimalAggregationAccumulatorBlockBuilder(null, positionCount, numberOfNoOverflowValues);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writePositionTo(position, blockBuilder);
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        if (length != getFixedSize()) {
            throw new IllegalStateException("Expected entry size to be exactly " + getFixedSize() + " but was " + length);
        }
        for (int i = 0; i <= numberOfNoOverflowValues; i++) {
            blockBuilder.writeLong(value.getLong(offset + i * Long.BYTES));
        }
        blockBuilder.closeEntry();
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getSliceLength(position));
    }
}
