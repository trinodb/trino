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

import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * {@link PositionsAppender} that will produce {@link RunLengthEncodedBlock} output if possible,
 * that is all inputs are {@link RunLengthEncodedBlock} blocks with the same value.
 */
public class RleAwarePositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(RleAwarePositionsAppender.class);
    private static final int NO_RLE = -1;

    private final BlockPositionIsDistinctFrom isDistinctFromOperator;
    private final PositionsAppender delegate;

    @Nullable
    private Block rleValue;

    // NO_RLE means flat state, 0 means initial empty state, positive means RLE state and the current RLE position count.
    private int rlePositionCount;

    public RleAwarePositionsAppender(BlockPositionIsDistinctFrom isDistinctFromOperator, PositionsAppender delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.isDistinctFromOperator = requireNonNull(isDistinctFromOperator, "isDistinctFromOperator is null");
    }

    @Override
    public void append(IntArrayList positions, Block source)
    {
        // RleAwarePositionsAppender should be used with FlatteningPositionsAppender that makes sure
        // append is called only with flat block
        checkArgument(!(source instanceof RunLengthEncodedBlock));
        switchToFlat();
        delegate.append(positions, source);
    }

    @Override
    public void appendRle(Block value, int positionCount)
    {
        if (positionCount == 0) {
            return;
        }
        checkArgument(value.getPositionCount() == 1, "Expected value to contain a single position but has %d positions".formatted(value.getPositionCount()));

        if (rlePositionCount == 0) {
            // initial empty state, switch to RLE state
            rleValue = value;
            rlePositionCount = positionCount;
        }
        else if (rleValue != null) {
            // we are in the RLE state
            if (!isDistinctFromOperator.isDistinctFrom(rleValue, 0, value, 0)) {
                // the values match. we can just add positions.
                this.rlePositionCount += positionCount;
                return;
            }
            // RLE values do not match. switch to flat state
            switchToFlat();
            delegate.appendRle(value, positionCount);
        }
        else {
            // flat state
            delegate.appendRle(value, positionCount);
        }
    }

    @Override
    public void append(int position, Block value)
    {
        switchToFlat();
        delegate.append(position, value);
    }

    @Override
    public Block build()
    {
        Block result;
        if (rleValue != null) {
            result = RunLengthEncodedBlock.create(rleValue, rlePositionCount);
        }
        else {
            result = delegate.build();
        }

        reset();
        return result;
    }

    private void reset()
    {
        rleValue = null;
        rlePositionCount = 0;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedRleSize = rleValue != null ? rleValue.getRetainedSizeInBytes() : 0;
        return INSTANCE_SIZE + retainedRleSize + delegate.getRetainedSizeInBytes();
    }

    @Override
    public long getSizeInBytes()
    {
        long rleSize = rleValue != null ? rleValue.getSizeInBytes() : 0;
        return rleSize + delegate.getSizeInBytes();
    }

    private void switchToFlat()
    {
        if (rleValue != null) {
            // we are in the RLE state, flatten all RLE blocks
            delegate.appendRle(rleValue, rlePositionCount);
            rleValue = null;
        }
        rlePositionCount = NO_RLE;
    }
}
