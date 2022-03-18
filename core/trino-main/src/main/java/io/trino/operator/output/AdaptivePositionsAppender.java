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
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class AdaptivePositionsAppender
        implements BlockTypeAwarePositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AdaptivePositionsAppender.class).instanceSize();
    private static final int EMPTY_STATE_MARKER = 0;
    private static final int FLAT_STATE_MARKER = -1;

    private final int expectedPositions;
    private final BlockPositionEqual equalOperator;
    private final BlockTypeAwarePositionsAppender flat;

    @Nullable
    private Block rleValue;

    // FLAT_STATE_MARKER means flat state, EMPTY_STATE_MARKER means initial empty state, positive means rle state and the current rle position count.
    private int rlePositionCount = EMPTY_STATE_MARKER;

    public AdaptivePositionsAppender(BlockPositionEqual equalOperator, BlockTypeAwarePositionsAppender flat, int expectedPositions)
    {
        this.flat = requireNonNull(flat, "appender is null");
        this.expectedPositions = expectedPositions;
        this.equalOperator = equalOperator;
    }

    @Override
    public void append(IntArrayList positions, Block source)
    {
        switchToFlat();
        flat.append(positions, source);
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        switchToFlat();
        flat.appendDictionary(positions, source);
    }

    @Override
    public void appendRle(RunLengthEncodedBlock source)
    {
        if (isEmpty()) {
            // initial empty state, switch to rle state
            rleValue = source.getValue();
            rlePositionCount = source.getPositionCount();
        }
        else if (rleValue != null) {
            // we are in the rle state
            if (equalOperator.equalNullSafe(rleValue, 0, source.getValue(), 0)) {
                // the values match. we can just add positions.
                this.rlePositionCount += source.getPositionCount();
                return;
            }
            // RLE values do not match. switch to flat state
            switchToFlat();
            flat.appendRle(source);
        }
        else {
            // flat state
            flat.appendRle(source);
        }
    }

    @Override
    public void appendRow(Block source, int position)
    {
        switchToFlat();
        flat.appendRow(source, position);
    }

    @Override
    public Block build()
    {
        if (rleValue != null) {
            return new RunLengthEncodedBlock(rleValue, rlePositionCount);
        }

        return flat.build();
    }

    @Override
    public BlockTypeAwarePositionsAppender newStateLike(@Nullable BlockBuilderStatus blockBuilderStatus)
    {
        return new AdaptivePositionsAppender(equalOperator, (BlockTypeAwarePositionsAppender) this.flat.newStateLike(blockBuilderStatus), expectedPositions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedRleSize = rleValue != null ? rleValue.getRetainedSizeInBytes() : 0;
        long currentRetainedSize = flat != null ? flat.getRetainedSizeInBytes() : 0;
        return INSTANCE_SIZE + retainedRleSize + currentRetainedSize;
    }

    private void switchToFlat()
    {
        if (rleValue != null) {
            // we are in the rle state, flatten all RLE blocks
            flat.appendRle(new RunLengthEncodedBlock(rleValue, rlePositionCount));
            rleValue = null;
        }
        rlePositionCount = FLAT_STATE_MARKER;
    }

    private boolean isEmpty()
    {
        return rlePositionCount == EMPTY_STATE_MARKER;
    }
}
