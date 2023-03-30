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
package io.trino.operator.aggregation.listagg;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class SingleListaggAggregationState
        implements ListaggAggregationState
{
    private static final int INSTANCE_SIZE = instanceSize(SingleListaggAggregationState.class);
    private BlockBuilder blockBuilder;
    private Slice separator;
    private boolean overflowError;
    private Slice overflowFiller;
    private boolean showOverflowEntryCount;

    @Override
    public long getEstimatedSize()
    {
        long estimatedSize = INSTANCE_SIZE;
        if (blockBuilder != null) {
            estimatedSize += blockBuilder.getRetainedSizeInBytes();
        }
        return estimatedSize;
    }

    @Override
    public void setSeparator(Slice separator)
    {
        this.separator = separator;
    }

    @Override
    public Slice getSeparator()
    {
        return separator;
    }

    @Override
    public void setOverflowFiller(Slice overflowFiller)
    {
        this.overflowFiller = overflowFiller;
    }

    @Override
    public Slice getOverflowFiller()
    {
        return overflowFiller;
    }

    @Override
    public void setOverflowError(boolean overflowError)
    {
        this.overflowError = overflowError;
    }

    @Override
    public boolean isOverflowError()
    {
        return overflowError;
    }

    @Override
    public void setShowOverflowEntryCount(boolean showOverflowEntryCount)
    {
        this.showOverflowEntryCount = showOverflowEntryCount;
    }

    @Override
    public boolean showOverflowEntryCount()
    {
        return showOverflowEntryCount;
    }

    @Override
    public void add(Block block, int position)
    {
        if (blockBuilder == null) {
            blockBuilder = VARCHAR.createBlockBuilder(null, 16);
        }
        VARCHAR.appendTo(block, position, blockBuilder);
    }

    @Override
    public void forEach(ListaggAggregationStateConsumer consumer)
    {
        if (blockBuilder == null) {
            return;
        }

        for (int i = 0; i < blockBuilder.getPositionCount(); i++) {
            if (!consumer.accept(blockBuilder, i)) {
                break;
            }
        }
    }

    @Override
    public boolean isEmpty()
    {
        if (blockBuilder == null) {
            return true;
        }
        verify(blockBuilder.getPositionCount() != 0);
        return false;
    }

    @Override
    public int getEntryCount()
    {
        if (blockBuilder == null) {
            return 0;
        }
        return blockBuilder.getPositionCount();
    }

    @Override
    public void reset()
    {
        separator = null;
        overflowError = false;
        overflowFiller = null;
        showOverflowEntryCount = false;
        blockBuilder = null;
    }
}
