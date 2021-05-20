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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.operator.aggregation.AbstractGroupCollectionAggregationState;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

public final class GroupListaggAggregationState
        extends AbstractGroupCollectionAggregationState<ListaggAggregationStateConsumer>
        implements ListaggAggregationState
{
    private static final int MAX_BLOCK_SIZE = 1024 * 1024;
    private static final int VALUE_CHANNEL = 0;

    private Slice separator;
    private boolean overflowError;
    private Slice overflowFiller;
    private boolean showOverflowEntryCount;

    public GroupListaggAggregationState(Type valueType)
    {
        super(PageBuilder.withMaxPageSize(MAX_BLOCK_SIZE, ImmutableList.of(valueType)));
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
    public final void add(Block block, int position)
    {
        prepareAdd();
        appendAtChannel(VALUE_CHANNEL, block, position);
    }

    @Override
    protected final boolean accept(ListaggAggregationStateConsumer consumer, PageBuilder pageBuilder, int currentPosition)
    {
        consumer.accept(pageBuilder.getBlockBuilder(VALUE_CHANNEL), currentPosition);
        return true;
    }
}
