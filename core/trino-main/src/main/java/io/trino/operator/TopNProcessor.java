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
package io.trino.operator;

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import jakarta.annotation.Nullable;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNProcessor
{
    private final LocalMemoryContext localUserMemoryContext;

    @Nullable
    private GroupedTopNBuilder topNBuilder;
    private Iterator<Page> outputIterator;

    public TopNProcessor(
            AggregatedMemoryContext aggregatedMemoryContext,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders, TypeOperators typeOperators)
    {
        requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null");
        this.localUserMemoryContext = aggregatedMemoryContext.newLocalMemoryContext(TopNProcessor.class.getSimpleName());
        checkArgument(n >= 0, "n must be positive");

        if (n == 0) {
            outputIterator = emptyIterator();
        }
        else {
            topNBuilder = new GroupedTopNRowNumberBuilder(
                    types,
                    new SimplePageWithPositionComparator(types, sortChannels, sortOrders, typeOperators),
                    n,
                    false,
                    new int[0],
                    new NoChannelGroupByHash());
        }
    }

    public void addInput(Page page)
    {
        requireNonNull(topNBuilder, "topNBuilder is null");
        boolean done = topNBuilder.processPage(requireNonNull(page, "page is null")).process();
        // there is no grouping so work will always be done
        verify(done);
        updateMemoryReservation();
    }

    public Page getOutput()
    {
        if (outputIterator == null) {
            // start flushing
            outputIterator = topNBuilder.buildResult();
        }

        Page output = null;
        if (outputIterator.hasNext()) {
            output = outputIterator.next();
        }
        else {
            outputIterator = emptyIterator();
        }
        updateMemoryReservation();
        return output;
    }

    public boolean noMoreOutput()
    {
        return outputIterator != null && !outputIterator.hasNext();
    }

    private void updateMemoryReservation()
    {
        requireNonNull(topNBuilder, "topNBuilder is null");
        localUserMemoryContext.setBytes(topNBuilder.getEstimatedSizeInBytes());
    }
}
