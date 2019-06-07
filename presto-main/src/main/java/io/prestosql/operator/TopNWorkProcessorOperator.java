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
package io.prestosql.operator;

import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNWorkProcessorOperator
        implements WorkProcessorOperator
{
    private final TopNProcessor topNProcessor;
    private final WorkProcessor<Page> pages;

    public TopNWorkProcessorOperator(
            MemoryTrackingContext memoryTrackingContext,
            WorkProcessor<Page> sourcePages,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        this.topNProcessor = new TopNProcessor(
                requireNonNull(memoryTrackingContext, "memoryTrackingContext is null").aggregateUserMemoryContext(),
                types,
                n,
                sortChannels,
                sortOrders);

        if (n == 0) {
            pages = WorkProcessor.of();
        }
        else {
            pages = sourcePages.transform(new TopNPages());
        }
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public void close()
            throws Exception
    {}

    private class TopNPages
            implements WorkProcessor.Transformation<Page, Page>
    {
        @Override
        public TransformationState<Page> process(Page inputPage)
        {
            if (inputPage != null) {
                topNProcessor.addInput(inputPage);
                return TransformationState.needsMoreData();
            }

            // no more input, return results
            Page page = null;
            while (page == null && !topNProcessor.noMoreOutput()) {
                page = topNProcessor.getOutput();
            }

            if (page != null) {
                return TransformationState.ofResult(page, false);
            }

            return TransformationState.finished();
        }
    }
}
