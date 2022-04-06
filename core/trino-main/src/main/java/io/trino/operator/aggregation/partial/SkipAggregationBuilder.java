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
package io.trino.operator.aggregation.partial;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.CompletedWork;
import io.trino.operator.HashCollisionsCounter;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.aggregation.builder.HashAggregationBuilder;
import io.trino.spi.Page;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * {@link HashAggregationBuilder} that does not aggregate input rows at all.
 * It passes the input pages, augmented with initial accumulator state to the output.
 * It can only be used at the partial aggregation step as it relies on rows be aggregated at the final step.
 */
public class SkipAggregationBuilder
        implements HashAggregationBuilder
{
    private final PartialAggregationOutputProcessor partialAggregationOutputProcessor;
    private final LocalMemoryContext memoryContext;
    @Nullable
    private Page currentPage;

    public SkipAggregationBuilder(
            PartialAggregationOutputProcessor partialAggregationOutputProcessor,
            LocalMemoryContext memoryContext)
    {
        this.partialAggregationOutputProcessor = requireNonNull(partialAggregationOutputProcessor, "partialAggregationOutputProcessor is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public Work<?> processPage(Page page)
    {
        checkArgument(currentPage == null);
        currentPage = page;
        return new CompletedWork<>();
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        if (currentPage == null) {
            return WorkProcessor.of();
        }

        Page result = partialAggregationOutputProcessor.processRawInputPage(currentPage);
        currentPage = null;
        return WorkProcessor.of(result);
    }

    @Override
    public boolean isFull()
    {
        return currentPage != null;
    }

    @Override
    public void updateMemory()
    {
        if (currentPage != null) {
            memoryContext.setBytes(currentPage.getSizeInBytes());
        }
    }

    @Override
    public void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter)
    {
        // no op
    }

    @Override
    public void close()
    {
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        throw new UnsupportedOperationException("startMemoryRevoke not supported for SkipAggregationBuilder");
    }

    @Override
    public void finishMemoryRevoke()
    {
        throw new UnsupportedOperationException("finishMemoryRevoke not supported for SkipAggregationBuilder");
    }
}
