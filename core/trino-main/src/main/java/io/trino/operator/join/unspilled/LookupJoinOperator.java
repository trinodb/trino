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
package io.trino.operator.join.unspilled;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.OperatorInfo;
import io.trino.operator.PageBuffer;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.trino.operator.join.JoinStatisticsCounter;
import io.trino.operator.join.LookupJoinOperatorFactory.JoinType;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.unspilled.JoinProbe.JoinProbeFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.operator.WorkProcessor.flatten;
import static java.util.Objects.requireNonNull;

/**
 * Like {@link io.trino.operator.join.LookupJoinOperator} but simplified,
 * without spill support.
 */
public class LookupJoinOperator
        implements AdapterWorkProcessorOperator
{
    private final ListenableFuture<LookupSource> lookupSourceFuture;
    private final boolean waitForBuild;
    private final PageBuffer pageBuffer;
    private final WorkProcessor<Page> pages;
    private final JoinStatisticsCounter statisticsCounter;

    private final Runnable afterClose;
    private final PageJoiner sourcePagesJoiner;
    private final WorkProcessor<Page> joinedSourcePages;

    private boolean closed;

    LookupJoinOperator(
            List<Type> buildOutputTypes,
            JoinType joinType,
            boolean outputSingleMatch,
            boolean waitForBuild,
            PartitionedLookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable afterClose,
            ProcessorContext processorContext,
            Optional<WorkProcessor<Page>> sourcePages)
    {
        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        this.waitForBuild = waitForBuild;
        lookupSourceFuture = lookupSourceFactory.createLookupSource();
        pageBuffer = new PageBuffer();
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        sourcePagesJoiner = new PageJoiner(
                processorContext,
                buildOutputTypes,
                joinType,
                outputSingleMatch,
                joinProbeFactory,
                lookupSourceFuture,
                statisticsCounter);
        joinedSourcePages = sourcePages.orElse(pageBuffer.pages()).transform(sourcePagesJoiner);

        pages = flatten(WorkProcessor.create(this::process));
    }

    @Override
    public Optional<OperatorInfo> getOperatorInfo()
    {
        return Optional.of(statisticsCounter.get());
    }

    @Override
    public boolean needsInput()
    {
        return (!waitForBuild || lookupSourceFuture.isDone()) && pageBuffer.isEmpty() && !pageBuffer.isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
    }

    @Override
    public void finish()
    {
        pageBuffer.finish();
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    public WorkProcessor.ProcessState<WorkProcessor<Page>> process()
    {
        // wait for build side to be completed before fetching any probe data
        // TODO: fix support for probe short-circuit: https://github.com/trinodb/trino/issues/3957
        if (waitForBuild && !lookupSourceFuture.isDone()) {
            return WorkProcessor.ProcessState.blocked(asVoid(lookupSourceFuture));
        }

        if (!joinedSourcePages.isFinished()) {
            return WorkProcessor.ProcessState.ofResult(joinedSourcePages);
        }

        close();
        return WorkProcessor.ProcessState.finished();
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(sourcePagesJoiner);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
