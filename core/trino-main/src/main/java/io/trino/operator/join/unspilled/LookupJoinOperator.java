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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.OperatorInfo;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.join.JoinStatisticsCounter;
import io.trino.operator.join.LookupJoinOperatorFactory.JoinType;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.unspilled.JoinProbe.JoinProbeFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

/**
 * Like {@link io.trino.operator.join.LookupJoinOperator} but simplified,
 * without spill support.
 */
public class LookupJoinOperator
        implements WorkProcessorOperator
{
    private final ListenableFuture<LookupSource> lookupSourceFuture;
    private final WorkProcessor<Page> pages;
    private final JoinStatisticsCounter statisticsCounter;
    private final Runnable afterClose;
    private final PageJoiner sourcePagesJoiner;

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
            WorkProcessor<Page> sourcePages)
    {
        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        lookupSourceFuture = lookupSourceFactory.createLookupSource();
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        sourcePagesJoiner = new PageJoiner(
                processorContext,
                buildOutputTypes,
                joinType,
                outputSingleMatch,
                joinProbeFactory,
                lookupSourceFuture,
                statisticsCounter);
        WorkProcessor<Page> pages = sourcePages.transform(sourcePagesJoiner);
        if (waitForBuild) {
            // wait for build side before fetching any probe pages
            pages = pages.blocking(() -> transform(lookupSourceFuture, ignored -> null, directExecutor()));
        }
        this.pages = pages;
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public Optional<OperatorInfo> getOperatorInfo()
    {
        return Optional.of(statisticsCounter.get());
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
