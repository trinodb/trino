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
package io.trino.operator.join;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.HashGenerator;
import io.trino.operator.OperatorInfo;
import io.trino.operator.ProcessorContext;
import io.trino.operator.SpillMetrics;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.join.JoinProbe.JoinProbeFactory;
import io.trino.operator.join.LookupJoinOperatorFactory.JoinType;
import io.trino.operator.join.PageJoiner.PageJoinerFactory;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.spiller.PartitioningSpillerFactory;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.operator.WorkProcessor.flatten;

public class LookupJoinOperator
        implements WorkProcessorOperator
{
    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private final WorkProcessor<Page> pages;
    private final SpillingJoinProcessor joinProcessor;
    private final JoinStatisticsCounter statisticsCounter;
    private final SpillMetrics spillMetrics = new SpillMetrics("Probe");

    LookupJoinOperator(
            List<Type> probeTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            boolean outputSingleMatch,
            boolean waitForBuild,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable afterClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator,
            PartitioningSpillerFactory partitioningSpillerFactory,
            ProcessorContext processorContext,
            WorkProcessor<Page> sourcePages)
    {
        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();
        PageJoinerFactory pageJoinerFactory = (lookupSourceProvider, joinerPartitioningSpillerFactory, savedRows) ->
                new DefaultPageJoiner(
                        processorContext,
                        probeTypes,
                        buildOutputTypes,
                        joinType,
                        outputSingleMatch,
                        spillMetrics,
                        hashGenerator,
                        joinProbeFactory,
                        lookupSourceFactory,
                        lookupSourceProvider,
                        joinerPartitioningSpillerFactory,
                        statisticsCounter,
                        savedRows);
        joinProcessor = new SpillingJoinProcessor(
                afterClose,
                lookupJoinsCount,
                waitForBuild,
                lookupSourceFactory,
                lookupSourceProviderFuture,
                partitioningSpillerFactory,
                spillMetrics,
                pageJoinerFactory,
                sourcePages);
        WorkProcessor<Page> pages = flatten(WorkProcessor.create(joinProcessor));
        if (waitForBuild) {
            // wait for build side before fetching any probe pages
            pages = pages.blocking(() -> transform(lookupSourceProviderFuture, ignored -> null, directExecutor()));
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
    public Metrics getMetrics()
    {
        return spillMetrics.getMetrics();
    }

    @Override
    public void close()
    {
        joinProcessor.close();
    }
}
