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
import io.trino.operator.PageBuffer;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.trino.operator.join.JoinProbe.JoinProbeFactory;
import io.trino.operator.join.LookupJoinOperatorFactory.JoinType;
import io.trino.operator.join.PageJoiner.PageJoinerFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spiller.PartitioningSpillerFactory;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.operator.WorkProcessor.flatten;

public class LookupJoinOperator
        implements AdapterWorkProcessorOperator
{
    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private final boolean waitForBuild;
    private final PageBuffer pageBuffer;
    private final WorkProcessor<Page> pages;
    private final SpillingJoinProcessor joinProcessor;
    private final JoinStatisticsCounter statisticsCounter;

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
            Optional<WorkProcessor<Page>> sourcePages)
    {
        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        this.waitForBuild = waitForBuild;
        lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();
        pageBuffer = new PageBuffer();
        PageJoinerFactory pageJoinerFactory = (lookupSourceProvider, joinerPartitioningSpillerFactory, savedRows) ->
                new DefaultPageJoiner(
                        processorContext,
                        probeTypes,
                        buildOutputTypes,
                        joinType,
                        outputSingleMatch,
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
                pageJoinerFactory,
                sourcePages.orElse(pageBuffer.pages()));
        pages = flatten(WorkProcessor.create(joinProcessor));
    }

    @Override
    public Optional<OperatorInfo> getOperatorInfo()
    {
        return Optional.of(statisticsCounter.get());
    }

    @Override
    public boolean needsInput()
    {
        return (!waitForBuild || lookupSourceProviderFuture.isDone()) && pageBuffer.isEmpty() && !pageBuffer.isFinished();
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

    @Override
    public void close()
    {
        joinProcessor.close();
    }
}
