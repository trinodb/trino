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
package io.trino.operator.window;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.SourceOperator;
import io.trino.operator.SourceOperatorFactory;
import io.trino.operator.TableScanWorkProcessorOperator;
import io.trino.operator.WindowOperator;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorSourceOperator;
import io.trino.operator.WorkProcessorSourceOperatorAdapter;
import io.trino.operator.WorkProcessorSourceOperatorAdapter.AdapterWorkProcessorSourceOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.split.EmptySplit;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.operator.PageUtils.recordMaterializedBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class MPDecisionOperator
        implements WorkProcessorSourceOperator // Should implement WorkProcessorSourceOperator? So we'll be able to choose MP per split
{
    private final WorkProcessor<Page> pages;
    private final SplitToPages splitToPages;
    Map<TableHandle, OperatorFactory> options;  // PhysicalOperation is private so I need to use OperatorFactory

    public MPDecisionOperator(
            Session session,
            MemoryTrackingContext memoryTrackingContext,
            DriverContext driverContext,
            WorkProcessor<Split> splits,
            PageSourceProvider pageSourceProvider,
            DynamicFilter dynamicFilter,
            Map<TableHandle, OperatorFactory> options)
    {
        this.splitToPages = new SplitToPages(
                session,
                pageSourceProvider,
                driverContext,
                dynamicFilter,
                memoryTrackingContext.aggregateUserMemoryContext(),
                options);
        this.pages = splits.flatTransform(splitToPages);
        this.options = requireNonNull(options, "options is null");
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public DataSize getPhysicalInputDataSize()
    {
        return splitToPages.getPhysicalInputDataSize();
    }

    @Override
    public long getPhysicalInputPositions()
    {
        return splitToPages.getPhysicalInputPositions();
    }

    @Override
    public DataSize getInputDataSize()
    {
        return splitToPages.getInputDataSize();
    }

    @Override
    public long getInputPositions()
    {
        return splitToPages.getInputPositions();
    }

    @Override
    public long getDynamicFilterSplitsProcessed()
    {
        return splitToPages.getDynamicFilterSplitsProcessed();
    }

    @Override
    public Metrics getConnectorMetrics()
    {
        return splitToPages.source.getMetrics();
    }

    @Override
    public Duration getReadTime()
    {
        return splitToPages.getReadTime();
    }

    @Override
    public void close()
    {
        splitToPages.close();
    }

    private static class SplitToPages
            implements WorkProcessor.Transformation<Split, WorkProcessor<Page>>
    {
        final Session session;
        final PageSourceProvider pageSourceProvider;
        final DriverContext driverContext;
        final DynamicFilter dynamicFilter;
        final LocalMemoryContext memoryContext;

        long processedBytes;
        long processedPositions;
        long dynamicFilterSplitsProcessed;

        @Nullable
        ConnectorPageSource source;

        Map<TableHandle, OperatorFactory> options;

        SplitToPages(
                Session session,
                PageSourceProvider pageSourceProvider,
                DriverContext driverContext,
                DynamicFilter dynamicFilter,
                AggregatedMemoryContext aggregatedMemoryContext,
                Map<TableHandle, OperatorFactory> options)
        {
            this.session = requireNonNull(session, "session is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.driverContext = requireNonNull(driverContext, "driverContext is null");
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(TableScanWorkProcessorOperator.class.getSimpleName());
            this.options = requireNonNull(options, "options is null");
        }

        @Override
        public WorkProcessor.TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                memoryContext.close();
                return WorkProcessor.TransformationState.finished();
            }

            if (!dynamicFilter.getCurrentPredicate().isAll()) {
                dynamicFilterSplitsProcessed++;
            }

            if (split.getConnectorSplit() instanceof EmptySplit) {
                source = new EmptyPageSource();
            }
            else {
                source = pageSourceProvider.createPageSource(session, split, null /* TODO 123 options */, dynamicFilter);
                Optional<TableHandle> chosenMicroPlan = pageSourceProvider.getChosenMicroPlan();
                TableHandle table;
                if (chosenMicroPlan.isEmpty()) {
                    // I'm not sure if the Map should be sorted, since if connector didn't implement getChosenMicroPlan() then the map should contain a single option
                    checkState(options.size() == 1, "MicroPlan is not chosen");
                    table = options.entrySet().stream().findFirst().get().getKey();
                }
                else {
                    table = chosenMicroPlan.get();
                }
                if (!options.containsKey(table)) {
                    throw new RuntimeException();
                }
                OperatorFactory factory = options.get(table);
                Operator operator = factory.createOperator(driverContext);
                return WorkProcessor.TransformationState.ofResult(
                        WorkProcessor.create(new OperatorToPages(operator))
                                .map(page -> {
                                    processedPositions += page.getPositionCount();
                                    recordMaterializedBytes(page, sizeInBytes -> processedBytes += sizeInBytes);
                                    return page;
                                })
                                .blocking(() -> memoryContext.setBytes(source.getMemoryUsage())));
            }

            return WorkProcessor.TransformationState.ofResult(
                    WorkProcessor.create(new ConnectorPageSourceToPages(source))
                            .map(page -> {
                                processedPositions += page.getPositionCount();
                                recordMaterializedBytes(page, sizeInBytes -> processedBytes += sizeInBytes);
                                return page;
                            })
                            .blocking(() -> memoryContext.setBytes(source.getMemoryUsage())));
        }

        DataSize getPhysicalInputDataSize()
        {
            if (source == null) {
                return DataSize.ofBytes(0);
            }

            return DataSize.ofBytes(source.getCompletedBytes());
        }

        long getPhysicalInputPositions()
        {
            if (source == null) {
                return 0;
            }
            return source.getCompletedPositions().orElse(processedPositions);
        }

        DataSize getInputDataSize()
        {
            return DataSize.ofBytes(processedBytes);
        }

        long getInputPositions()
        {
            return processedPositions;
        }

        long getDynamicFilterSplitsProcessed()
        {
            return dynamicFilterSplitsProcessed;
        }

        Duration getReadTime()
        {
            if (source == null) {
                return new Duration(0, NANOSECONDS);
            }

            return new Duration(source.getReadTimeNanos(), NANOSECONDS);
        }

        void close()
        {
            if (source != null) {
                try {
                    source.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static class OperatorToPages
            implements WorkProcessor.Process<Page>
    {
        final Operator operator;

        OperatorToPages(Operator operator)
        {
            this.operator = operator;
        }

        @Override
        public WorkProcessor.ProcessState<Page> process()
        {
            Page page = operator.getOutput();
            if (page == null) {
                if (operator.isFinished()) {
                    return WorkProcessor.ProcessState.finished();
                }
                return WorkProcessor.ProcessState.yielded();
            }

            return WorkProcessor.ProcessState.ofResult(page);
        }
    }

    private static class ConnectorPageSourceToPages
            implements WorkProcessor.Process<Page>
    {
        final ConnectorPageSource pageSource;

        ConnectorPageSourceToPages(ConnectorPageSource pageSource)
        {
            this.pageSource = pageSource;
        }

        @Override
        public WorkProcessor.ProcessState<Page> process()
        {
            if (pageSource.isFinished()) {
                return WorkProcessor.ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return WorkProcessor.ProcessState.blocked(asVoid(toListenableFuture(isBlocked)));
            }

            Page page = pageSource.getNextPage();

            if (page == null) {
                if (pageSource.isFinished()) {
                    return WorkProcessor.ProcessState.finished();
                }
                return WorkProcessor.ProcessState.yielded();
            }

            return WorkProcessor.ProcessState.ofResult(page);
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    public static class MPDecisionOperatorFactory
            implements SourceOperatorFactory, AdapterWorkProcessorSourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSourceProvider pageSourceProvider;
        private final DynamicFilter dynamicFilter;
        private final Map<TableHandle, OperatorFactory> options;

        public MPDecisionOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PageSourceProvider pageSourceProvider,
                DynamicFilter dynamicFilter,
                Map<TableHandle, OperatorFactory> options)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.options = requireNonNull(options, "options is null");
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public int getOperatorId()
        {
            return 0;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return null;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return null;
        }

        @Override
        public String getOperatorType()
        {
            return null;
        }

        @Override
        public WorkProcessorSourceOperator create(Session session, MemoryTrackingContext memoryTrackingContext, DriverYieldSignal yieldSignal, WorkProcessor<Split> splits)
        {
            return null;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, WindowOperator.class.getSimpleName());

            return new WorkProcessorSourceOperatorAdapter(operatorContext, this);
        }

        // TODO 123 I changed the signature to get DriverContext
        @Override
        public WorkProcessorSourceOperator createAdapterOperator(Session session, MemoryTrackingContext memoryTrackingContext, DriverContext driverContext, WorkProcessor<Split> splits)
        {
            return new MPDecisionOperator(
                    session,
                    memoryTrackingContext,
                    driverContext,
                    splits,
                    pageSourceProvider,
                    dynamicFilter,
                    options);
        }
    }
}
