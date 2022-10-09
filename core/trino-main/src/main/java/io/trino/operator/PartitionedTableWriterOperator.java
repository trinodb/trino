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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import io.trino.split.PageSinkManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.util.AutoCloseableCloser;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.trino.sql.planner.plan.TableWriterNode.RefreshMaterializedViewTarget;
import static io.trino.sql.planner.plan.TableWriterNode.TableExecuteTarget;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Partitions the pages and then create a separate pageSink for each partitioned page.
 * Therefore, it allows us to track physicalWrittenBytes per partition.
 */
public class PartitionedTableWriterOperator
        extends AbstractTableWriterOperator
{
    public static class PartitionedTableWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final WriterTarget target;
        private final List<Integer> columnChannels;
        private final Session session;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final List<Type> types;
        private final PartitionFunctionFactory partitionFunctionFactory;
        private final PartitioningHandle partitioning;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionChannelTypes;
        private final int partitionCount;
        private boolean closed;

        public PartitionedTableWriterOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PageSinkManager pageSinkManager,
                WriterTarget writerTarget,
                List<Integer> columnChannels,
                Session session,
                OperatorFactory statisticsAggregationOperatorFactory,
                List<Type> types,
                PartitionFunctionFactory partitionFunctionFactory,
                PartitioningHandle partitioning,
                List<Integer> partitionChannels,
                List<Type> partitionChannelTypes,
                int partitionCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            requireNonNull(writerTarget, "writerTarget is null");
            checkArgument(
                    writerTarget instanceof CreateTarget
                            || writerTarget instanceof InsertTarget
                            || writerTarget instanceof RefreshMaterializedViewTarget
                            || writerTarget instanceof TableExecuteTarget,
                    "writerTarget must be CreateTarget, InsertTarget, RefreshMaterializedViewTarget or TableExecuteTarget");
            this.target = writerTarget;
            this.session = requireNonNull(session, "session is null");
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.types = requireNonNull(types, "types is null");
            this.partitionFunctionFactory = requireNonNull(partitionFunctionFactory, "partitionFunctionFactory is null");
            this.partitioning = requireNonNull(partitioning, "partitioning is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionChannelTypes = requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
            this.partitionCount = partitionCount;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, PartitionedTableWriterOperator.class.getSimpleName());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean statisticsCpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);

            PartitionFunction partitionFunction = partitionFunctionFactory.create(
                    session,
                    partitioning,
                    partitionChannels,
                    partitionChannelTypes,
                    Optional.empty(),
                    partitionCount);
            Function<Page, Page> partitionPagePreparer = partitionFunctionFactory.createPartitionPagePreparer(partitioning, partitionChannels);

            return new PartitionedTableWriterOperator(
                    context,
                    statisticsAggregationOperator,
                    types,
                    statisticsCpuTimerEnabled,
                    this::createPageSink,
                    Ints.toArray(columnChannels),
                    partitionFunction,
                    partitionPagePreparer,
                    partitionCount);
        }

        private ConnectorPageSink createPageSink()
        {
            if (target instanceof TableWriterNode.CreateTarget) {
                return pageSinkManager.createPageSink(session, ((TableWriterNode.CreateTarget) target).getHandle());
            }
            if (target instanceof TableWriterNode.InsertTarget) {
                return pageSinkManager.createPageSink(session, ((TableWriterNode.InsertTarget) target).getHandle());
            }
            if (target instanceof TableWriterNode.RefreshMaterializedViewTarget) {
                return pageSinkManager.createPageSink(session, ((TableWriterNode.RefreshMaterializedViewTarget) target).getInsertHandle());
            }
            if (target instanceof TableWriterNode.TableExecuteTarget) {
                return pageSinkManager.createPageSink(session, ((TableWriterNode.TableExecuteTarget) target).getExecuteHandle());
            }
            throw new UnsupportedOperationException("Unhandled target type: " + target.getClass().getName());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PartitionedTableWriterOperatorFactory(
                    operatorId,
                    planNodeId,
                    pageSinkManager,
                    target,
                    columnChannels,
                    session,
                    statisticsAggregationOperatorFactory,
                    types,
                    partitionFunctionFactory,
                    partitioning,
                    partitionChannels,
                    partitionChannelTypes,
                    partitionCount);
        }
    }

    private final LocalMemoryContext pageSinkMemoryContext;
    private final Supplier<ConnectorPageSink> pageSinkSupplier;
    private final int[] columnChannels;
    private final AtomicLong pageSinkPeakMemoryUsage = new AtomicLong();
    private final PartitionFunction partitionFunction;
    private final Function<Page, Page> partitionPagePreparer;

    private List<CompletableFuture<Collection<Slice>>> pageSinkFinishFutures = ImmutableList.of();
    private final ConnectorPageSink[] partitionPageSinks;
    private final IntArrayList[] partitionAssignments;

    private long writtenBytes;

    public PartitionedTableWriterOperator(
            OperatorContext operatorContext,
            Operator statisticAggregationOperator,
            List<Type> types,
            boolean statisticsCpuTimerEnabled,
            Supplier<ConnectorPageSink> pageSinkSupplier,
            int[] columnChannels,
            PartitionFunction partitionFunction,
            Function<Page, Page> partitionPagePreparer,
            int partitionCount)
    {
        super(operatorContext, statisticAggregationOperator, types, statisticsCpuTimerEnabled);

        this.pageSinkMemoryContext = operatorContext.newLocalUserMemoryContext(PartitionedTableWriterOperator.class.getSimpleName());
        this.pageSinkSupplier = requireNonNull(pageSinkSupplier, "pageSinkSupplier is null");
        this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionPagePreparer = requireNonNull(partitionPagePreparer, "partitionPagePreparer is null");

        // Initialize partitionPageSinks and partitionAssignments with the artificial partition limit.
        partitionPageSinks = new ConnectorPageSink[partitionCount];
        partitionAssignments = new IntArrayList[partitionCount];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
            partitionPageSinks[i] = null;
        }
    }

    @Override
    protected List<ListenableFuture<?>> writePage(Page page)
    {
        ImmutableList.Builder<ListenableFuture<?>> blockedOnWrites = ImmutableList.builder();
        Page partitionedPage = partitionPagePreparer.apply(page);

        // Assign each row to a partition which limits to the partitionCount. If there are more physical partitions than
        // this artificial limit, then it is possible that multiple physical partitions will get assigned the same
        // bucket id. Thus, multiple partitions will be scaled together since we track partition physicalWrittenBytes
        // using the artificial limit (partitionCount). The assignments lists are all expected to cleared by the
        // previous iterations.
        for (int position = 0; position < partitionedPage.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(partitionedPage, position);
            partitionAssignments[partition].add(position);
        }

        // Send partitioned page to respective page sink
        for (int partition = 0; partition < partitionAssignments.length; partition++) {
            IntArrayList positionsList = partitionAssignments[partition];
            int partitionSize = positionsList.size();
            if (partitionSize == 0) {
                continue;
            }

            // Lazily initialize a new pageSink for a partition
            ConnectorPageSink pageSink = partitionPageSinks[partition];
            if (pageSink == null) {
                pageSink = pageSinkSupplier.get();
                partitionPageSinks[partition] = pageSink;
            }

            // clear the assigned positions list size for the next iteration to start empty. This
            // only resets the size() to 0 which controls the index where subsequent calls to add()
            // will store new values, but does not modify the positions array
            int[] positions = positionsList.elements();
            positionsList.clear();

            if (partitionSize == page.getPositionCount()) {
                // entire page will be sent to this writer.
                blockedOnWrites.add(sendPageToPartitionPageSink(pageSink, page));
                break;
            }

            Page pageSplit = page.copyPositions(positions, 0, partitionSize);
            blockedOnWrites.add(sendPageToPartitionPageSink(pageSink, pageSplit));
        }

        return blockedOnWrites.build();
    }

    private ListenableFuture<?> sendPageToPartitionPageSink(ConnectorPageSink pageSink, Page page)
    {
        // Only send the columnChannels
        CompletableFuture<?> future = pageSink.appendPage(page.getColumns(columnChannels));
        return toListenableFuture(future);
    }

    @Override
    protected Collection<Slice> getOutputFragments()
    {
        return pageSinkFinishFutures.stream()
                .flatMap(future -> getFutureValue(future).stream())
                .collect(toImmutableList());
    }

    @Override
    protected List<ListenableFuture<?>> finishWriter()
    {
        ImmutableList.Builder<ListenableFuture<?>> blockedFutures = ImmutableList.builder();
        ImmutableList.Builder<CompletableFuture<Collection<Slice>>> futures = ImmutableList.builder();
        for (ConnectorPageSink pageSink : partitionPageSinks) {
            if (pageSink != null) {
                CompletableFuture<Collection<Slice>> finish = pageSink.finish();
                futures.add(finish);
                blockedFutures.add(toListenableFuture(finish));
            }
        }
        pageSinkFinishFutures = futures.build();
        return blockedFutures.build();
    }

    @Override
    protected void closeWriter(boolean committed)
            throws Exception
    {
        AutoCloseableCloser closer = AutoCloseableCloser.create();
        if (!committed) {
            for (ConnectorPageSink pageSink : partitionPageSinks) {
                if (pageSink != null) {
                    closer.register(pageSink::abort);
                }
            }
        }
        closer.register(pageSinkMemoryContext::close);
        closer.close();
    }

    @Override
    protected void updateWrittenBytes()
    {
        long currentPhysicalWrittenBytes = 0;
        for (int partition = 0; partition < partitionPageSinks.length; partition++) {
            ConnectorPageSink pageSink = partitionPageSinks[partition];
            long completedBytes = 0;
            if (pageSink != null) {
                completedBytes = pageSink.getCompletedBytes();
                operatorContext.setPartitionPhysicalWrittenBytes(partition, completedBytes);
            }
            currentPhysicalWrittenBytes += completedBytes;
        }
        operatorContext.recordPhysicalWrittenData(currentPhysicalWrittenBytes - writtenBytes);
        writtenBytes = currentPhysicalWrittenBytes;
    }

    @Override
    protected void updateMemoryUsage()
    {
        long pageSinkMemoryUsage = 0;
        for (ConnectorPageSink pageSink : partitionPageSinks) {
            if (pageSink != null) {
                pageSinkMemoryUsage += pageSink.getMemoryUsage();
            }
        }
        pageSinkMemoryContext.setBytes(pageSinkMemoryUsage);
        pageSinkPeakMemoryUsage.accumulateAndGet(pageSinkMemoryUsage, Math::max);
    }

    @Override
    protected Supplier<TableWriterInfo> createTableWriterInfoSupplier()
    {
        return () -> {
            double validationCpuNanos = 0;
            for (ConnectorPageSink pageSink : partitionPageSinks) {
                if (pageSink != null) {
                    validationCpuNanos += pageSink.getValidationCpuNanos();
                }
            }
            return new TableWriterInfo(
                    pageSinkPeakMemoryUsage.get(),
                    new Duration(statisticsTiming.getWallNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(statisticsTiming.getCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(validationCpuNanos, NANOSECONDS).convertToMostSuccinctTimeUnit());
        };
    }
}
