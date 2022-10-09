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
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import io.trino.util.AutoCloseableCloser;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TableWriterOperator
        extends AbstractTableWriterOperator
{
    public static class TableWriterOperatorFactory
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
        private boolean closed;

        public TableWriterOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PageSinkManager pageSinkManager,
                WriterTarget writerTarget,
                List<Integer> columnChannels,
                Session session,
                OperatorFactory statisticsAggregationOperatorFactory,
                List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            checkArgument(
                    writerTarget instanceof CreateTarget
                            || writerTarget instanceof InsertTarget
                            || writerTarget instanceof TableWriterNode.RefreshMaterializedViewTarget
                            || writerTarget instanceof TableWriterNode.TableExecuteTarget,
                    "writerTarget must be CreateTarget, InsertTarget, RefreshMaterializedViewTarget or TableExecuteTarget");
            this.target = requireNonNull(writerTarget, "writerTarget is null");
            this.session = requireNonNull(session, "session is null");
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableWriterOperator.class.getSimpleName());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean statisticsCpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            return new TableWriterOperator(context, statisticsAggregationOperator, types, statisticsCpuTimerEnabled, createPageSink(), Ints.toArray(columnChannels));
        }

        private ConnectorPageSink createPageSink()
        {
            if (target instanceof CreateTarget) {
                return pageSinkManager.createPageSink(session, ((CreateTarget) target).getHandle());
            }
            if (target instanceof InsertTarget) {
                return pageSinkManager.createPageSink(session, ((InsertTarget) target).getHandle());
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
            return new TableWriterOperatorFactory(operatorId, planNodeId, pageSinkManager, target, columnChannels, session, statisticsAggregationOperatorFactory, types);
        }
    }

    private final LocalMemoryContext pageSinkMemoryContext;
    private final ConnectorPageSink pageSink;
    private final int[] columnChannels;
    private final AtomicLong pageSinkPeakMemoryUsage = new AtomicLong();

    private CompletableFuture<Collection<Slice>> finishFuture;
    private long writtenBytes;

    public TableWriterOperator(
            OperatorContext operatorContext,
            Operator statisticAggregationOperator,
            List<Type> types,
            boolean statisticsCpuTimerEnabled,
            ConnectorPageSink pageSink,
            int[] columnChannels)
    {
        super(operatorContext, statisticAggregationOperator, types, statisticsCpuTimerEnabled);
        this.pageSinkMemoryContext = operatorContext.newLocalUserMemoryContext(TableWriterOperator.class.getSimpleName());
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.columnChannels = columnChannels;
    }

    @Override
    protected List<ListenableFuture<?>> finishWriter()
    {
        finishFuture = pageSink.finish();
        return ImmutableList.of(toListenableFuture(finishFuture));
    }

    @Override
    protected List<ListenableFuture<?>> writePage(Page page)
    {
        CompletableFuture<?> future = pageSink.appendPage(page.getColumns(columnChannels));
        return ImmutableList.of(toListenableFuture(future));
    }

    @Override
    protected Collection<Slice> getOutputFragments()
    {
        return getFutureValue(finishFuture);
    }

    @Override
    protected void closeWriter(boolean committed)
            throws Exception
    {
        AutoCloseableCloser closer = AutoCloseableCloser.create();
        if (!committed) {
            closer.register(pageSink::abort);
        }
        closer.register(pageSinkMemoryContext::close);
        closer.close();
    }

    @Override
    protected void updateWrittenBytes()
    {
        long current = pageSink.getCompletedBytes();
        operatorContext.recordPhysicalWrittenData(current - writtenBytes);
        writtenBytes = current;
    }

    @Override
    protected void updateMemoryUsage()
    {
        long pageSinkMemoryUsage = pageSink.getMemoryUsage();
        pageSinkMemoryContext.setBytes(pageSinkMemoryUsage);
        pageSinkPeakMemoryUsage.accumulateAndGet(pageSinkMemoryUsage, Math::max);
    }

    @Override
    protected Supplier<TableWriterInfo> createTableWriterInfoSupplier()
    {
        return () -> new TableWriterInfo(
                pageSinkPeakMemoryUsage.get(),
                new Duration(statisticsTiming.getWallNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(statisticsTiming.getCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(pageSink.getValidationCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit());
    }
}
