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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.trino.spi.Mergeable;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.util.AutoCloseableCloser;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.operator.OperationTimer.OperationTiming;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class AbstractTableWriterOperator
        implements Operator
{
    public static final int ROW_COUNT_CHANNEL = 0;
    public static final int FRAGMENT_CHANNEL = 1;
    public static final int STATS_START_CHANNEL = 2;

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    protected final OperatorContext operatorContext;
    private final Operator statisticAggregationOperator;
    private final List<Type> types;
    private final boolean statisticsCpuTimerEnabled;

    private ListenableFuture<Void> blocked = NOT_BLOCKED;
    private State state = State.RUNNING;

    protected long rowCount;
    private boolean committed;
    private boolean closed;

    protected final OperationTiming statisticsTiming = new OperationTiming();
    private final Supplier<TableWriterInfo> tableWriterInfoSupplier;

    public AbstractTableWriterOperator(
            OperatorContext operatorContext,
            Operator statisticAggregationOperator,
            List<Type> types,
            boolean statisticsCpuTimerEnabled)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.statisticAggregationOperator = requireNonNull(statisticAggregationOperator, "statisticAggregationOperator is null");
        this.types = requireNonNull(types, "types is null");
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;

        this.tableWriterInfoSupplier = createTableWriterInfoSupplier();
        this.operatorContext.setInfoSupplier(tableWriterInfoSupplier);
    }

    @VisibleForTesting
    Operator getStatisticAggregationOperator()
    {
        return statisticAggregationOperator;
    }

    @VisibleForTesting
    TableWriterInfo getInfo()
    {
        return tableWriterInfoSupplier.get();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        if (state != State.RUNNING || !blocked.isDone()) {
            return false;
        }
        return statisticAggregationOperator.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(needsInput(), "Operator does not need input");

        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        statisticAggregationOperator.addInput(page);
        timer.end(statisticsTiming);
        ListenableFuture<Void> blockedOnAggregation = statisticAggregationOperator.isBlocked();

        ImmutableList.Builder<ListenableFuture<?>> blockedOnWrites = ImmutableList.builder();
        blockedOnWrites.add(blockedOnAggregation);
        blockedOnWrites.addAll(writePage(page));

        updateMemoryUsage();
        blocked = asVoid(allAsList(blockedOnWrites.build()));
        rowCount += page.getPositionCount();
        updateWrittenBytes();
    }

    protected abstract List<ListenableFuture<?>> writePage(Page page);

    @Override
    public Page getOutput()
    {
        if (!blocked.isDone()) {
            return null;
        }

        if (!statisticAggregationOperator.isFinished()) {
            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            Page aggregationOutput = statisticAggregationOperator.getOutput();
            timer.end(statisticsTiming);

            if (aggregationOutput == null) {
                return null;
            }
            return createStatisticsPage(aggregationOutput);
        }

        if (state != State.FINISHING) {
            return null;
        }

        Page fragmentsPage = createFragmentsPage();
        int positionCount = fragmentsPage.getPositionCount();
        Block[] outputBlocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel < STATS_START_CHANNEL) {
                outputBlocks[channel] = fragmentsPage.getBlock(channel);
            }
            else {
                outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
            }
        }

        state = State.FINISHED;
        return new Page(positionCount, outputBlocks);
    }

    private Page createStatisticsPage(Page aggregationOutput)
    {
        int positionCount = aggregationOutput.getPositionCount();
        Block[] outputBlocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel < STATS_START_CHANNEL) {
                outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
            }
            else {
                outputBlocks[channel] = aggregationOutput.getBlock(channel - 2);
            }
        }
        return new Page(positionCount, outputBlocks);
    }

    private Page createFragmentsPage()
    {
        Collection<Slice> fragments = getOutputFragments();
        committed = true;
        updateWrittenBytes();

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(fragments.size() + 1, ImmutableList.of(types.get(ROW_COUNT_CHANNEL), types.get(FRAGMENT_CHANNEL)));
        BlockBuilder rowsBuilder = page.getBlockBuilder(0);
        BlockBuilder fragmentBuilder = page.getBlockBuilder(1);

        // write row count
        page.declarePosition();
        BIGINT.writeLong(rowsBuilder, rowCount);
        fragmentBuilder.appendNull();

        // write fragments
        for (Slice fragment : fragments) {
            page.declarePosition();
            rowsBuilder.appendNull();
            VARBINARY.writeSlice(fragmentBuilder, fragment);
        }

        return page.build();
    }

    protected abstract Collection<Slice> getOutputFragments();

    @Override
    public void finish()
    {
        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        statisticAggregationOperator.finish();
        timer.end(statisticsTiming);

        ImmutableList.Builder<ListenableFuture<?>> blockedFutures = ImmutableList.builder();
        blockedFutures.add(blocked);
        blockedFutures.add(statisticAggregationOperator.isBlocked());

        if (state == State.RUNNING) {
            state = State.FINISHING;
            blockedFutures.addAll(finishWriter());
            updateWrittenBytes();
        }
        this.blocked = asVoid(allAsList(blockedFutures.build()));
    }

    protected abstract List<ListenableFuture<?>> finishWriter();

    @Override
    public void close()
            throws Exception
    {
        AutoCloseableCloser closer = AutoCloseableCloser.create();
        if (!closed) {
            closed = true;
            closeWriter(committed);
        }
        closer.register(() -> statisticAggregationOperator.getOperatorContext().destroy());
        closer.register(statisticAggregationOperator);
        closer.close();
    }

    protected abstract void closeWriter(boolean committed)
            throws Exception;

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED && blocked.isDone();
    }

    protected abstract void updateWrittenBytes();

    protected abstract void updateMemoryUsage();

    protected abstract Supplier<TableWriterInfo> createTableWriterInfoSupplier();

    public static class TableWriterInfo
            implements Mergeable<TableWriterInfo>, OperatorInfo
    {
        private final long pageSinkPeakMemoryUsage;
        private final Duration statisticsWallTime;
        private final Duration statisticsCpuTime;
        private final Duration validationCpuTime;

        @JsonCreator
        public TableWriterInfo(
                @JsonProperty("pageSinkPeakMemoryUsage") long pageSinkPeakMemoryUsage,
                @JsonProperty("statisticsWallTime") Duration statisticsWallTime,
                @JsonProperty("statisticsCpuTime") Duration statisticsCpuTime,
                @JsonProperty("validationCpuTime") Duration validationCpuTime)
        {
            this.pageSinkPeakMemoryUsage = pageSinkPeakMemoryUsage;
            this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
            this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
            this.validationCpuTime = requireNonNull(validationCpuTime, "validationCpuTime is null");
        }

        @JsonProperty
        public long getPageSinkPeakMemoryUsage()
        {
            return pageSinkPeakMemoryUsage;
        }

        @JsonProperty
        public Duration getStatisticsWallTime()
        {
            return statisticsWallTime;
        }

        @JsonProperty
        public Duration getStatisticsCpuTime()
        {
            return statisticsCpuTime;
        }

        @JsonProperty
        public Duration getValidationCpuTime()
        {
            return validationCpuTime;
        }

        @Override
        public TableWriterInfo mergeWith(TableWriterInfo other)
        {
            return new TableWriterInfo(
                    Math.max(pageSinkPeakMemoryUsage, other.pageSinkPeakMemoryUsage),
                    new Duration(statisticsWallTime.getValue(NANOSECONDS) + other.statisticsWallTime.getValue(NANOSECONDS), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(statisticsCpuTime.getValue(NANOSECONDS) + other.statisticsCpuTime.getValue(NANOSECONDS), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(validationCpuTime.getValue(NANOSECONDS) + other.validationCpuTime.getValue(NANOSECONDS), NANOSECONDS).convertToMostSuccinctTimeUnit());
        }

        @Override
        public boolean isFinal()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pageSinkPeakMemoryUsage", pageSinkPeakMemoryUsage)
                    .add("statisticsWallTime", statisticsWallTime)
                    .add("statisticsCpuTime", statisticsCpuTime)
                    .add("validationCpuTime", validationCpuTime)
                    .toString();
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }
}
