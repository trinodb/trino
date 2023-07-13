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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.type.Type;
import io.trino.split.PageSinkId;
import io.trino.split.PageSinkManager;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;

public class MergeWriterOperator
        implements Operator
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARBINARY);

    public static class MergeWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final MergeTarget target;
        private final Session session;
        private final Function<Page, Page> pagePreprocessor;

        private boolean closed;

        public MergeWriterOperatorFactory(int operatorId, PlanNodeId planNodeId, PageSinkManager pageSinkManager, MergeTarget target, Session session, Function<Page, Page> pagePreprocessor)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            this.target = requireNonNull(target, "target is null");
            this.session = requireNonNull(session, "session is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, MergeWriterOperator.class.getSimpleName());
            ConnectorMergeSink mergeSink = pageSinkManager.createMergeSink(session, target.getMergeHandle().orElseThrow(), PageSinkId.fromTaskId(driverContext.getTaskId()));
            return new MergeWriterOperator(context, mergeSink, pagePreprocessor);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new MergeWriterOperatorFactory(operatorId, planNodeId, pageSinkManager, target, session, pagePreprocessor);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private State state = State.RUNNING;
    private final ConnectorMergeSink mergeSink;
    private final Function<Page, Page> pagePreprocessor;
    private ListenableFuture<Collection<Slice>> finishFuture;
    private ListenableFuture<Void> blockedFutureView;
    private long rowCount;
    private boolean closed;

    public MergeWriterOperator(OperatorContext operatorContext, ConnectorMergeSink mergeSink, Function<Page, Page> pagePreprocessor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.mergeSink = requireNonNull(mergeSink, "mergeSink is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.RUNNING;
    }

    @Override
    public void addInput(Page suppliedPage)
    {
        requireNonNull(suppliedPage, "suppliedPage is null");
        Page page = pagePreprocessor.apply(suppliedPage);
        checkState(state == State.RUNNING, "Operator is %s", state);

        // Copy all but the last block to a new page.
        // The last block exists only to get the rowCount right.
        int outputChannelCount = page.getChannelCount() - 1;
        int[] columns = IntStream.range(0, outputChannelCount).toArray();
        Page newPage = page.getColumns(columns);

        // Store the page
        mergeSink.storeMergedRows(newPage);

        // Calculate the amount to increment the rowCount
        Block insertFromUpdateColumn = page.getBlock(page.getChannelCount() - 1);
        long insertsFromUpdates = 0;
        int positionCount = page.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            insertsFromUpdates += TINYINT.getByte(insertFromUpdateColumn, position);
        }
        rowCount += positionCount - insertsFromUpdates;
    }

    @Override
    public Page getOutput()
    {
        if ((state != State.FINISHING) || !finishFuture.isDone()) {
            return null;
        }
        state = State.FINISHED;

        Collection<Slice> fragments = getFutureValue(finishFuture);

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(fragments.size() + 1, TYPES);
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

    @Override
    public void finish()
    {
        if (state == State.RUNNING) {
            state = State.FINISHING;
            finishFuture = toListenableFuture(mergeSink.finish());
            blockedFutureView = asVoid(finishFuture);
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        if (blockedFutureView == null) {
            return NOT_BLOCKED;
        }
        return blockedFutureView;
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            if (finishFuture != null) {
                finishFuture.cancel(true);
            }
        }
    }
}
