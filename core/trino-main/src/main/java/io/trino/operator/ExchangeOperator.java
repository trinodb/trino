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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.slice.Slice;
import io.trino.exchange.ExchangeDataSource;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.exchange.LazyExchangeDataSource;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.exchange.ExchangeId;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.util.Ciphers;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExchangeOperator
        implements SourceOperator
{
    public static final CatalogHandle REMOTE_CATALOG_HANDLE = createRootCatalogHandle("$remote", new CatalogVersion("remote"));

    public static class ExchangeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final DirectExchangeClientSupplier directExchangeClientSupplier;
        private final PagesSerdeFactory serdeFactory;
        private final RetryPolicy retryPolicy;
        private final ExchangeManagerRegistry exchangeManagerRegistry;
        private ExchangeDataSource exchangeDataSource;
        private boolean closed;

        private final NoMoreSplitsTracker noMoreSplitsTracker = new NoMoreSplitsTracker();
        private int nextOperatorInstanceId;

        public ExchangeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                DirectExchangeClientSupplier directExchangeClientSupplier,
                PagesSerdeFactory serdeFactory,
                RetryPolicy retryPolicy,
                ExchangeManagerRegistry exchangeManagerRegistry)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.directExchangeClientSupplier = directExchangeClientSupplier;
            this.serdeFactory = serdeFactory;
            this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, ExchangeOperator.class.getSimpleName());
            LocalMemoryContext memoryContext = driverContext.getPipelineContext().localMemoryContext();
            if (exchangeDataSource == null) {
                // The decision of what exchange to use (streaming vs external) is currently made at the scheduling phase. It is more convenient to deliver it as part of a RemoteSplit.
                // Postponing this decision until scheduling allows to dynamically change the exchange type as part of an adaptive query re-planning.
                // LazyExchangeDataSource allows to choose an exchange source implementation based on the information received from a split.
                TaskId taskId = taskContext.getTaskId();
                exchangeDataSource = new LazyExchangeDataSource(
                        taskId.getQueryId(),
                        new ExchangeId(format("direct-exchange-%s-%s", taskId.getStageId().getId(), sourceId)),
                        directExchangeClientSupplier,
                        memoryContext,
                        taskContext::sourceTaskFailed,
                        retryPolicy,
                        exchangeManagerRegistry);
            }
            int operatorInstanceId = nextOperatorInstanceId;
            nextOperatorInstanceId++;
            ExchangeOperator exchangeOperator = new ExchangeOperator(
                    operatorContext,
                    sourceId,
                    exchangeDataSource,
                    serdeFactory.createDeserializer(driverContext.getSession().getExchangeEncryptionKey().map(Ciphers::deserializeAesEncryptionKey)),
                    noMoreSplitsTracker,
                    operatorInstanceId);
            noMoreSplitsTracker.operatorAdded(operatorInstanceId);
            return exchangeOperator;
        }

        @Override
        public void noMoreOperators()
        {
            noMoreSplitsTracker.noMoreOperators();
            if (noMoreSplitsTracker.isNoMoreSplits()) {
                if (exchangeDataSource != null) {
                    exchangeDataSource.noMoreInputs();
                }
            }
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeDataSource exchangeDataSource;
    private final PageDeserializer deserializer;
    private final NoMoreSplitsTracker noMoreSplitsTracker;
    private final int operatorInstanceId;

    private ListenableFuture<Void> isBlocked = NOT_BLOCKED;

    public ExchangeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            ExchangeDataSource exchangeDataSource,
            PageDeserializer deserializer,
            NoMoreSplitsTracker noMoreSplitsTracker,
            int operatorInstanceId)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeDataSource = requireNonNull(exchangeDataSource, "exchangeDataSource is null");
        this.deserializer = requireNonNull(deserializer, "serializer is null");
        this.noMoreSplitsTracker = requireNonNull(noMoreSplitsTracker, "noMoreSplitsTracker is null");
        this.operatorInstanceId = operatorInstanceId;

        LocalMemoryContext memoryContext = operatorContext.localUserMemoryContext();
        // memory footprint of deserializer does not change over time
        memoryContext.setBytes(deserializer.getRetainedSizeInBytes());

        operatorContext.setInfoSupplier(exchangeDataSource::getInfo);
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public void addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getCatalogHandle().equals(REMOTE_CATALOG_HANDLE), "split is not a remote split");

        RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
        exchangeDataSource.addInput(remoteSplit.getExchangeInput());
    }

    @Override
    public void noMoreSplits()
    {
        noMoreSplitsTracker.noMoreSplits(operatorInstanceId);
        if (noMoreSplitsTracker.isNoMoreSplits()) {
            exchangeDataSource.noMoreInputs();
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return exchangeDataSource.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        // Avoid registering a new callback in the data source when one is already pending
        if (isBlocked.isDone()) {
            isBlocked = exchangeDataSource.isBlocked();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " cannot take input");
    }

    @Override
    public Page getOutput()
    {
        Slice page = exchangeDataSource.pollPage();
        if (page == null) {
            return null;
        }

        Page deserializedPage = deserializer.deserialize(page);
        operatorContext.recordNetworkInput(page.length(), deserializedPage.getPositionCount());
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), deserializedPage.getPositionCount());

        return deserializedPage;
    }

    @Override
    public void close()
    {
        exchangeDataSource.close();
    }

    @ThreadSafe
    private static class NoMoreSplitsTracker
    {
        private final IntSet allOperators = new IntOpenHashSet();
        private final IntSet noMoreSplitsOperators = new IntOpenHashSet();
        private boolean noMoreOperators;

        public synchronized void operatorAdded(int operatorInstanceId)
        {
            checkState(!noMoreOperators, "noMoreOperators is set");
            allOperators.add(operatorInstanceId);
        }

        public synchronized void noMoreOperators()
        {
            noMoreOperators = true;
        }

        public synchronized void noMoreSplits(int operatorInstanceId)
        {
            checkState(allOperators.contains(operatorInstanceId), "operatorInstanceId not found: %s", operatorInstanceId);
            noMoreSplitsOperators.add(operatorInstanceId);
        }

        public synchronized boolean isNoMoreSplits()
        {
            return noMoreOperators && noMoreSplitsOperators.containsAll(allOperators);
        }
    }
}
