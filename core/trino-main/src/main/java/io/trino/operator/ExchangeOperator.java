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
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.trino.connector.CatalogName;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.TaskFailureListener;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.SerializedPage;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.split.RemoteSplit;
import io.trino.split.RemoteSplit.DirectExchangeInput;
import io.trino.split.RemoteSplit.ExternalExchangeInput;
import io.trino.sql.planner.plan.PlanNodeId;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.execution.buffer.PagesSerdeUtil.readSerializedPage;
import static java.util.Objects.requireNonNull;

public class ExchangeOperator
        implements SourceOperator
{
    public static final CatalogName REMOTE_CONNECTOR_ID = new CatalogName("$remote");

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
            LocalMemoryContext memoryContext = driverContext.getPipelineContext().localSystemMemoryContext();
            if (exchangeDataSource == null) {
                exchangeDataSource = new LazyExchangeDataSource(directExchangeClientSupplier, memoryContext, taskContext::sourceTaskFailed, retryPolicy, exchangeManagerRegistry);
            }
            return new ExchangeOperator(
                    operatorContext,
                    sourceId,
                    exchangeDataSource,
                    serdeFactory.createPagesSerde());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeDataSource exchangeDataSource;
    private final PagesSerde serde;
    private ListenableFuture<Void> isBlocked = NOT_BLOCKED;

    public ExchangeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            ExchangeDataSource exchangeDataSource,
            PagesSerde serde)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeDataSource = requireNonNull(exchangeDataSource, "exchangeDataSource is null");
        this.serde = requireNonNull(serde, "serde is null");

        operatorContext.setInfoSupplier(exchangeDataSource::getInfo);
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getCatalogName().equals(REMOTE_CONNECTOR_ID), "split is not a remote split");

        exchangeDataSource.addSplit((RemoteSplit) split.getConnectorSplit());

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        exchangeDataSource.noMoreSplits();
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
        SerializedPage page = exchangeDataSource.pollPage();
        if (page == null) {
            return null;
        }

        operatorContext.recordNetworkInput(page.getSizeInBytes(), page.getPositionCount());

        Page deserializedPage = serde.deserialize(page);
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), page.getPositionCount());

        return deserializedPage;
    }

    @Override
    public void close()
    {
        exchangeDataSource.close();
    }

    private interface ExchangeDataSource
            extends Closeable
    {
        SerializedPage pollPage();

        boolean isFinished();

        ListenableFuture<Void> isBlocked();

        void addSplit(RemoteSplit remoteSplit);

        void noMoreSplits();

        OperatorInfo getInfo();

        @Override
        void close();
    }

    private static class LazyExchangeDataSource
            implements ExchangeDataSource
    {
        private final DirectExchangeClientSupplier directExchangeClientSupplier;
        private final LocalMemoryContext systemMemoryContext;
        private final TaskFailureListener taskFailureListener;
        private final RetryPolicy retryPolicy;
        private final ExchangeManagerRegistry exchangeManagerRegistry;

        private final SettableFuture<Void> initializationFuture = SettableFuture.create();
        private final AtomicReference<ExchangeDataSource> delegate = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean();

        private LazyExchangeDataSource(
                DirectExchangeClientSupplier directExchangeClientSupplier,
                LocalMemoryContext systemMemoryContext,
                TaskFailureListener taskFailureListener,
                RetryPolicy retryPolicy,
                ExchangeManagerRegistry exchangeManagerRegistry)
        {
            this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
            this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
            this.taskFailureListener = requireNonNull(taskFailureListener, "taskFailureListener is null");
            this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        }

        @Override
        public SerializedPage pollPage()
        {
            if (closed.get()) {
                return null;
            }

            ExchangeDataSource dataSource = delegate.get();
            if (dataSource == null) {
                return null;
            }
            return dataSource.pollPage();
        }

        @Override
        public boolean isFinished()
        {
            if (closed.get()) {
                return true;
            }
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource == null) {
                return false;
            }
            return dataSource.isFinished();
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            if (closed.get()) {
                return immediateVoidFuture();
            }
            if (!initializationFuture.isDone()) {
                return initializationFuture;
            }
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource == null) {
                return immediateVoidFuture();
            }
            return dataSource.isBlocked();
        }

        @Override
        public void addSplit(RemoteSplit remoteSplit)
        {
            SettableFuture<Void> future = null;
            synchronized (this) {
                if (closed.get()) {
                    return;
                }
                ExchangeDataSource dataSource = delegate.get();
                if (dataSource == null) {
                    if (remoteSplit.getExchangeInput() instanceof DirectExchangeInput) {
                        DirectExchangeClient directExchangeClient = directExchangeClientSupplier.get(systemMemoryContext, taskFailureListener, retryPolicy);
                        dataSource = new DirectExchangeDataSource(directExchangeClient);
                        dataSource.addSplit(remoteSplit);
                    }
                    else if (remoteSplit.getExchangeInput() instanceof ExternalExchangeInput) {
                        ExternalExchangeInput externalExchangeInput = (ExternalExchangeInput) remoteSplit.getExchangeInput();
                        ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
                        ExchangeSource exchangeSource = exchangeManager.createSource(externalExchangeInput.getExchangeSourceHandles());
                        dataSource = new ExternalExchangeDataSource(exchangeSource, systemMemoryContext);
                    }
                    else {
                        throw new IllegalArgumentException("Unexpected split: " + remoteSplit);
                    }
                    delegate.set(dataSource);
                    future = initializationFuture;
                }
                else {
                    dataSource.addSplit(remoteSplit);
                }
            }

            if (future != null) {
                future.set(null);
            }
        }

        @Override
        public synchronized void noMoreSplits()
        {
            if (closed.get()) {
                return;
            }
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource != null) {
                dataSource.noMoreSplits();
            }
            else {
                close();
            }
        }

        @Override
        public OperatorInfo getInfo()
        {
            ExchangeDataSource dataSource = delegate.get();
            if (dataSource == null) {
                return null;
            }
            return dataSource.getInfo();
        }

        @Override
        public void close()
        {
            SettableFuture<Void> future;
            synchronized (this) {
                if (!closed.compareAndSet(false, true)) {
                    return;
                }
                ExchangeDataSource dataSource = delegate.get();
                if (dataSource != null) {
                    dataSource.close();
                }
                future = initializationFuture;
            }
            future.set(null);
        }
    }

    private static class DirectExchangeDataSource
            implements ExchangeDataSource
    {
        private final DirectExchangeClient directExchangeClient;

        private DirectExchangeDataSource(DirectExchangeClient directExchangeClient)
        {
            this.directExchangeClient = requireNonNull(directExchangeClient, "directExchangeClient is null");
        }

        @Override
        public SerializedPage pollPage()
        {
            return directExchangeClient.pollPage();
        }

        @Override
        public boolean isFinished()
        {
            return directExchangeClient.isFinished();
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return directExchangeClient.isBlocked();
        }

        @Override
        public void addSplit(RemoteSplit remoteSplit)
        {
            DirectExchangeInput taskInput = (DirectExchangeInput) remoteSplit.getExchangeInput();
            directExchangeClient.addLocation(taskInput.getTaskId(), taskInput.getLocation());
        }

        @Override
        public void noMoreSplits()
        {
            directExchangeClient.noMoreLocations();
        }

        @Override
        public OperatorInfo getInfo()
        {
            return directExchangeClient.getStatus();
        }

        @Override
        public void close()
        {
            directExchangeClient.close();
        }
    }

    private static class ExternalExchangeDataSource
            implements ExchangeDataSource
    {
        private final ExchangeSource exchangeSource;
        private final LocalMemoryContext systemMemoryContext;

        private ExternalExchangeDataSource(ExchangeSource exchangeSource, LocalMemoryContext systemMemoryContext)
        {
            this.exchangeSource = requireNonNull(exchangeSource, "exchangeSource is null");
            this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        }

        @Override
        public SerializedPage pollPage()
        {
            Slice data = exchangeSource.read();
            systemMemoryContext.setBytes(exchangeSource.getSystemMemoryUsage());
            if (data == null) {
                return null;
            }
            // TODO: Avoid extra memory copy
            return readSerializedPage(data.getInput());
        }

        @Override
        public boolean isFinished()
        {
            return exchangeSource.isFinished();
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return asVoid(toListenableFuture(exchangeSource.isBlocked()));
        }

        @Override
        public void addSplit(RemoteSplit remoteSplit)
        {
            // ignore
        }

        @Override
        public void noMoreSplits()
        {
            // ignore
        }

        @Override
        public OperatorInfo getInfo()
        {
            return null;
        }

        @Override
        public void close()
        {
            exchangeSource.close();
        }
    }
}
