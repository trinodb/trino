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
import io.airlift.slice.Slice;
import io.trino.connector.CatalogHandle;
import io.trino.exchange.ExchangeDataSource;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.exchange.LazyExchangeDataSource;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;

public class ExchangeOperator
        implements SourceOperator
{
    public static final CatalogHandle REMOTE_CATALOG_HANDLE = createRootCatalogHandle("$remote");

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
            LocalMemoryContext memoryContext = driverContext.getPipelineContext().localMemoryContext();
            if (exchangeDataSource == null) {
                // The decision of what exchange to use (streaming vs external) is currently made at the scheduling phase. It is more convenient to deliver it as part of a RemoteSplit.
                // Postponing this decision until scheduling allows to dynamically change the exchange type as part of an adaptive query re-planning.
                // LazyExchangeDataSource allows to choose an exchange source implementation based on the information received from a split.
                exchangeDataSource = new LazyExchangeDataSource(
                        taskContext.getTaskId(),
                        sourceId,
                        directExchangeClientSupplier,
                        memoryContext,
                        taskContext::sourceTaskFailed,
                        retryPolicy,
                        exchangeManagerRegistry);
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
        checkArgument(split.getCatalogHandle().equals(REMOTE_CATALOG_HANDLE), "split is not a remote split");

        RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
        exchangeDataSource.addInput(remoteSplit.getExchangeInput());

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        exchangeDataSource.noMoreInputs();
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

        Page deserializedPage = serde.deserialize(page);
        operatorContext.recordNetworkInput(page.length(), deserializedPage.getPositionCount());
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), deserializedPage.getPositionCount());

        return deserializedPage;
    }

    @Override
    public void close()
    {
        exchangeDataSource.close();
    }
}
