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
import io.trino.connector.CatalogName;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;

import java.net.URI;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
        private DirectExchangeClient exchangeClient;
        private boolean closed;

        public ExchangeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                DirectExchangeClientSupplier directExchangeClientSupplier,
                PagesSerdeFactory serdeFactory,
                RetryPolicy retryPolicy)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.directExchangeClientSupplier = directExchangeClientSupplier;
            this.serdeFactory = serdeFactory;
            this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
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
            if (exchangeClient == null) {
                exchangeClient = directExchangeClientSupplier.get(driverContext.getPipelineContext().localSystemMemoryContext(), taskContext::sourceTaskFailed, retryPolicy);
            }

            return new ExchangeOperator(
                    operatorContext,
                    sourceId,
                    serdeFactory.createPagesSerde(),
                    exchangeClient);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final DirectExchangeClient exchangeClient;
    private final PagesSerde serde;
    private ListenableFuture<Void> isBlocked = NOT_BLOCKED;

    public ExchangeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PagesSerde serde,
            DirectExchangeClient exchangeClient)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeClient = requireNonNull(exchangeClient, "exchangeClient is null");
        this.serde = requireNonNull(serde, "serde is null");

        operatorContext.setInfoSupplier(exchangeClient::getStatus);
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

        RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
        exchangeClient.addLocation(remoteSplit.getTaskId(), URI.create(remoteSplit.getLocation()));

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        exchangeClient.noMoreLocations();
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
        return exchangeClient.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        // Avoid registering a new callback in the ExchangeClient when one is already pending
        if (isBlocked.isDone()) {
            isBlocked = exchangeClient.isBlocked();
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
        Slice page = exchangeClient.pollPage();
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
        exchangeClient.close();
    }
}
