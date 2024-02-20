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
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionProcessorState.Blocked;
import io.trino.spi.function.table.TableFunctionProcessorState.Processed;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.split.EmptySplit;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayDeque;
import java.util.Deque;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static java.util.Objects.requireNonNull;

public class LeafTableFunctionOperator
        implements SourceOperator
{
    public static class LeafTableFunctionOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final CatalogHandle functionCatalog;
        private final TableFunctionProcessorProvider tableFunctionProvider;
        private final ConnectorTableFunctionHandle functionHandle;
        private boolean closed;

        public LeafTableFunctionOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                CatalogHandle functionCatalog,
                TableFunctionProcessorProvider tableFunctionProvider,
                ConnectorTableFunctionHandle functionHandle)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.functionCatalog = requireNonNull(functionCatalog, "functionCatalog is null");
            this.tableFunctionProvider = requireNonNull(tableFunctionProvider, "tableFunctionProvider is null");
            this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, LeafTableFunctionOperator.class.getSimpleName());
            return new LeafTableFunctionOperator(operatorContext, sourceId, functionCatalog, tableFunctionProvider, functionHandle);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final TableFunctionProcessorProvider tableFunctionProvider;
    private final ConnectorTableFunctionHandle functionHandle;
    private final ConnectorSession session;

    private final Deque<ConnectorSplit> pendingSplits = new ArrayDeque<>();
    private boolean noMoreSplits;

    private TableFunctionSplitProcessor processor;
    private boolean processorFinishedSplit = true;
    private ListenableFuture<Void> processorBlocked = NOT_BLOCKED;

    public LeafTableFunctionOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            CatalogHandle functionCatalog,
            TableFunctionProcessorProvider tableFunctionProvider,
            ConnectorTableFunctionHandle functionHandle)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.tableFunctionProvider = requireNonNull(tableFunctionProvider, "tableFunctionProvider is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.session = operatorContext.getSession().toConnectorSession(functionCatalog);
    }

    private void resetProcessor(ConnectorSplit nextSplit)
    {
        this.processor = tableFunctionProvider.getSplitProcessor(session, functionHandle, nextSplit);
        this.processorFinishedSplit = false;
        this.processorBlocked = NOT_BLOCKED;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " does not take input");
    }

    @Override
    public void addSplit(Split split)
    {
        checkState(!noMoreSplits, "no more splits expected");
        pendingSplits.add(split.getConnectorSplit());
    }

    @Override
    public void noMoreSplits()
    {
        noMoreSplits = true;
    }

    @Override
    public Page getOutput()
    {
        if (processorFinishedSplit) {
            // start processing a new split
            while (pendingSplits.peekFirst() instanceof EmptySplit) {
                pendingSplits.remove();
            }
            if (pendingSplits.isEmpty()) {
                // no more splits to process at the moment
                return null;
            }
            ConnectorSplit nextSplit = pendingSplits.remove();
            resetProcessor(nextSplit);
        }

        TableFunctionProcessorState state = processor.process();
        if (state == FINISHED) {
            processorFinishedSplit = true;
        }
        if (state instanceof Blocked blocked) {
            processorBlocked = toListenableFuture(blocked.getFuture());
        }
        if (state instanceof Processed processed) {
            if (processed.isUsedInput()) {
                throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Invalid state, as no input has been provided: " + state);
            }
            return processed.getResult();
        }
        return null;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return processorBlocked;
    }

    @Override
    public void finish()
    {
        // this method is redundant. the operator takes no input at all. noMoreSplits() should be called instead.
    }

    @Override
    public boolean isFinished()
    {
        return processorFinishedSplit && pendingSplits.isEmpty() && noMoreSplits;
    }

    @Override
    public void close()
            throws Exception
    {
        // TODO
    }
}
