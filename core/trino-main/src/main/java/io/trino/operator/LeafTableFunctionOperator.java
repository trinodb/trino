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
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.TableFunctionProcessorProvider;
import io.trino.spi.ptf.TableFunctionProcessorState;
import io.trino.spi.ptf.TableFunctionProcessorState.Blocked;
import io.trino.spi.ptf.TableFunctionProcessorState.Processed;
import io.trino.spi.ptf.TableFunctionSplitProcessor;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.spi.ptf.TableFunctionProcessorState.Finished.FINISHED;
import static java.util.Objects.requireNonNull;

public class LeafTableFunctionOperator
        implements SourceOperator
{
    public static class LeafTableFunctionOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final TableFunctionProcessorProvider tableFunctionProvider;
        private final ConnectorTableFunctionHandle functionHandle;
        private boolean closed;

        public LeafTableFunctionOperatorFactory(int operatorId, PlanNodeId sourceId, TableFunctionProcessorProvider tableFunctionProvider, ConnectorTableFunctionHandle functionHandle)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
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
            return new LeafTableFunctionOperator(operatorContext, sourceId, tableFunctionProvider, functionHandle);
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

    private ConnectorSplit currentSplit;
    private final List<ConnectorSplit> pendingSplits = new ArrayList<>();
    private boolean noMoreSplits;

    private TableFunctionSplitProcessor processor;
    private boolean processorUsedData;
    private boolean processorFinishedSplit = true;
    private ListenableFuture<Void> processorBlocked = NOT_BLOCKED;

    public LeafTableFunctionOperator(OperatorContext operatorContext, PlanNodeId sourceId, TableFunctionProcessorProvider tableFunctionProvider, ConnectorTableFunctionHandle functionHandle)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.tableFunctionProvider = requireNonNull(tableFunctionProvider, "tableFunctionProvider is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
    }

    private void resetProcessor()
    {
        this.processor = tableFunctionProvider.getSplitProcessor(functionHandle);
        this.processorUsedData = false;
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
            if (pendingSplits.isEmpty()) {
                // no more splits to process at the moment
                return null;
            }
            currentSplit = pendingSplits.remove(0);
            resetProcessor();
        }
        else {
            // a split is being processed
            requireNonNull(currentSplit, "currentSplit is null");
        }

        TableFunctionProcessorState state = processor.process(processorUsedData ? null : currentSplit);
        if (state == FINISHED) {
            processorFinishedSplit = true;
        }
        if (state instanceof Blocked blocked) {
            processorBlocked = toListenableFuture(blocked.getFuture());
        }
        if (state instanceof Processed processed) {
            if (processed.isUsedInput()) {
                processorUsedData = true;
            }
            if (processed.getResult() != null) {
                return processed.getResult();
            }
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
