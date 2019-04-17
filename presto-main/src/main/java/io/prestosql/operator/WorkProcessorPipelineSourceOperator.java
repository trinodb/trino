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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Split;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.operator.WorkProcessor.ProcessState.Type.FINISHED;
import static java.util.Objects.requireNonNull;

public class WorkProcessorPipelineSourceOperator
        implements SourceOperator
{
    private static final Logger log = Logger.get(WorkProcessorPipelineSourceOperator.class);

    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final WorkProcessor<Page> pages;
    // operator instances including source operator
    private final List<WorkProcessorOperatorContext> workProcessorOperatorContexts = new ArrayList<>();
    private final List<Split> pendingSplits = new ArrayList<>();

    private WorkProcessorSourceOperator sourceOperator;
    private SettableFuture<?> blockedOnSplits = SettableFuture.create();
    private boolean operatorFinishing;

    public static List<OperatorFactory> convertOperators(int operatorId, List<OperatorFactory> operatorFactories)
    {
        if (operatorFactories.isEmpty() || !(operatorFactories.get(0) instanceof WorkProcessorSourceOperatorFactory)) {
            return operatorFactories;
        }

        WorkProcessorSourceOperatorFactory sourceOperatorFactory = (WorkProcessorSourceOperatorFactory) operatorFactories.get(0);
        ImmutableList.Builder<WorkProcessorOperatorFactory> workProcessorOperatorFactoriesBuilder = ImmutableList.builder();
        int operatorIndex = 1;
        for (; operatorIndex < operatorFactories.size(); ++operatorIndex) {
            if (!(operatorFactories.get(operatorIndex) instanceof WorkProcessorOperatorFactory)) {
                break;
            }
            workProcessorOperatorFactoriesBuilder.add((WorkProcessorOperatorFactory) operatorFactories.get(operatorIndex));
        }

        List<WorkProcessorOperatorFactory> workProcessorOperatorFactories = workProcessorOperatorFactoriesBuilder.build();
        if (workProcessorOperatorFactories.isEmpty()) {
            return operatorFactories;
        }

        return ImmutableList.<OperatorFactory>builder()
                .add(new WorkProcessorPipelineSourceOperatorFactory(operatorId, sourceOperatorFactory, workProcessorOperatorFactories))
                .addAll(operatorFactories.subList(operatorIndex, operatorFactories.size()))
                .build();
    }

    private WorkProcessorPipelineSourceOperator(
            int operatorId,
            DriverContext driverContext,
            WorkProcessorSourceOperatorFactory sourceOperatorFactory,
            List<WorkProcessorOperatorFactory> operatorFactories)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(sourceOperatorFactory, "sourceOperatorFactory is null");
        requireNonNull(operatorFactories, "operatorFactories is null");
        this.sourceId = sourceOperatorFactory.getSourceId();
        // TODO: make OperatorContext aware of WorkProcessorOperators
        this.operatorContext = driverContext.addOperatorContext(operatorId, sourceId, WorkProcessorPipelineSourceOperator.class.getSimpleName());

        // TODO: measure and report WorkProcessorOperator memory usage
        MemoryTrackingContext sourceOperatorMemoryTrackingContext = createMemoryTrackingContext(operatorContext);
        WorkProcessor<Split> splits = WorkProcessor.create(new Splits());

        sourceOperator = sourceOperatorFactory.create(
                operatorContext.getSession(),
                sourceOperatorMemoryTrackingContext,
                operatorContext.getDriverContext().getYieldSignal(),
                splits);
        sourceOperatorMemoryTrackingContext.initializeLocalMemoryContexts(sourceOperator.getClass().getSimpleName());
        workProcessorOperatorContexts.add(new WorkProcessorOperatorContext(
                sourceOperator,
                sourceOperatorFactory.getOperatorId(),
                sourceOperatorMemoryTrackingContext));
        WorkProcessor<Page> pages = sourceOperator.getOutputPages();
        pages = pages.withProcessStateMonitor(state -> {
            if (state.getType() == FINISHED) {
                // immediately close source operator
                closeOperators(0);
            }
        });

        for (int i = 0; i < operatorFactories.size(); ++i) {
            MemoryTrackingContext operatorMemoryTrackingContext = createMemoryTrackingContext(operatorContext);
            WorkProcessorOperator operator = operatorFactories.get(i).create(
                    operatorContext.getSession(),
                    operatorMemoryTrackingContext,
                    operatorContext.getDriverContext().getYieldSignal(),
                    pages);
            operatorMemoryTrackingContext.initializeLocalMemoryContexts(operator.getClass().getSimpleName());
            workProcessorOperatorContexts.add(new WorkProcessorOperatorContext(
                    operator,
                    operatorFactories.get(i).getOperatorId(),
                    operatorMemoryTrackingContext));
            pages = operator.getOutputPages();
            int operatorIndex = i + 1;
            pages = pages.withProcessStateMonitor(state -> {
                if (state.getType() == FINISHED) {
                    // immediately close all upstream operators (including finished operator)
                    closeOperators(operatorIndex);
                }
            });
        }

        // materialize output pages as there are no semantics guarantees for non WorkProcessor operators
        pages = pages.map(Page::getLoadedPage);

        // finish early when entire pipeline is closed
        this.pages = pages.finishWhen(() -> operatorFinishing);
    }

    private static MemoryTrackingContext createMemoryTrackingContext(OperatorContext operatorContext)
    {
        return new MemoryTrackingContext(
                operatorContext.newAggregateUserMemoryContext(),
                operatorContext.newAggregateRevocableMemoryContext(),
                operatorContext.newAggregateSystemMemoryContext());
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        if (sourceOperator == null) {
            return Optional::empty;
        }

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
        }

        pendingSplits.add(split);
        blockedOnSplits.set(null);

        return sourceOperator.getUpdatablePageSourceSupplier();
    }

    @Override
    public void noMoreSplits()
    {
        blockedOnSplits.set(null);
        sourceOperator = null;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            return null;
        }

        return pages.getResult();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        // TODO: support spill
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishMemoryRevoke()
    {
        // TODO: support spill
        throw new UnsupportedOperationException();
    }

    @Override
    public void finish()
    {
        // operator is finished early without waiting for all pages to process
        operatorFinishing = true;
        noMoreSplits();
        closeOperators(workProcessorOperatorContexts.size() - 1);
    }

    @Override
    public boolean isFinished()
    {
        return pages.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!pages.isBlocked()) {
            return NOT_BLOCKED;
        }

        return pages.getBlockedFuture();
    }

    @Override
    public void close()
    {
        finish();
    }

    private class Splits
            implements WorkProcessor.Process<Split>
    {
        @Override
        public ProcessState<Split> process()
        {
            boolean noMoreSplits = sourceOperator == null;

            if (pendingSplits.isEmpty()) {
                if (noMoreSplits) {
                    return ProcessState.finished();
                }

                blockedOnSplits = SettableFuture.create();
                return ProcessState.blocked(blockedOnSplits);
            }

            return ProcessState.ofResult(pendingSplits.remove(0));
        }
    }

    private void closeOperators(int lastOperatorIndex)
    {
        // record the current interrupted status (and clear the flag); we'll reset it later
        boolean wasInterrupted = Thread.interrupted();
        Throwable inFlightException = null;
        try {
            for (int i = 0; i <= lastOperatorIndex; ++i) {
                WorkProcessorOperatorContext workProcessorOperatorContext = workProcessorOperatorContexts.get(i);
                if (workProcessorOperatorContext == null) {
                    continue;
                }

                try {
                    workProcessorOperatorContext.operator.close();
                }
                catch (InterruptedException t) {
                    // don't record the stack
                    wasInterrupted = true;
                }
                catch (Throwable t) {
                    inFlightException = handleOperatorCloseError(
                            inFlightException,
                            t,
                            "Error closing WorkProcessor operator %s for task %s",
                            workProcessorOperatorContext.operatorId,
                            operatorContext.getDriverContext().getTaskId());
                }
                finally {
                    workProcessorOperatorContext.memoryTrackingContext.close();
                    workProcessorOperatorContexts.set(i, null);
                }
            }
        }
        finally {
            // reset the interrupted flag
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (inFlightException != null) {
            throwIfUnchecked(inFlightException);
            throw new RuntimeException(inFlightException);
        }
    }

    private static Throwable handleOperatorCloseError(Throwable inFlightException, Throwable newException, String message, Object... args)
    {
        if (newException instanceof Error) {
            if (inFlightException == null) {
                inFlightException = newException;
            }
            else {
                // Self-suppression not permitted
                if (inFlightException != newException) {
                    inFlightException.addSuppressed(newException);
                }
            }
        }
        else {
            // log normal exceptions instead of rethrowing them
            log.error(newException, message, args);
        }
        return inFlightException;
    }

    private class WorkProcessorOperatorContext
    {
        final WorkProcessorOperator operator;
        final int operatorId;
        final MemoryTrackingContext memoryTrackingContext;

        private WorkProcessorOperatorContext(
                WorkProcessorOperator operator,
                int operatorId,
                MemoryTrackingContext memoryTrackingContext)
        {
            this.operator = operator;
            this.operatorId = operatorId;
            this.memoryTrackingContext = memoryTrackingContext;
        }
    }

    public static class WorkProcessorPipelineSourceOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final WorkProcessorSourceOperatorFactory sourceOperatorFactory;
        private final List<WorkProcessorOperatorFactory> operatorFactories;
        private boolean closed;

        private WorkProcessorPipelineSourceOperatorFactory(
                int operatorId,
                WorkProcessorSourceOperatorFactory sourceOperatorFactory,
                List<WorkProcessorOperatorFactory> operatorFactories)
        {
            this.operatorId = operatorId;
            this.sourceOperatorFactory = requireNonNull(sourceOperatorFactory, "sourceOperatorFactory is null");
            this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceOperatorFactory.getSourceId();
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return new WorkProcessorPipelineSourceOperator(operatorId, driverContext, sourceOperatorFactory, operatorFactories);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
