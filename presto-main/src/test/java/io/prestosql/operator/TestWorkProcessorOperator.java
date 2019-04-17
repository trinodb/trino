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

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TestWorkProcessorOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final WorkProcessor<Page> pages;

    private boolean noMoreInput;
    private Page inputPage;

    private TestWorkProcessorOperator(
            OperatorContext operatorContext,
            WorkProcessorOperatorFactory workProcessorOperatorFactory)
    {
        this.operatorContext = operatorContext;
        this.pages = workProcessorOperatorFactory.create(
                operatorContext.getSession(),
                operatorContext.getOperatorMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal(),
                WorkProcessor.create(new Pages()))
                .getOutputPages();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !noMoreInput && !isFinished() && inputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        inputPage = page;
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
    public void finish()
    {
        noMoreInput = true;
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
    public boolean isFinished()
    {
        return pages.isFinished();
    }

    private class Pages
            implements WorkProcessor.Process<Page>
    {
        @Override
        public ProcessState<Page> process()
        {
            if (noMoreInput) {
                return ProcessState.finished();
            }

            if (inputPage == null) {
                return ProcessState.yield();
            }

            Page page = inputPage;
            inputPage = null;
            return ProcessState.ofResult(page);
        }
    }

    static class TestWorkProcessorOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final WorkProcessorOperatorFactory workProcessorOperatorFactory;

        private boolean closed;

        TestWorkProcessorOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                WorkProcessorOperatorFactory workProcessorOperatorFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.workProcessorOperatorFactory = requireNonNull(workProcessorOperatorFactory, "workProcessorOperatorFactory is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TestWorkProcessorOperator.class.getSimpleName());
            return new TestWorkProcessorOperator(operatorContext, workProcessorOperatorFactory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException();
        }
    }
}
