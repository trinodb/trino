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
import io.prestosql.Session;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.spi.Page;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.operator.WorkProcessor.ProcessState.finished;
import static io.prestosql.operator.WorkProcessor.ProcessState.ofResult;
import static io.prestosql.operator.WorkProcessor.ProcessState.yield;
import static java.util.Objects.requireNonNull;

public class WorkProcessorOperatorAdapter
        implements Operator
{
    private final OperatorContext operatorContext;
    private final WorkProcessorOperator workProcessorOperator;
    private final WorkProcessor<Page> pages;
    private final PageBuffer pageBuffer = new PageBuffer();
    private final LocalMemoryContext memoryContext;

    public interface AdapterWorkProcessorOperatorFactory
            extends WorkProcessorOperatorFactory
    {
        WorkProcessorOperator create(
                Session session,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                PageBuffer sourcePageBuffer);
    }

    public WorkProcessorOperatorAdapter(OperatorContext operatorContext, AdapterWorkProcessorOperatorFactory workProcessorOperatorFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.workProcessorOperator = requireNonNull(workProcessorOperatorFactory, "workProcessorOperatorFactory is null")
                .create(
                        operatorContext.getSession(),
                        new MemoryTrackingContext(
                                operatorContext.aggregateUserMemoryContext(),
                                operatorContext.aggregateRevocableMemoryContext(),
                                operatorContext.aggregateSystemMemoryContext()),
                        operatorContext.getDriverContext().getYieldSignal(),
                        pageBuffer);
        this.pages = workProcessorOperator.getOutputPages();
        this.memoryContext = operatorContext.localUserMemoryContext();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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
    public boolean needsInput()
    {
        return !isFinished() && !pageBuffer.isFinished() && pageBuffer.isEmpty();
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
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
        pageBuffer.finish();
    }

    @Override
    public boolean isFinished()
    {
        return pages.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        workProcessorOperator.close();
    }

    public class PageBuffer
    {
        @Nullable
        private Page page;
        private boolean finished;
        @Nullable
        private Runnable addPageListener;

        public WorkProcessor<Page> pages()
        {
            return WorkProcessor.create(() -> {
                if (!isEmpty()) {
                    return ofResult(poll());
                }

                if (isFinished()) {
                    return finished();
                }

                return yield();
            });
        }

        @Nullable
        public Page poll()
        {
            Page page = this.page;
            this.page = null;
            memoryContext.setBytes(0);
            return page;
        }

        public boolean isEmpty()
        {
            return page == null;
        }

        public boolean isFinished()
        {
            return finished;
        }

        public void setAddPageListener(Runnable stateChangeListener)
        {
            this.addPageListener = requireNonNull(stateChangeListener, "stateChangeListener is null");
        }

        private void add(Page page)
        {
            checkState(isEmpty());
            this.page = requireNonNull(page, "page is null");

            if (addPageListener != null) {
                addPageListener.run();
            }

            // if page was not immediately consumed, account it's memory
            if (!isEmpty()) {
                memoryContext.setBytes(page.getSizeInBytes());
            }
        }

        private void finish()
        {
            finished = true;
        }
    }
}
