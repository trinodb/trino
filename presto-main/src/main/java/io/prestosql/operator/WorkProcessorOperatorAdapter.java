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
import io.prestosql.execution.Lifespan;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.spi.Page;

import static java.util.Objects.requireNonNull;

public class WorkProcessorOperatorAdapter
        implements Operator
{
    private final OperatorContext operatorContext;
    private final AdapterWorkProcessorOperator workProcessorOperator;
    private final WorkProcessor<Page> pages;

    public interface AdapterWorkProcessorOperator
            extends WorkProcessorOperator
    {
        boolean needsInput();

        void addInput(Page page);

        void finish();
    }

    public interface AdapterWorkProcessorOperatorFactory
            extends WorkProcessorOperatorFactory
    {
        AdapterWorkProcessorOperator create(ProcessorContext processorContext);
    }

    public WorkProcessorOperatorAdapter(OperatorContext operatorContext, AdapterWorkProcessorOperatorFactory workProcessorOperatorFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        MemoryTrackingContext memoryTrackingContext = new MemoryTrackingContext(
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.aggregateRevocableMemoryContext(),
                operatorContext.aggregateSystemMemoryContext());
        memoryTrackingContext.initializeLocalMemoryContexts(workProcessorOperatorFactory.getOperatorType());
        this.workProcessorOperator = requireNonNull(workProcessorOperatorFactory, "workProcessorOperatorFactory is null")
                .create(new ProcessorContext(operatorContext.getSession(), memoryTrackingContext, operatorContext));
        this.pages = workProcessorOperator.getOutputPages();
        operatorContext.setInfoSupplier(() -> workProcessorOperator.getOperatorInfo().orElse(null));
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
        return !isFinished() && workProcessorOperator.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        workProcessorOperator.addInput(page);
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
        workProcessorOperator.finish();
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

    public static class ProcessorContext
    {
        private final Session session;
        private final MemoryTrackingContext memoryTrackingContext;
        private final DriverYieldSignal driverYieldSignal;
        private final Lifespan lifespan;
        private final SpillContext spillContext;

        public ProcessorContext(Session session, MemoryTrackingContext memoryTrackingContext, OperatorContext operatorContext)
        {
            this.session = requireNonNull(session, "session is null");
            this.memoryTrackingContext = requireNonNull(memoryTrackingContext, "memoryTrackingContext is null");
            requireNonNull(operatorContext, "operatorContext is null");
            this.driverYieldSignal = operatorContext.getDriverContext().getYieldSignal();
            this.lifespan = operatorContext.getDriverContext().getLifespan();
            this.spillContext = operatorContext.getSpillContext();
        }

        public Session getSession()
        {
            return session;
        }

        public MemoryTrackingContext getMemoryTrackingContext()
        {
            return memoryTrackingContext;
        }

        public DriverYieldSignal getDriverYieldSignal()
        {
            return driverYieldSignal;
        }

        public Lifespan getLifespan()
        {
            return lifespan;
        }

        public SpillContext getSpillContext()
        {
            return spillContext;
        }
    }
}
