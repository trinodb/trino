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
import io.trino.execution.Lifespan;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import static java.util.Objects.requireNonNull;

/**
 * This {@link WorkProcessorOperator} adapter allows to adapt {@link WorkProcessor} operators
 * that require customization of input handling (e.g aggregation operators that want to skip extra
 * buffering step or operators that require more sophisticated initial blocking condition).
 * If such customization is not required, it's recommended to use {@link BasicWorkProcessorOperatorAdapter}
 * instead.
 */
public class WorkProcessorOperatorAdapter
        implements Operator
{
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
        AdapterWorkProcessorOperator createAdapterOperator(ProcessorContext processorContext);

        AdapterWorkProcessorOperatorFactory duplicate();
    }

    public static OperatorFactory createAdapterOperatorFactory(AdapterWorkProcessorOperatorFactory operatorFactory)
    {
        return new Factory(operatorFactory);
    }

    /**
     * Provides dual {@link OperatorFactory} and {@link WorkProcessorOperatorFactory} interface.
     */
    private static class Factory
            implements OperatorFactory, WorkProcessorOperatorFactory
    {
        final AdapterWorkProcessorOperatorFactory operatorFactory;

        Factory(AdapterWorkProcessorOperatorFactory operatorFactory)
        {
            this.operatorFactory = requireNonNull(operatorFactory, "operatorFactory is null");
        }

        // Methods from OperatorFactory

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(
                    operatorFactory.getOperatorId(),
                    operatorFactory.getPlanNodeId(),
                    operatorFactory.getOperatorType());
            return new WorkProcessorOperatorAdapter(operatorContext, operatorFactory);
        }

        @Override
        public void noMoreOperators()
        {
            close();
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            lifespanFinished(lifespan);
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new Factory(operatorFactory.duplicate());
        }

        // Methods from WorkProcessorOperatorFactory

        @Override
        public int getOperatorId()
        {
            return operatorFactory.getOperatorId();
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return operatorFactory.getPlanNodeId();
        }

        @Override
        public String getOperatorType()
        {
            return operatorFactory.getOperatorType();
        }

        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            return operatorFactory.create(processorContext, sourcePages);
        }

        @Override
        public void lifespanFinished(Lifespan lifespan)
        {
            operatorFactory.lifespanFinished(lifespan);
        }

        @Override
        public void close()
        {
            operatorFactory.close();
        }
    }

    private final OperatorContext operatorContext;
    private final AdapterWorkProcessorOperator workProcessorOperator;
    private final WorkProcessor<Page> pages;

    public WorkProcessorOperatorAdapter(OperatorContext operatorContext, AdapterWorkProcessorOperatorFactory workProcessorOperatorFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        MemoryTrackingContext memoryTrackingContext = new MemoryTrackingContext(
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.aggregateRevocableMemoryContext(),
                operatorContext.aggregateSystemMemoryContext());
        memoryTrackingContext.initializeLocalMemoryContexts(workProcessorOperatorFactory.getOperatorType());
        this.workProcessorOperator = requireNonNull(workProcessorOperatorFactory, "workProcessorOperatorFactory is null")
                .createAdapterOperator(new ProcessorContext(operatorContext.getSession(), memoryTrackingContext, operatorContext));
        this.pages = workProcessorOperator.getOutputPages();
        operatorContext.setInfoSupplier(() -> workProcessorOperator.getOperatorInfo().orElse(null));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
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
            updateOperatorMetrics();
            return null;
        }

        if (pages.isFinished()) {
            updateOperatorMetrics();
            return null;
        }

        updateOperatorMetrics();
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

    private void updateOperatorMetrics()
    {
        operatorContext.setLatestMetrics(workProcessorOperator.getMetrics());
    }
}
