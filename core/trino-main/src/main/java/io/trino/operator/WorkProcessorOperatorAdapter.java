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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.join.JoinOperatorFactory;
import io.trino.operator.join.LookupJoinOperatorFactory;
import io.trino.spi.Page;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * This {@link WorkProcessorOperator} adapter allows to adapt {@link WorkProcessor} operators
 * as {@link Operator} instances.
 */
public class WorkProcessorOperatorAdapter
        implements Operator
{
    public static OperatorFactory createAdapterOperatorFactory(WorkProcessorOperatorFactory operatorFactory)
    {
        return new Factory(operatorFactory);
    }

    /**
     * Provides {@link OperatorFactory} implementation for {@link WorkProcessorSourceOperator}.
     * This class implements {@link JoinOperatorFactory} interface because it's required to
     * propagate {@link LookupJoinOperatorFactory} implementation of {@link JoinOperatorFactory#createOuterOperatorFactory()}.
     * For non-join operators {@link Factory#createOuterOperatorFactory()} returns {@link Optional#empty()}.
     */
    @VisibleForTesting
    public static class Factory
            implements OperatorFactory, JoinOperatorFactory
    {
        private final WorkProcessorOperatorFactory operatorFactory;

        Factory(WorkProcessorOperatorFactory operatorFactory)
        {
            this.operatorFactory = requireNonNull(operatorFactory, "operatorFactory is null");
        }

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
            operatorFactory.close();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new Factory(operatorFactory.duplicate());
        }

        @Override
        public Optional<OperatorFactory> createOuterOperatorFactory()
        {
            if (!(operatorFactory instanceof JoinOperatorFactory lookupJoin)) {
                return Optional.empty();
            }

            return lookupJoin.createOuterOperatorFactory();
        }

        @VisibleForTesting
        public WorkProcessorOperatorFactory getWorkProcessorOperatorFactory()
        {
            return operatorFactory;
        }
    }

    private final OperatorContext operatorContext;
    private final WorkProcessor<Page> pages;
    private final PageBuffer pageBuffer = new PageBuffer();
    private final WorkProcessorOperator workProcessorOperator;

    public WorkProcessorOperatorAdapter(OperatorContext operatorContext, WorkProcessorOperatorFactory workProcessorOperatorFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        MemoryTrackingContext memoryTrackingContext = new MemoryTrackingContext(
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.aggregateRevocableMemoryContext());
        memoryTrackingContext.initializeLocalMemoryContexts(workProcessorOperatorFactory.getOperatorType());
        this.workProcessorOperator = workProcessorOperatorFactory.create(new ProcessorContext(operatorContext.getSession(), memoryTrackingContext, operatorContext), pageBuffer.pages());
        this.pages = workProcessorOperator.getOutputPages();
        operatorContext.setInfoSupplier(createInfoSupplier(workProcessorOperator));
    }

    // static method to avoid capturing a reference to "this"
    private static Supplier<OperatorInfo> createInfoSupplier(WorkProcessorOperator workProcessorOperator)
    {
        return () -> workProcessorOperator.getOperatorInfo().orElse(null);
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
        return !pages.isBlocked() && !pages.isFinished() && pageBuffer.isEmpty() && !pageBuffer.isFinished();
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

    private void updateOperatorMetrics()
    {
        operatorContext.setLatestMetrics(workProcessorOperator.getMetrics());
    }
}
