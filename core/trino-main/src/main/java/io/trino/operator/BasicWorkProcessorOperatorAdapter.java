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

import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This {@link WorkProcessorOperator} adapter allows to adapt {@link WorkProcessor} operators
 * that do not require special input handling (e.g. streaming operators).
 */
public class BasicWorkProcessorOperatorAdapter
        implements AdapterWorkProcessorOperator
{
    public interface BasicAdapterWorkProcessorOperatorFactory
            extends WorkProcessorOperatorFactory
    {
        default WorkProcessorOperator createAdapterOperator(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            return create(processorContext, sourcePages);
        }

        BasicAdapterWorkProcessorOperatorFactory duplicate();
    }

    public static OperatorFactory createAdapterOperatorFactory(BasicAdapterWorkProcessorOperatorFactory operatorFactory)
    {
        return WorkProcessorOperatorAdapter.createAdapterOperatorFactory(new Factory(operatorFactory));
    }

    private static class Factory
            implements AdapterWorkProcessorOperatorFactory
    {
        private final BasicAdapterWorkProcessorOperatorFactory operatorFactory;

        Factory(BasicAdapterWorkProcessorOperatorFactory operatorFactory)
        {
            this.operatorFactory = requireNonNull(operatorFactory, "operatorFactory is null");
        }

        @Override
        public AdapterWorkProcessorOperatorFactory duplicate()
        {
            return new Factory(operatorFactory.duplicate());
        }

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
        public AdapterWorkProcessorOperator createAdapterOperator(ProcessorContext processorContext)
        {
            return new BasicWorkProcessorOperatorAdapter(processorContext, operatorFactory);
        }

        @Override
        public void close()
        {
            operatorFactory.close();
        }
    }

    private final PageBuffer pageBuffer;
    private final WorkProcessorOperator operator;

    private BasicWorkProcessorOperatorAdapter(
            ProcessorContext processorContext,
            BasicAdapterWorkProcessorOperatorFactory operatorFactory)
    {
        this.pageBuffer = new PageBuffer();
        this.operator = operatorFactory.createAdapterOperator(processorContext, pageBuffer.pages());
    }

    @Override
    public void finish()
    {
        pageBuffer.finish();
    }

    @Override
    public boolean needsInput()
    {
        return pageBuffer.isEmpty() && !pageBuffer.isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return operator.getOutputPages();
    }

    @Override
    public Optional<OperatorInfo> getOperatorInfo()
    {
        return operator.getOperatorInfo();
    }

    @Override
    public void close()
            throws Exception
    {
        operator.close();
    }

    @Override
    public Metrics getMetrics()
    {
        return operator.getMetrics();
    }
}
