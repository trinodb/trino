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

import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.BasicWorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static java.util.Objects.requireNonNull;

/**
 * This operator is used by operations like SQL MERGE and UPDATE to support connectors
 * that represent a modification of a row as a DELETE plus an INSERT, and support a
 * partition and/or bucket paradigm.  NOTE: Not all
 * {@link io.trino.spi.connector.RowChangeParadigm}s require
 * separation of UPDATEs in to DELETEs and INSERTs.  That is determined by the
 * {@link RowChangeProcessor}
 */
public class DeleteAndInsertOperator
        implements WorkProcessorOperator
{
    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            RowChangeProcessor rowChangeProcessor)
    {
        return createAdapterOperatorFactory(new Factory(operatorId, planNodeId, rowChangeProcessor));
    }

    public static class Factory
            implements BasicWorkProcessorOperatorAdapter.BasicAdapterWorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final RowChangeProcessor rowChangeProcessor;
        private boolean closed;

        public Factory(int operatorId, PlanNodeId planNodeId, RowChangeProcessor rowChangeProcessor)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.rowChangeProcessor = requireNonNull(rowChangeProcessor, "rowChangeProcessor is null");
        }

        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new DeleteAndInsertOperator(sourcePages, rowChangeProcessor);
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        @Override
        public String getOperatorType()
        {
            return DeleteAndInsertOperator.class.getSimpleName();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(operatorId, planNodeId, rowChangeProcessor);
        }
    }

    private final WorkProcessor<Page> pages;

    private DeleteAndInsertOperator(
            WorkProcessor<Page> sourcePages,
            RowChangeProcessor rowChangeProcessor)
    {
        pages = sourcePages
                .transform(page -> {
                    if (page == null) {
                        return finished();
                    }
                    return ofResult(rowChangeProcessor.transformPage(page));
                });
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }
}
