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

import io.trino.execution.TaskId;
import io.trino.operator.BasicWorkProcessorOperatorAdapter.BasicAdapterWorkProcessorOperatorFactory;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.BasicWorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class AssignUniqueIdOperator
        implements WorkProcessorOperator
{
    private static final long ROW_IDS_PER_REQUEST = 1L << 20L;
    private static final long MAX_ROW_ID = 1L << 40L;

    public static OperatorFactory createOperatorFactory(int operatorId, PlanNodeId planNodeId)
    {
        return createAdapterOperatorFactory(new Factory(operatorId, planNodeId));
    }

    private static class Factory
            implements BasicAdapterWorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;
        private final AtomicLong valuePool = new AtomicLong();

        private Factory(int operatorId, PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new AssignUniqueIdOperator(processorContext, sourcePages, valuePool);
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
            return AssignUniqueIdOperator.class.getSimpleName();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(operatorId, planNodeId);
        }
    }

    private final WorkProcessor<Page> pages;

    private AssignUniqueIdOperator(ProcessorContext context, WorkProcessor<Page> sourcePages, AtomicLong rowIdPool)
    {
        pages = sourcePages
                .transform(new AssignUniqueId(
                        context.getTaskId(),
                        rowIdPool));
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    private static class AssignUniqueId
            implements WorkProcessor.Transformation<Page, Page>
    {
        private final AtomicLong rowIdPool;
        private final long uniqueValueMask;

        private long rowIdCounter;
        private long maxRowIdCounterValue;

        private AssignUniqueId(TaskId taskId, AtomicLong rowIdPool)
        {
            this.rowIdPool = requireNonNull(rowIdPool, "rowIdPool is null");

            uniqueValueMask = (((long) taskId.getStageId().getId()) << 54) | (((long) taskId.getPartitionId()) << 40);
            requestValues();
        }

        @Override
        public TransformationState<Page> process(@Nullable Page inputPage)
        {
            if (inputPage == null) {
                return finished();
            }

            return ofResult(inputPage.appendColumn(generateIdColumn(inputPage.getPositionCount())));
        }

        private Block generateIdColumn(int positionCount)
        {
            BlockBuilder block = BIGINT.createFixedSizeBlockBuilder(positionCount);
            for (int currentPosition = 0; currentPosition < positionCount; currentPosition++) {
                if (rowIdCounter >= maxRowIdCounterValue) {
                    requestValues();
                }
                long rowId = rowIdCounter++;
                verify((rowId & uniqueValueMask) == 0, "RowId and uniqueValue mask overlaps");
                BIGINT.writeLong(block, uniqueValueMask | rowId);
            }
            return block.build();
        }

        private void requestValues()
        {
            rowIdCounter = rowIdPool.getAndAdd(ROW_IDS_PER_REQUEST);
            maxRowIdCounterValue = Math.min(rowIdCounter + ROW_IDS_PER_REQUEST, MAX_ROW_ID);
            checkState(rowIdCounter < MAX_ROW_ID, "Unique row id exceeds a limit: %s", MAX_ROW_ID);
        }
    }
}
