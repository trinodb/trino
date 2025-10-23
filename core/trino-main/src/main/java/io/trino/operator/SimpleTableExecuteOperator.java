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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableExecuteHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SimpleTableExecuteOperator
        implements Operator
{
    public static class SimpleTableExecuteOperatorOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Metadata metadata;
        private final Session session;
        private final TableExecuteHandle executeHandle;
        private final List<Type> types;
        private boolean closed;

        public SimpleTableExecuteOperatorOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Metadata metadata,
                Session session,
                TableExecuteHandle executeHandle,
                List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.metadata = requireNonNull(metadata, "planNodeId is null");
            this.session = requireNonNull(session, "planNodeId is null");
            this.executeHandle = requireNonNull(executeHandle, "executeHandle is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, SimpleTableExecuteOperator.class.getSimpleName());
            return new SimpleTableExecuteOperator(
                    context,
                    metadata,
                    session,
                    executeHandle,
                    types);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SimpleTableExecuteOperatorOperatorFactory(
                    operatorId,
                    planNodeId,
                    metadata,
                    session,
                    executeHandle,
                    types);
        }
    }

    private final OperatorContext operatorContext;
    private final Metadata metadata;
    private final Session session;
    private final TableExecuteHandle executeHandle;
    private final List<Type> types;

    private boolean finished;

    public SimpleTableExecuteOperator(
            OperatorContext operatorContext,
            Metadata metadata,
            Session session,
            TableExecuteHandle executeHandle,
            List<Type> types)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.executeHandle = requireNonNull(executeHandle, "executeHandle is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
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
        if (finished) {
            return null;
        }

        Map<String, Long> metrics = metadata.executeTableExecute(session, executeHandle);
        finished = true;
        PageBuilder pageBuilder = new PageBuilder(types);
        BlockBuilder metricNameBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder metricValueBuilder = pageBuilder.getBlockBuilder(1);
        for (Map.Entry<String, Long> entry : metrics.entrySet()) {
            types.get(0).writeSlice(metricNameBuilder, Slices.utf8Slice(entry.getKey()));
            types.get(1).writeLong(metricValueBuilder, entry.getValue());
            pageBuilder.declarePosition();
        }
        return pageBuilder.build();
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished()
    {
        return finished;
    }
}
