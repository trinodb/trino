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
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Objects.requireNonNull;

public class RefreshMaterializedViewOperator
        implements Operator
{
    public static class RefreshMaterializedViewOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Metadata metadata;
        private final QualifiedObjectName viewName;
        private boolean closed;

        public RefreshMaterializedViewOperatorFactory(int operatorId, PlanNodeId planNodeId, Metadata metadata, QualifiedObjectName viewName)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.viewName = requireNonNull(viewName, "viewName is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ValuesOperator.class.getSimpleName());
            return new RefreshMaterializedViewOperator(operatorContext, metadata, viewName);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new RefreshMaterializedViewOperatorFactory(operatorId, planNodeId, metadata, viewName);
        }
    }

    private final OperatorContext operatorContext;
    private final Metadata metadata;
    private final QualifiedObjectName viewName;
    @Nullable
    private ListenableFuture<Void> refreshFuture;
    private boolean closed;

    public RefreshMaterializedViewOperator(OperatorContext operatorContext, Metadata metadata, QualifiedObjectName viewName)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.viewName = requireNonNull(viewName, "viewName is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        if (refreshFuture == null) {
            return NOT_BLOCKED;
        }

        return refreshFuture;
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
        return null;
    }

    @Override
    public void finish()
    {
    }

    @Override
    public void close()
    {
        if (refreshFuture != null) {
            refreshFuture.cancel(false);
        }

        closed = true;
    }

    @Override
    public boolean isFinished()
    {
        if (closed) {
            return true;
        }

        if (refreshFuture == null) {
            // perform refresh in isFinished method. getOutput method won't be called by Driver
            // since RefreshMaterializedViewOperator is last operator in pipeline
            refreshFuture = metadata.refreshMaterializedView(operatorContext.getSession(), viewName);
        }

        if (refreshFuture.isDone()) {
            getDone(refreshFuture);
        }

        return refreshFuture.isDone();
    }
}
