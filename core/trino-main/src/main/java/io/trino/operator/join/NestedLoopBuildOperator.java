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
package io.trino.operator.join;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.operator.RuntimeConstraintWiringContext;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NestedLoopBuildOperator
        implements Operator
{
    public static class NestedLoopBuildOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager;
        @Nullable
        private final NestedLoopRuntimeConstraintSource runtimeConstraintSource;

        private boolean closed;

        public NestedLoopBuildOperatorFactory(int operatorId, PlanNodeId planNodeId, JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.nestedLoopJoinBridgeManager = requireNonNull(nestedLoopJoinBridgeManager, "nestedLoopJoinBridgeManager is null");
            this.runtimeConstraintSource = null;
        }

        public NestedLoopBuildOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager,
                NestedLoopRuntimeConstraintSource runtimeConstraintSource)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.nestedLoopJoinBridgeManager = requireNonNull(nestedLoopJoinBridgeManager, "nestedLoopJoinBridgeManager is null");
            this.runtimeConstraintSource = requireNonNull(runtimeConstraintSource, "runtimeConstraintSource is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopBuildOperator.class.getSimpleName());
            return new NestedLoopBuildOperator(
                    operatorContext,
                    nestedLoopJoinBridgeManager.getJoinBridge(),
                    runtimeConstraintSource == null ? null : runtimeConstraintSource.createCollector(driverContext, operatorContext));
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (runtimeConstraintSource != null) {
                runtimeConstraintSource.noMoreOperators();
            }
        }

        @Override
        public void registerRuntimeConstraintInput(Consumer<List<RuntimeConstraintRequest>> requests, RuntimeConstraintWiringContext context)
        {
            if (runtimeConstraintSource != null) {
                runtimeConstraintSource.registerBuildInput(requests, context);
            }
        }

        @Override
        public void propagateRuntimeConstraint(
                RuntimeConstraintRequest request,
                Consumer<RuntimeConstraintRequest> input,
                RuntimeConstraintWiringContext context)
        {
            if (runtimeConstraintSource == null || !request.isCollection() || !runtimeConstraintSource.isBuildChannel(request.channel())) {
                context.stop(this, request);
                return;
            }
            input.accept(request);
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new NestedLoopBuildOperatorFactory(operatorId, planNodeId, nestedLoopJoinBridgeManager, runtimeConstraintSource);
        }
    }

    private final OperatorContext operatorContext;
    private final NestedLoopJoinBridge nestedLoopJoinBridge;
    private final NestedLoopJoinPagesBuilder nestedLoopJoinPagesBuilder;
    private final LocalMemoryContext localUserMemoryContext;
    private final Operator runtimeConstraintCollector;

    // Initially, probeDoneWithPages is not present.
    // Once finish is called, probeDoneWithPages will be set to a future that completes when the pages are no longer needed by the probe side.
    // When the pages are no longer needed, the isFinished method on this operator will return true.
    private Optional<ListenableFuture<Void>> probeDoneWithPages = Optional.empty();

    public NestedLoopBuildOperator(OperatorContext operatorContext, NestedLoopJoinBridge nestedLoopJoinBridge, Operator runtimeConstraintCollector)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nestedLoopJoinBridge = requireNonNull(nestedLoopJoinBridge, "nestedLoopJoinBridge is null");
        this.nestedLoopJoinPagesBuilder = new NestedLoopJoinPagesBuilder(operatorContext);
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.runtimeConstraintCollector = runtimeConstraintCollector;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (probeDoneWithPages.isPresent()) {
            return;
        }
        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.finish();
        }

        // nestedLoopJoinPagesBuilder and the built NestedLoopJoinPages will mostly share the same objects.
        // Extra allocation is minimal during build call. As a result, memory accounting is not updated here.
        probeDoneWithPages = Optional.of(nestedLoopJoinBridge.setPages(nestedLoopJoinPagesBuilder.build()));
    }

    @Override
    public boolean isFinished()
    {
        return probeDoneWithPages.map(Future::isDone).orElse(false);
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return probeDoneWithPages.orElse(NOT_BLOCKED);
    }

    @Override
    public boolean needsInput()
    {
        return probeDoneWithPages.isEmpty();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        if (page.getPositionCount() == 0) {
            return;
        }

        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.addInput(page);
            runtimeConstraintCollector.getOutput();
        }

        nestedLoopJoinPagesBuilder.addPage(page);
        if (!localUserMemoryContext.trySetBytes(nestedLoopJoinPagesBuilder.getEstimatedSize().toBytes())) {
            nestedLoopJoinPagesBuilder.compact();
            localUserMemoryContext.setBytes(nestedLoopJoinPagesBuilder.getEstimatedSize().toBytes());
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
            throws Exception
    {
        if (runtimeConstraintCollector != null) {
            runtimeConstraintCollector.close();
        }
    }
}
