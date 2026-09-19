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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DriverFactory
{
    private final int pipelineId;
    private final boolean inputDriver;
    private final boolean outputDriver;
    private final Optional<PlanNodeId> sourceId;
    private final OptionalInt driverInstances;

    // must synchronize between createDriver() and noMoreDrivers(), but isNoMoreDrivers() is safe without synchronizing
    @GuardedBy("this")
    private volatile boolean noMoreDrivers;
    private volatile List<OperatorFactory> operatorFactories;
    private final List<OperatorFactory> runtimeConstraintOperatorFactories;

    public DriverFactory(int pipelineId, boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances)
    {
        this.pipelineId = pipelineId;
        this.inputDriver = inputDriver;
        this.outputDriver = outputDriver;
        this.operatorFactories = ImmutableList.copyOf(requireNonNull(operatorFactories, "operatorFactories is null"));
        this.runtimeConstraintOperatorFactories = this.operatorFactories;
        checkArgument(!operatorFactories.isEmpty(), "There must be at least one operator");
        this.driverInstances = requireNonNull(driverInstances, "driverInstances is null");

        List<PlanNodeId> sourceIds = operatorFactories.stream()
                .filter(SourceOperatorFactory.class::isInstance)
                .map(SourceOperatorFactory.class::cast)
                .map(SourceOperatorFactory::getSourceId)
                .collect(toImmutableList());
        checkArgument(sourceIds.size() <= 1, "Expected at most one source operator in driver factory, but found %s", sourceIds);
        this.sourceId = sourceIds.isEmpty() ? Optional.empty() : Optional.of(sourceIds.get(0));
    }

    public int getPipelineId()
    {
        return pipelineId;
    }

    public boolean isInputDriver()
    {
        return inputDriver;
    }

    public boolean isOutputDriver()
    {
        return outputDriver;
    }

    /**
     * return the sourceId of this DriverFactory.
     * A DriverFactory doesn't always have source node.
     * For example, ValuesNode is not a source node.
     */
    public Optional<PlanNodeId> getSourceId()
    {
        return sourceId;
    }

    public OptionalInt getDriverInstances()
    {
        return driverInstances;
    }

    public void initializeRuntimeConstraints(RuntimeConstraintWiringContext context)
    {
        requireNonNull(context, "context is null");
        runtimeConstraintOperatorFactories.getLast().registerRuntimeConstraintInput(requests -> propagateRuntimeConstraints(requests, context), context);
        if (!context.isEnabled()) {
            runtimeConstraintOperatorFactories.stream()
                    .filter(SourceOperatorFactory.class::isInstance)
                    .forEach(operatorFactory -> operatorFactory.completeRuntimeConstraintWiring(context));
            return;
        }
        List<RuntimeConstraintRequest> pending = ImmutableList.of();
        for (int index = runtimeConstraintOperatorFactories.size() - 1; index >= 0; index--) {
            OperatorFactory operatorFactory = runtimeConstraintOperatorFactories.get(index);
            ImmutableList.Builder<RuntimeConstraintRequest> inputRequests = ImmutableList.builder();
            String owner = pipelineId + ":" + index + ":" + operatorFactory.getClass().getSimpleName();
            pending.forEach(request -> operatorFactory.propagateRuntimeConstraint(context.enterOperator(owner, request), inputRequests::add, context));
            inputRequests.addAll(operatorFactory.getInputRuntimeConstraints(context));
            operatorFactory.completeRuntimeConstraintWiring(context);
            pending = inputRequests.build();
        }
        pending.forEach(request -> context.stop(runtimeConstraintOperatorFactories.getFirst(), request));
    }

    public synchronized boolean propagateRuntimeConstraints(List<RuntimeConstraintRequest> requests, RuntimeConstraintWiringContext context)
    {
        requireNonNull(requests, "requests is null");
        requireNonNull(context, "context is null");
        boolean applied = true;
        List<RuntimeConstraintRequest> pending = ImmutableList.copyOf(requests);
        if (noMoreDrivers && !(runtimeConstraintOperatorFactories.getLast() instanceof RuntimeConstraintOutputOperatorFactory)) {
            List<RuntimeConstraintRequest> collectionRequests = requests.stream()
                    .filter(RuntimeConstraintRequest::isCollection)
                    .toList();
            collectionRequests.forEach(request -> context.stop("closed " + DriverFactory.class.getSimpleName(), request));
            pending = requests.stream()
                    .filter(request -> !request.isCollection())
                    .toList();
            applied = collectionRequests.isEmpty();
        }
        for (int index = runtimeConstraintOperatorFactories.size() - 1; index >= 0; index--) {
            OperatorFactory operatorFactory = runtimeConstraintOperatorFactories.get(index);
            ImmutableList.Builder<RuntimeConstraintRequest> inputRequests = ImmutableList.builder();
            String owner = pipelineId + ":" + index + ":" + operatorFactory.getClass().getSimpleName();
            pending.forEach(request -> operatorFactory.propagateRuntimeConstraint(context.enterOperator(owner, request), inputRequests::add, context));
            operatorFactory.completeRuntimeConstraintWiring(context);
            pending = inputRequests.build();
        }
        pending.forEach(request -> context.stop(runtimeConstraintOperatorFactories.getFirst(), request));
        return applied;
    }

    @Nullable
    public List<OperatorFactory> getOperatorFactories()
    {
        return operatorFactories;
    }

    public Driver createDriver(DriverContext driverContext)
    {
        requireNonNull(driverContext, "driverContext is null");
        List<Operator> operators = new ArrayList<>(operatorFactories.size());
        try {
            synchronized (this) {
                // must check noMoreDrivers after acquiring the lock
                checkState(!noMoreDrivers, "noMoreDrivers is already set");
                for (OperatorFactory operatorFactory : operatorFactories) {
                    Operator operator = operatorFactory.createOperator(driverContext);
                    operators.add(operator);
                }
            }
            // Driver creation can continue without holding the lock
            return Driver.createDriver(driverContext, operators);
        }
        catch (Throwable failure) {
            for (Operator operator : operators) {
                try {
                    operator.close();
                }
                catch (Throwable closeFailure) {
                    if (failure != closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            for (OperatorContext operatorContext : driverContext.getOperatorContexts()) {
                try {
                    operatorContext.destroy();
                }
                catch (Throwable destroyFailure) {
                    if (failure != destroyFailure) {
                        failure.addSuppressed(destroyFailure);
                    }
                }
            }
            driverContext.failed(failure);
            throw failure;
        }
    }

    public synchronized void noMoreDrivers()
    {
        if (noMoreDrivers) {
            return;
        }
        for (OperatorFactory operatorFactory : operatorFactories) {
            operatorFactory.noMoreOperators();
        }
        operatorFactories = null;
        noMoreDrivers = true;
    }

    // no need to synchronize when just checking the boolean flag
    @SuppressWarnings("GuardedBy")
    public boolean isNoMoreDrivers()
    {
        return noMoreDrivers;
    }
}
