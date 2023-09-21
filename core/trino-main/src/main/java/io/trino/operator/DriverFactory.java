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
    private final List<OperatorFactory> operatorFactories;
    private final Optional<PlanNodeId> sourceId;
    private final OptionalInt driverInstances;

    // must synchronize between createDriver() and noMoreDrivers(), but isNoMoreDrivers() is safe without synchronizing
    @GuardedBy("this")
    private volatile boolean noMoreDrivers;

    public DriverFactory(int pipelineId, boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances)
    {
        this.pipelineId = pipelineId;
        this.inputDriver = inputDriver;
        this.outputDriver = outputDriver;
        this.operatorFactories = ImmutableList.copyOf(requireNonNull(operatorFactories, "operatorFactories is null"));
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
        noMoreDrivers = true;
        for (OperatorFactory operatorFactory : operatorFactories) {
            operatorFactory.noMoreOperators();
        }
    }

    // no need to synchronize when just checking the boolean flag
    @SuppressWarnings("GuardedBy")
    public boolean isNoMoreDrivers()
    {
        return noMoreDrivers;
    }
}
