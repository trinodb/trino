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

import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.RuntimeConstraintCollectionLimits;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.operator.RuntimeConstraintSourceOperator;
import io.trino.operator.RuntimeConstraintWiringContext;
import io.trino.operator.TaskRuntimeConstraintManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.LocalRuntimeConstraintConsumer;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class NestedLoopRuntimeConstraintSource
{
    private final PlanNodeId sourceId;
    private final List<Type> buildTypes;
    private final RuntimeConstraintCollectionLimits limits;
    private final TypeOperators typeOperators;
    private final DistributedCompletionPolicy completionPolicy;
    private final List<Binding> bindings = new ArrayList<>();
    private final AtomicReference<TaskRuntimeConstraintManager> manager = new AtomicReference<>();

    private boolean registrationDeferred;
    private boolean collectionRelocated;
    private Consumer<List<RuntimeConstraintRequest>> buildInput;
    private LocalRuntimeConstraintConsumer consumer;
    private int createdOperators;

    public NestedLoopRuntimeConstraintSource(
            PlanNodeId sourceId,
            List<Type> buildTypes,
            RuntimeConstraintCollectionLimits limits,
            TypeOperators typeOperators,
            DistributedCompletionPolicy completionPolicy)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.buildTypes = List.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
        this.limits = requireNonNull(limits, "limits is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.completionPolicy = requireNonNull(completionPolicy, "completionPolicy is null");
    }

    public synchronized RuntimeConstraintRequest addComparison(
            int buildChannel,
            int probeChannel,
            ComparisonOperator operator,
            boolean nullAllowed,
            Type probeType,
            RuntimeConstraintWiringContext context)
    {
        Binding candidate = new Binding(buildChannel, probeChannel, operator, nullAllowed, probeType);
        int index = bindings.indexOf(candidate);
        if (index < 0) {
            checkState(consumer == null, "runtime constraint source is already initialized");
            bindings.add(candidate);
            index = bindings.size() - 1;
            if (collectionRelocated) {
                checkState(buildInput != null, "runtime constraint build input is not registered");
                buildInput.accept(List.of(collectionRequest(index, candidate)));
            }
        }
        if (!collectionRelocated && !registrationDeferred) {
            registrationDeferred = true;
            context.defer(() -> register(context));
        }
        return new RuntimeConstraintRequest(
                RuntimeConstraintRequest.joinConstraintId(sourceId, index),
                probeChannel,
                operator,
                nullAllowed,
                probeType);
    }

    private synchronized void register(RuntimeConstraintWiringContext context)
    {
        List<Integer> channels = buildChannels();
        context.registerSource(
                sourceId,
                IntStream.range(0, bindings.size())
                        .mapToObj(index -> new CollectedConstraint(
                                RuntimeConstraintRequest.joinConstraintId(sourceId, index),
                                bindings.get(index).operator(),
                                bindings.get(index).nullAllowed(),
                                channels.indexOf(bindings.get(index).buildChannel())))
                        .toList(),
                channels.stream().map(buildTypes::get).toList(),
                completionPolicy);
        initializeConsumer();
    }

    public synchronized Operator createCollector(DriverContext driverContext, OperatorContext operatorContext)
    {
        TaskRuntimeConstraintManager taskManager = driverContext.getPipelineContext().getTaskContext().getRuntimeConstraintManager();
        manager.compareAndSet(null, taskManager);
        checkState(manager.get() == taskManager, "runtime constraint manager changed");
        createdOperators++;
        if (bindings.isEmpty() || collectionRelocated) {
            return null;
        }
        checkState(consumer != null, "runtime constraint source is not initialized");
        return RuntimeConstraintSourceOperator.createCollector(
                operatorContext,
                consumer,
                buildChannels().stream()
                        .map(channel -> new RuntimeConstraintSourceOperator.Channel(buildTypes.get(channel), channel))
                        .toList(),
                limits.maxDistinctValues(),
                limits.maxFilterSize(),
                limits.minMaxCollectionLimit(),
                typeOperators);
    }

    public synchronized void noMoreOperators()
    {
        if (!collectionRelocated && consumer != null && createdOperators > 0) {
            consumer.setPartitionCount(createdOperators);
        }
    }

    public synchronized void registerBuildInput(Consumer<List<RuntimeConstraintRequest>> buildInput, RuntimeConstraintWiringContext context)
    {
        requireNonNull(buildInput, "buildInput is null");
        requireNonNull(context, "context is null");
        if (!context.isTaskRetry()) {
            return;
        }
        checkState(this.buildInput == null, "runtime constraint build input is already registered");
        collectionRelocated = true;
        this.buildInput = buildInput;
        if (!bindings.isEmpty()) {
            buildInput.accept(IntStream.range(0, bindings.size())
                    .mapToObj(index -> collectionRequest(index, bindings.get(index)))
                    .toList());
        }
    }

    public boolean isBuildChannel(int channel)
    {
        return channel >= 0 && channel < buildTypes.size();
    }

    private RuntimeConstraintRequest collectionRequest(int index, Binding binding)
    {
        return RuntimeConstraintRequest.collection(
                RuntimeConstraintRequest.joinConstraintId(sourceId, index),
                binding.buildChannel(),
                binding.operator(),
                binding.nullAllowed(),
                buildTypes.get(binding.buildChannel()),
                completionPolicy == DistributedCompletionPolicy.EQUIVALENT_REPLICAS);
    }

    private void initializeConsumer()
    {
        if (consumer != null) {
            return;
        }
        List<Integer> channels = buildChannels();
        consumer = new LocalRuntimeConstraintConsumer(
                channels,
                channels.stream().map(buildTypes::get).toList(),
                payload -> manager.get().addContribution(sourceId, payload),
                limits.maxSizePerOperator());
    }

    private List<Integer> buildChannels()
    {
        return bindings.stream().map(Binding::buildChannel).distinct().toList();
    }

    private record Binding(int buildChannel, int probeChannel, ComparisonOperator operator, boolean nullAllowed, Type probeType)
    {
        private Binding
        {
            requireNonNull(operator, "operator is null");
            requireNonNull(probeType, "probeType is null");
        }
    }
}
