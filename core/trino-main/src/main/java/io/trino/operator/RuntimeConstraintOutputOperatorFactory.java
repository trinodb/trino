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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.Page;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.LocalRuntimeConstraintConsumer;
import io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class RuntimeConstraintOutputOperatorFactory
        implements OperatorFactory
{
    private final OperatorFactory delegate;
    private final List<Integer> inputChannels;
    private final RuntimeConstraintCollectionLimits collectionLimits;
    private final TypeOperators typeOperators;
    private final Map<RuntimeConstraintRequest, CollectionState> collections = new LinkedHashMap<>();
    private final List<CollectingOutputOperator> operators = new ArrayList<>();

    private boolean closed;

    public RuntimeConstraintOutputOperatorFactory(
            OperatorFactory delegate,
            List<Integer> inputChannels,
            RuntimeConstraintCollectionLimits collectionLimits,
            TypeOperators typeOperators)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.inputChannels = ImmutableList.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
        this.collectionLimits = requireNonNull(collectionLimits, "collectionLimits is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    @Override
    public synchronized Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        CollectingOutputOperator operator = new CollectingOutputOperator(delegate.createOperator(driverContext));
        operators.add(operator);
        collections.values().forEach(collection -> addCollector(operator, collection));
        return operator;
    }

    @Override
    public synchronized void noMoreOperators()
    {
        checkState(!closed, "Factory is already closed");
        closed = true;
        delegate.noMoreOperators();
        collections.values().forEach(CollectionState::noMoreOperators);
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new RuntimeConstraintOutputOperatorFactory(delegate.duplicate(), inputChannels, collectionLimits, typeOperators);
    }

    @Override
    public synchronized void propagateRuntimeConstraint(
            RuntimeConstraintRequest request,
            Consumer<RuntimeConstraintRequest> input,
            RuntimeConstraintWiringContext context)
    {
        if (!request.channelsMatch(channel -> channel < inputChannels.size())) {
            context.stop(this, request);
            return;
        }
        RuntimeConstraintRequest inputRequest = request.mapChannels(inputChannels::get);
        if (!request.isCollection()) {
            input.accept(inputRequest);
            return;
        }

        context.registerSource(
                inputRequest.collectionSourceId(),
                ImmutableList.of(new CollectedConstraint(inputRequest.constraintId(), inputRequest.operator(), inputRequest.nullAllowed(), 0)),
                ImmutableList.of(inputRequest.targetType().orElseThrow()),
                DistributedCompletionPolicy.UNION_ALL_PARTITIONS,
                inputRequest.isReplicatedCollection());
        CollectionState collection = collections.computeIfAbsent(inputRequest, key -> new CollectionState(key, context));
        // Closing the factory prevents new operators, but existing operators can
        // still install a collector before consuming their first input page.
        boolean collectionInstalled = !closed || !operators.isEmpty();
        for (CollectingOutputOperator operator : operators) {
            collectionInstalled &= addCollector(operator, collection);
        }
        if (!collectionInstalled) {
            collection.disable();
            context.stop(this, request);
        }
        if (closed && collectionInstalled) {
            collection.noMoreOperators();
        }
    }

    private boolean addCollector(CollectingOutputOperator operator, CollectionState collection)
    {
        if (operator.hasCollection(collection.request())) {
            return true;
        }
        LocalRuntimeConstraintConsumer consumer = collection.consumer;
        boolean installed = operator.addCollection(collection.request(), () -> RuntimeConstraintSourceOperator.createCollector(
                operator.getOperatorContext(),
                consumer,
                ImmutableList.of(new RuntimeConstraintSourceOperator.Channel(
                        collection.request().targetType().orElseThrow(),
                        collection.request().channel())),
                collectionLimits.maxDistinctValues(),
                collectionLimits.maxFilterSize(),
                collectionLimits.minMaxCollectionLimit(),
                typeOperators));
        if (installed) {
            collection.operatorCreated();
        }
        return installed;
    }

    private final class CollectionState
    {
        private final RuntimeConstraintRequest request;
        private final LocalRuntimeConstraintConsumer consumer;
        private int operatorCount;

        private CollectionState(RuntimeConstraintRequest request, RuntimeConstraintWiringContext context)
        {
            this.request = requireNonNull(request, "request is null");
            Type type = request.targetType().orElseThrow();
            consumer = new LocalRuntimeConstraintConsumer(
                    ImmutableList.of(request.channel()),
                    ImmutableList.of(type),
                    payload -> context.addContribution(request.collectionSourceId(), payload),
                    collectionLimits.maxSizePerOperator());
        }

        public RuntimeConstraintRequest request()
        {
            return request;
        }

        public void disable()
        {
            // Complete the shared consumer so collectors installed on only some
            // drivers cannot later replace this unrestricted contribution.
            consumer.addPartition(
                    ImmutableList.of(Domain.all(request.targetType().orElseThrow())),
                    new RuntimeConstraintSourceConsumer.Observation(false, ImmutableList.of(false)));
        }

        public void operatorCreated()
        {
            operatorCount++;
        }

        public void noMoreOperators()
        {
            consumer.setPartitionCount(operatorCount);
        }
    }

    private static final class CollectingOutputOperator
            implements Operator
    {
        private final Operator delegate;
        private final Map<RuntimeConstraintRequest, Operator> collectors = new LinkedHashMap<>();
        private boolean collectionClosed;
        private boolean finished;

        private CollectingOutputOperator(Operator delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        public synchronized boolean hasCollection(RuntimeConstraintRequest request)
        {
            return collectors.containsKey(request);
        }

        public synchronized boolean addCollection(RuntimeConstraintRequest request, Supplier<Operator> collector)
        {
            if (collectionClosed) {
                return false;
            }
            Operator collectingOperator = collector.get();
            checkState(collectors.putIfAbsent(request, collectingOperator) == null, "runtime constraint collection is already registered");
            if (finished || delegate.isFinished()) {
                collectingOperator.finish();
            }
            return true;
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return delegate.getOperatorContext();
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return delegate.isBlocked();
        }

        @Override
        public boolean needsInput()
        {
            return delegate.needsInput() && collectors.values().stream().allMatch(Operator::needsInput);
        }

        @Override
        public synchronized void addInput(Page page)
        {
            collectionClosed = true;
            for (Operator collector : collectors.values()) {
                collector.addInput(page);
                checkState(collector.getOutput() == page, "runtime constraint collector replaced output page");
            }
            delegate.addInput(page);
        }

        @Override
        public Page getOutput()
        {
            return delegate.getOutput();
        }

        @Override
        public ListenableFuture<Void> startMemoryRevoke()
        {
            return delegate.startMemoryRevoke();
        }

        @Override
        public void finishMemoryRevoke()
        {
            delegate.finishMemoryRevoke();
        }

        @Override
        public synchronized void finish()
        {
            finished = true;
            collectors.values().forEach(Operator::finish);
            delegate.finish();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public void close()
                throws Exception
        {
            synchronized (this) {
                collectionClosed = true;
            }
            Exception failure = null;
            for (Operator collector : collectors.values()) {
                try {
                    collector.close();
                }
                catch (Exception e) {
                    failure = e;
                }
            }
            try {
                delegate.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
