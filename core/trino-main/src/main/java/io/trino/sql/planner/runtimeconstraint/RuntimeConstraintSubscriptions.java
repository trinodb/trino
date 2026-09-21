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
package io.trino.sql.planner.runtimeconstraint;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.DISABLED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.FINAL;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.PENDING;
import static java.util.Objects.requireNonNull;

/// A runtime graph shared by the drivers of a physical task, or mirrored for coordinator split pruning.
/// Callbacks run outside the graph lock and are drained iteratively, including synchronous late subscriptions.
public final class RuntimeConstraintSubscriptions
        implements AutoCloseable
{
    private final long generation;
    private final long maxRetainedBytesPerConstraint;
    private final RuntimeConstraintTransform.Context context;
    private final LongPredicate reserveMemory;
    private final Function<RuntimeConstraintId, CompletableFuture<Void>> initialUnblock;
    private final Map<RuntimeConstraintId, Node> nodes = new HashMap<>();
    private final Map<RuntimeConstraintId, Long> retainedByConstraint = new HashMap<>();
    private final Queue<Runnable> pending = new ArrayDeque<>();
    private long retainedBytes;
    private boolean draining;
    private boolean closed;

    public RuntimeConstraintSubscriptions(
            long generation,
            long maxRetainedBytesPerConstraint,
            RuntimeConstraintTransform.Context context,
            LongPredicate reserveMemory,
            Function<RuntimeConstraintId, CompletableFuture<Void>> initialUnblock)
    {
        checkArgument(generation >= 0, "generation is negative");
        checkArgument(maxRetainedBytesPerConstraint >= 0, "maxRetainedBytesPerConstraint is negative");
        this.generation = generation;
        this.maxRetainedBytesPerConstraint = maxRetainedBytesPerConstraint;
        this.context = context;
        this.reserveMemory = requireNonNull(reserveMemory, "reserveMemory is null");
        this.initialUnblock = initialUnblock;
    }

    public void registerInput(RuntimeConstraintSubscription.Input input, CompletableFuture<RuntimeConstraintSnapshot> updates)
    {
        registerInput(input, () -> updates);
    }

    public void registerInput(RuntimeConstraintSubscription.Input input, Supplier<CompletableFuture<RuntimeConstraintSnapshot>> updates)
    {
        synchronized (this) {
            if (closed) {
                return;
            }
            Node node = nodes.computeIfAbsent(input.id(), _ -> new Node());
            checkState(node.subscription == null, "local subscription cannot also be a graph input: %s", input.id());
            checkArgument(node.constraintId == null || node.constraintId.equals(input.constraintId()), "conflicting subscription root: %s", input.id());
            node.constraintId = input.constraintId();
            if (node.input) {
                return;
            }
            node.input = true;
        }
        updates.get().whenComplete((snapshot, failure) -> enqueue(() -> {
            if (failure != null) {
                fail(input.id(), failure);
                return;
            }
            try {
                publish(input.id(), snapshot);
            }
            catch (Throwable e) {
                fail(input.id(), e);
            }
        }));
    }

    public void register(RuntimeConstraintSubscription subscription)
    {
        CompletableFuture<RuntimeConstraintSnapshot> input;
        synchronized (this) {
            if (closed) {
                return;
            }
            Node node = nodes.computeIfAbsent(subscription.id(), _ -> new Node());
            checkState(!node.input, "graph input cannot also be a local subscription: %s", subscription.id());
            if (node.subscription != null) {
                checkArgument(node.subscription.equals(subscription), "conflicting subscription: %s", subscription.id());
                return;
            }
            Node inputNode = nodes.computeIfAbsent(subscription.inputId(), _ -> new Node());
            // Each node has at most one input. Connecting nodes already in the same tree creates a cycle.
            Node component = node.component();
            Node inputComponent = inputNode.component();
            checkArgument(component != inputComponent, "subscription cycle: %s", subscription.id());
            component.merge(inputComponent);
            node.subscription = subscription;
            node.constraintId = subscription.constraintId();
            input = inputNode.value;
        }
        input.whenComplete((snapshot, failure) -> enqueue(() -> {
            if (failure != null) {
                fail(subscription.id(), failure);
                return;
            }
            try {
                RuntimeConstraintSnapshot result;
                if (snapshot.state() == FINAL) {
                    RuntimeMembershipPayload original = (RuntimeMembershipPayload) snapshot.payload().orElseThrow();
                    RuntimeMembershipPayload transformed = subscription.transform().apply(original, context);
                    result = RuntimeConstraintSnapshot.finalSnapshot(subscription.id(), generation, snapshot.version(), transformed);
                    if (transformed != original && !retain(subscription.constraintId(), transformed.getRetainedSizeInBytes())) {
                        result = RuntimeConstraintSnapshot.terminal(subscription.id(), generation, snapshot.version(), DISABLED);
                    }
                }
                else {
                    result = RuntimeConstraintSnapshot.terminal(subscription.id(), generation, snapshot.version(), snapshot.state());
                }
                publish(subscription.id(), result);
            }
            catch (Throwable e) {
                fail(subscription.id(), e);
            }
        }));
    }

    public RuntimeConstraintWiringReport.Binding registerBinding(String owner, RuntimeConstraintWiringReport.Binding binding)
    {
        if (binding.request().subscriptionId().isPresent()) {
            return binding;
        }
        var request = binding.request();
        RuntimeConstraintSubscription comparison = RuntimeConstraintSubscription.create(
                request.constraintId(),
                request.constraintId(),
                owner,
                RuntimeConstraintTransform.comparison(request.operator(), request.nullAllowed()));
        register(comparison);
        RuntimeConstraintId id = comparison.id();
        if (request.targetType().isPresent()) {
            RuntimeConstraintSubscription cast = RuntimeConstraintSubscription.create(
                    id,
                    request.constraintId(),
                    owner,
                    RuntimeConstraintTransform.cast(request.targetType().orElseThrow()));
            register(cast);
            id = cast.id();
        }
        return new RuntimeConstraintWiringReport.Binding(request.withSubscription(id), binding.column());
    }

    public synchronized CompletableFuture<RuntimeConstraintSnapshot> waitForUpdate(RuntimeConstraintId id, long afterVersion)
    {
        if (closed) {
            return CompletableFuture.completedFuture(RuntimeConstraintSnapshot.terminal(id, generation, afterVersion + 1, CLOSED));
        }
        // Membership subscriptions publish one terminal update. Copies prevent consumer cancellation from cancelling the graph.
        return nodes.computeIfAbsent(id, _ -> new Node()).value.copy();
    }

    public CompletableFuture<Void> waitForInitialUnblock(RuntimeConstraintId id)
    {
        RuntimeConstraintId constraintId;
        synchronized (this) {
            if (closed) {
                return CompletableFuture.completedFuture(null);
            }
            constraintId = requireNonNull(nodes.get(id), "subscription is not registered").constraintId;
        }
        if (initialUnblock == null) {
            return new CompletableFuture<>();
        }
        return initialUnblock.apply(requireNonNull(constraintId, "subscription root is not registered"));
    }

    public synchronized long retainedBytes()
    {
        return retainedBytes;
    }

    private synchronized boolean retain(RuntimeConstraintId id, long bytes)
    {
        if (closed) {
            return false;
        }
        long retained = retainedByConstraint.getOrDefault(id, 0L);
        if (bytes > maxRetainedBytesPerConstraint - retained || !reserveMemory.test(bytes)) {
            return false;
        }
        retainedByConstraint.put(id, retained + bytes);
        retainedBytes += bytes;
        return true;
    }

    private void publish(RuntimeConstraintId id, RuntimeConstraintSnapshot snapshot)
    {
        CompletableFuture<RuntimeConstraintSnapshot> value;
        synchronized (this) {
            if (closed || snapshot.generation() != generation) {
                return;
            }
            checkArgument(snapshot.constraintId().equals(id), "subscription update has wrong identity");
            checkArgument(snapshot.state() != PENDING, "subscription update is pending");
            value = nodes.get(id).value;
        }
        value.complete(snapshot);
    }

    private void fail(RuntimeConstraintId id, Throwable failure)
    {
        CompletableFuture<RuntimeConstraintSnapshot> value;
        synchronized (this) {
            if (closed) {
                return;
            }
            value = nodes.get(id).value;
        }
        value.completeExceptionally(failure);
    }

    private void enqueue(Runnable action)
    {
        synchronized (this) {
            if (closed) {
                return;
            }
            pending.add(action);
            if (draining) {
                return;
            }
            draining = true;
        }
        while (true) {
            Runnable next;
            synchronized (this) {
                next = pending.poll();
                if (next == null) {
                    draining = false;
                    return;
                }
            }
            next.run();
        }
    }

    @Override
    public void close()
    {
        List<Runnable> completions = new ArrayList<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            nodes.forEach((id, node) -> completions.add(() -> node.value.complete(RuntimeConstraintSnapshot.terminal(id, generation, 1, CLOSED))));
            nodes.clear();
            pending.clear();
            retainedByConstraint.clear();
            verify(reserveMemory.test(-retainedBytes), "subscription memory release failed");
            retainedBytes = 0;
        }
        completions.forEach(Runnable::run);
    }

    private static final class Node
    {
        private final CompletableFuture<RuntimeConstraintSnapshot> value = new CompletableFuture<>();
        private RuntimeConstraintSubscription subscription;
        private RuntimeConstraintId constraintId;
        private boolean input;
        private Node component = this;
        private int rank;

        public Node component()
        {
            Node node = this;
            while (node.component != node) {
                node.component = node.component.component;
                node = node.component;
            }
            return node;
        }

        public void merge(Node other)
        {
            if (rank < other.rank) {
                component = other;
            }
            else {
                other.component = this;
                if (rank == other.rank) {
                    rank++;
                }
            }
        }
    }
}
