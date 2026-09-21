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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public final class RuntimeConstraintDynamicFilter
        implements DynamicFilter
{
    private final Set<ColumnHandle> columnsCovered;
    private final Map<RuntimeConstraintId, List<ConstraintBinding>> bindings;
    @GuardedBy("this")
    private final Set<RuntimeConstraintId> pending;
    @GuardedBy("this")
    private final Set<RuntimeConstraintId> pendingAwaitable;

    @GuardedBy("this")
    private TupleDomain<ColumnHandle> currentPredicate = TupleDomain.all();
    @GuardedBy("this")
    private CompletableFuture<Void> blocked;
    @GuardedBy("this")
    private RuntimeException failure;

    private RuntimeConstraintDynamicFilter(
            BiFunction<RuntimeConstraintId, Long, CompletableFuture<RuntimeConstraintSnapshot>> updates,
            Function<RuntimeConstraintId, CompletableFuture<Void>> initialUnblock,
            List<RuntimeConstraintWiringReport.Binding> scanBindings,
            List<ColumnHandle> columns)
    {
        requireNonNull(updates, "updates is null");
        requireNonNull(scanBindings, "scanBindings is null");
        requireNonNull(columns, "columns is null");

        Map<RuntimeConstraintId, List<ConstraintBinding>> bindings = new HashMap<>();
        for (RuntimeConstraintWiringReport.Binding binding : scanBindings) {
            int channel = columns.indexOf(binding.column());
            checkArgument(channel >= 0, "runtime constraint column is not produced by scan");
            ConstraintBinding constraintBinding = new ConstraintBinding(
                    ImmutableList.of(columns.get(channel)),
                    true);
            bindings.computeIfAbsent(binding.request().subscription(), _ -> new ArrayList<>()).add(constraintBinding);
        }
        this.bindings = bindings.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
        this.columnsCovered = bindings.values().stream()
                .flatMap(List::stream)
                .flatMap(binding -> binding.columns().stream())
                .collect(toImmutableSet());
        this.pending = new HashSet<>(bindings.keySet());
        this.pendingAwaitable = new HashSet<>(bindings.entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(ConstraintBinding::awaitable))
                .map(Map.Entry::getKey)
                .collect(toImmutableSet()));
        this.blocked = pendingAwaitable.isEmpty() ? null : new NonCancellableCompletableFuture<>();

        Set<RuntimeConstraintId> initiallyAwaitable = ImmutableSet.copyOf(pendingAwaitable);
        bindings.keySet().forEach(id -> updates.apply(id, 0L).whenComplete((snapshot, failure) -> {
            if (failure != null) {
                fail(failure);
            }
            else {
                try {
                    update(id, snapshot);
                }
                catch (Throwable e) {
                    fail(e);
                }
            }
        }));
        if (initialUnblock != null) {
            initiallyAwaitable.forEach(id -> initialUnblock.apply(id).thenRun(() -> unblock(id)));
        }
    }

    public static DynamicFilter create(
            RuntimeConstraintSubscriptions subscriptions,
            List<RuntimeConstraintWiringReport.Binding> scanBindings,
            List<ColumnHandle> columns)
    {
        if (scanBindings.isEmpty()) {
            return EMPTY;
        }
        return new RuntimeConstraintDynamicFilter(subscriptions::waitForUpdate, subscriptions::waitForInitialUnblock, scanBindings, columns);
    }

    public static DynamicFilter combine(DynamicFilter first, DynamicFilter second)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        if (first == EMPTY) {
            return second;
        }
        if (second == EMPTY) {
            return first;
        }
        return new CombinedDynamicFilter(first, second);
    }

    private void fail(Throwable cause)
    {
        CompletableFuture<Void> currentBlocked;
        synchronized (this) {
            failure = new CompletionException(cause);
            currentBlocked = blocked;
        }
        if (currentBlocked != null) {
            currentBlocked.completeExceptionally(cause);
        }
    }

    private void update(RuntimeConstraintId id, RuntimeConstraintSnapshot snapshot)
    {
        CompletableFuture<Void> currentBlocked;
        synchronized (this) {
            verify(pending.remove(id), "runtime constraint completed more than once: %s", id);
            pendingAwaitable.remove(id);
            if (snapshot.state() == RuntimeConstraintPublicationState.FINAL) {
                RuntimeConstraintPayload payload = snapshot.payload().orElseThrow();
                checkArgument(payload instanceof RuntimeMembershipPayload, "unsupported runtime constraint payload: %s", payload.getClass().getSimpleName());
                RuntimeMembershipPayload membership = (RuntimeMembershipPayload) payload;
                List<ConstraintBinding> constraintBindings = requireNonNull(bindings.get(id), "constraint binding is missing");
                Map<ColumnHandle, Domain> domains = new HashMap<>();
                for (ConstraintBinding binding : constraintBindings) {
                    checkArgument(binding.columns().size() == membership.lanes().size(), "runtime constraint payload has wrong lane count");
                    for (int lane = 0; lane < binding.columns().size(); lane++) {
                        Domain domain = membership.lanes().get(lane).domain();
                        domains.merge(binding.columns().get(lane), domain, Domain::intersect);
                    }
                }
                currentPredicate = currentPredicate.intersect(TupleDomain.withColumnDomains(domains));
            }
            currentBlocked = blocked;
            blocked = pendingAwaitable.isEmpty() ? null : new NonCancellableCompletableFuture<>();
        }
        if (currentBlocked != null) {
            currentBlocked.complete(null);
        }
    }

    private void unblock(RuntimeConstraintId id)
    {
        CompletableFuture<Void> currentBlocked;
        synchronized (this) {
            if (!pendingAwaitable.remove(id)) {
                return;
            }
            currentBlocked = blocked;
            blocked = pendingAwaitable.isEmpty() ? null : new NonCancellableCompletableFuture<>();
        }
        if (currentBlocked != null) {
            currentBlocked.complete(null);
        }
    }

    @Override
    public Set<ColumnHandle> getColumnsCovered()
    {
        return columnsCovered;
    }

    @Override
    public synchronized CompletableFuture<?> isBlocked()
    {
        if (failure != null) {
            return CompletableFuture.failedFuture(failure);
        }
        return blocked == null ? NOT_BLOCKED : blocked;
    }

    @Override
    public synchronized boolean isComplete()
    {
        return pending.isEmpty();
    }

    @Override
    public synchronized boolean isAwaitable()
    {
        return !pendingAwaitable.isEmpty();
    }

    @Override
    public synchronized TupleDomain<ColumnHandle> getCurrentPredicate()
    {
        if (failure != null) {
            throw failure;
        }
        return currentPredicate;
    }

    private record ConstraintBinding(List<ColumnHandle> columns, boolean awaitable)
    {
        private ConstraintBinding
        {
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
        }
    }

    private static final class CombinedDynamicFilter
            implements DynamicFilter
    {
        private final DynamicFilter first;
        private final DynamicFilter second;
        private final Set<ColumnHandle> columnsCovered;

        private CombinedDynamicFilter(DynamicFilter first, DynamicFilter second)
        {
            this.first = requireNonNull(first, "first is null");
            this.second = requireNonNull(second, "second is null");
            this.columnsCovered = ImmutableSet.<ColumnHandle>builder()
                    .addAll(first.getColumnsCovered())
                    .addAll(second.getColumnsCovered())
                    .build();
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return columnsCovered;
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            List<CompletableFuture<?>> futures = new ArrayList<>();
            if (first.isAwaitable()) {
                futures.add(first.isBlocked());
            }
            if (second.isAwaitable()) {
                futures.add(second.isBlocked());
            }
            if (futures.isEmpty()) {
                return NOT_BLOCKED;
            }
            NonCancellableCompletableFuture<Object> blocked = new NonCancellableCompletableFuture<>();
            CompletableFuture.anyOf(futures.toArray(CompletableFuture[]::new))
                    .whenComplete((value, failure) -> {
                        if (failure != null) {
                            blocked.completeExceptionally(failure);
                        }
                        else {
                            blocked.complete(value);
                        }
                    });
            return blocked;
        }

        @Override
        public boolean isComplete()
        {
            return first.isComplete() && second.isComplete();
        }

        @Override
        public boolean isAwaitable()
        {
            return first.isAwaitable() || second.isAwaitable();
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return first.getCurrentPredicate().intersect(second.getCurrentPredicate());
        }
    }

    private static final class NonCancellableCompletableFuture<T>
            extends CompletableFuture<T>
    {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }
    }
}
