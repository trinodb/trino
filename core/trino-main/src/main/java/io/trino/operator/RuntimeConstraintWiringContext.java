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
import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintDynamicFilter;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSnapshot;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSubscription;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSubscriptions;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintTransform;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static java.util.Objects.requireNonNull;

public final class RuntimeConstraintWiringContext
{
    private final TaskRuntimeConstraintManager manager;
    private final boolean enabled;
    private final Session session;
    private final RuntimeConstraintSubscriptions subscriptions;
    private final Map<RuntimeConstraintId, RuntimeConstraintSubscription> subscriptionNodes = new LinkedHashMap<>();
    private final Map<RuntimeConstraintId, RuntimeConstraintSubscription.Input> subscriptionInputs = new LinkedHashMap<>();
    private final List<ScanBinding> scanBindings = new ArrayList<>();
    private final List<StoppedRequest> stoppedRequests = new ArrayList<>();
    private final Set<PlanNodeId> completedScans = new LinkedHashSet<>();
    private final Set<RuntimeConstraintWiringReport.Source> sources = new LinkedHashSet<>();
    private final List<Runnable> deferred = new ArrayList<>();
    private final Set<RuntimeConstraintWiringReport.RemoteRequest> remoteRequests = new LinkedHashSet<>();
    private final Map<PlanNodeId, ScanRegistration> scanRegistrations = new LinkedHashMap<>();
    private final Set<RuntimeConstraintRequest> appliedOutputRequests = new LinkedHashSet<>();
    private final Set<RuntimeConstraintRequest> rejectedOutputRequests = new LinkedHashSet<>();
    private final Map<Object, Set<RuntimeConstraintRequest>> localRequests = new LinkedHashMap<>();
    private final Map<Object, List<Consumer<List<RuntimeConstraintRequest>>>> localConsumers = new LinkedHashMap<>();
    private final Deque<Runnable> pendingScanInstallations = new ArrayDeque<>();
    private boolean installingScanFilters;

    public RuntimeConstraintWiringContext()
    {
        manager = null;
        enabled = true;
        session = null;
        subscriptions = null;
    }

    public RuntimeConstraintWiringContext(TaskRuntimeConstraintManager manager)
    {
        this(manager, true);
    }

    public RuntimeConstraintWiringContext(TaskRuntimeConstraintManager manager, boolean enabled)
    {
        this(manager, enabled, null, null, null, null);
    }

    public RuntimeConstraintWiringContext(
            TaskRuntimeConstraintManager manager,
            boolean enabled,
            Metadata metadata,
            FunctionManager functionManager,
            TypeOperators typeOperators,
            Session session)
    {
        this(manager, enabled, metadata, functionManager, typeOperators, session, Long.MAX_VALUE);
    }

    public RuntimeConstraintWiringContext(
            TaskRuntimeConstraintManager manager,
            boolean enabled,
            Metadata metadata,
            FunctionManager functionManager,
            TypeOperators typeOperators,
            Session session,
            long maxRetainedBytesPerConstraint)
    {
        this.manager = requireNonNull(manager, "manager is null");
        this.enabled = enabled;
        this.session = session;
        subscriptions = manager.createSubscriptions(metadata == null ? null : new RuntimeConstraintTransform.Context(metadata, functionManager, typeOperators, session), maxRetainedBytesPerConstraint);
    }

    public RuntimeConstraintRequest enterOperator(String owner, RuntimeConstraintRequest request)
    {
        if (!request.isConstraint() || subscriptions == null || !enabled) {
            return request;
        }
        request = startSubscription(owner, request);
        return transform(owner, request, RuntimeConstraintTransform.IDENTITY);
    }

    private RuntimeConstraintRequest startSubscription(String owner, RuntimeConstraintRequest request)
    {
        RuntimeConstraintId id = request.subscription();
        boolean external;
        synchronized (this) {
            external = !subscriptionNodes.containsKey(id) && !subscriptionInputs.containsKey(id);
            if (external) {
                subscriptionInputs.put(id, new RuntimeConstraintSubscription.Input(id, request.constraintId()));
            }
        }
        if (external) {
            manager.registerConsumer(id);
            subscriptions.registerInput(new RuntimeConstraintSubscription.Input(id, request.constraintId()), manager.waitForUpdate(id, 0));
        }
        if (request.subscriptionId().isPresent()) {
            return request;
        }
        RuntimeConstraintRequest normalized = transform(owner, request, RuntimeConstraintTransform.comparison(request.operator(), request.nullAllowed()));
        if (request.targetType().isPresent()) {
            normalized = transform(owner, normalized, RuntimeConstraintTransform.cast(request.targetType().orElseThrow()));
        }
        return normalized;
    }

    public RuntimeConstraintRequest transform(String owner, RuntimeConstraintRequest request, RuntimeConstraintTransform transformation)
    {
        if (!request.isConstraint() || subscriptions == null || !enabled) {
            return request;
        }
        RuntimeConstraintSubscription node = RuntimeConstraintSubscription.create(request.subscription(), request.constraintId(), manager.taskId().stageId().id() + ":" + owner, transformation);
        synchronized (this) {
            subscriptionNodes.putIfAbsent(node.id(), node);
        }
        subscriptions.register(node);
        return request.withSubscription(node.id());
    }

    public RuntimeConstraintRequest mapConstraint(String owner, RuntimeConstraintRequest request, RuntimeConstraintRequest mapped)
    {
        if (mapped.isConstraint() && mapped.targetType().isPresent() && !mapped.targetType().equals(request.targetType())) {
            return transform(owner, mapped, RuntimeConstraintTransform.cast(mapped.targetType().orElseThrow()));
        }
        return mapped;
    }

    /// Subscribe at this physical boundary; consumers receive the value after all downstream transformations.
    public CompletableFuture<RuntimeConstraintSnapshot> subscribe(RuntimeConstraintRequest request)
    {
        return requireNonNull(subscriptions, "subscriptions are not enabled").waitForUpdate(request.subscription(), 0);
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public boolean isTaskRetry()
    {
        return session != null && getRetryPolicy(session) == RetryPolicy.TASK;
    }

    public void bindScan(PlanNodeId scanId, ColumnHandle column, RuntimeConstraintRequest request)
    {
        if (!request.isConstraint()) {
            stop("scan", request);
            return;
        }
        if (subscriptions != null && enabled && request.subscriptionId().isEmpty()) {
            request = startSubscription("scan " + scanId, request);
        }
        ScanBinding binding = new ScanBinding(scanId, column, request);
        synchronized (this) {
            if (!scanBindings.contains(binding)) {
                scanBindings.add(binding);
                ScanRegistration registration = scanRegistrations.get(scanId);
                if (manager != null && registration != null) {
                    enqueueScanInstallation(
                            registration,
                            ImmutableList.of(new RuntimeConstraintWiringReport.Binding(request, column)));
                }
            }
        }
        installScanFilters();
    }

    public synchronized void defer(Runnable action)
    {
        deferred.add(requireNonNull(action, "action is null"));
    }

    public void finish()
    {
        List<Runnable> actions;
        synchronized (this) {
            actions = ImmutableList.copyOf(deferred);
            deferred.clear();
        }
        actions.forEach(Runnable::run);
    }

    public void completeScan(PlanNodeId scanId, List<ColumnHandle> columns, Consumer<DynamicFilter> installer)
    {
        requireNonNull(scanId, "scanId is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(installer, "installer is null");
        synchronized (this) {
            completedScans.add(scanId);
            ScanRegistration registration = new ScanRegistration(columns, installer);
            ScanRegistration previous = scanRegistrations.putIfAbsent(scanId, registration);
            if (previous == null && manager != null) {
                enqueueScanInstallation(registration, getBindings(scanId));
            }
        }
        installScanFilters();
    }

    private void enqueueScanInstallation(
            ScanRegistration registration,
            List<RuntimeConstraintWiringReport.Binding> bindings)
    {
        pendingScanInstallations.add(() -> registration.installer().accept(createDynamicFilter(bindings, registration.columns())));
    }

    private void installScanFilters()
    {
        synchronized (this) {
            if (installingScanFilters || pendingScanInstallations.isEmpty()) {
                return;
            }
            installingScanFilters = true;
        }
        while (true) {
            Runnable installation;
            synchronized (this) {
                installation = pendingScanInstallations.poll();
                if (installation == null) {
                    installingScanFilters = false;
                    return;
                }
            }
            try {
                installation.run();
            }
            catch (Throwable failure) {
                synchronized (this) {
                    installingScanFilters = false;
                }
                throw failure;
            }
        }
    }

    private DynamicFilter createDynamicFilter(List<RuntimeConstraintWiringReport.Binding> bindings, List<ColumnHandle> columns)
    {
        return RuntimeConstraintDynamicFilter.create(subscriptions, bindings, columns);
    }

    public synchronized void completeScan(PlanNodeId scanId)
    {
        completedScans.add(requireNonNull(scanId, "scanId is null"));
    }

    public void registerSource(PlanNodeId sourceId, List<Type> types)
    {
        registerSource(sourceId, types, DistributedCompletionPolicy.UNION_ALL_PARTITIONS);
    }

    public void registerSource(PlanNodeId sourceId, List<Type> types, DistributedCompletionPolicy completionPolicy)
    {
        List<CollectedConstraint> constraints = IntStream.range(0, types.size())
                .mapToObj(index -> new CollectedConstraint(RuntimeConstraintRequest.joinConstraintId(sourceId, index), index))
                .toList();
        registerSource(sourceId, constraints, types, completionPolicy);
    }

    public void registerSource(PlanNodeId sourceId, List<CollectedConstraint> constraints, List<Type> types)
    {
        registerSource(sourceId, constraints, types, DistributedCompletionPolicy.UNION_ALL_PARTITIONS);
    }

    public void registerSource(PlanNodeId sourceId, List<CollectedConstraint> constraints, List<Type> types, DistributedCompletionPolicy completionPolicy)
    {
        registerSource(sourceId, constraints, types, completionPolicy, completionPolicy == DistributedCompletionPolicy.EQUIVALENT_REPLICAS);
    }

    public void registerSource(PlanNodeId sourceId, List<CollectedConstraint> constraints, List<Type> types, DistributedCompletionPolicy completionPolicy, boolean replicated)
    {
        RuntimeConstraintWiringReport.Source source = new RuntimeConstraintWiringReport.Source(sourceId, constraints, types, completionPolicy, replicated);
        boolean added;
        synchronized (this) {
            added = sources.add(source);
        }
        if (added && manager != null) {
            manager.registerSource(source);
        }
    }

    public void stop(OperatorFactory operatorFactory, RuntimeConstraintRequest request)
    {
        stop(operatorFactory.getClass().getSimpleName(), request);
    }

    public synchronized void stop(String operatorType, RuntimeConstraintRequest request)
    {
        stoppedRequests.add(new StoppedRequest(operatorType, request));
        if (request.isCollection()) {
            rejectedOutputRequests.add(request);
        }
    }

    public void addContribution(PlanNodeId sourceId, RuntimeMembershipPayload payload)
    {
        if (manager != null) {
            manager.addContribution(sourceId, payload);
        }
    }

    public void disableCollection(RuntimeConstraintRequest request)
    {
        checkArgument(request.isCollection(), "request is not a collection request");
        Type targetType = request.targetType().orElseThrow();
        registerSource(
                request.collectionSourceId(),
                ImmutableList.of(new CollectedConstraint(request.constraintId(), request.operator(), request.nullAllowed(), 0)),
                ImmutableList.of(targetType),
                DistributedCompletionPolicy.UNION_ALL_PARTITIONS,
                request.isReplicatedCollection());
        if (manager != null) {
            manager.addUnrestrictedContribution(request.collectionSourceId(), targetType);
        }
    }

    public synchronized void bindRemoteSource(List<PlanFragmentId> sourceFragmentIds, RuntimeConstraintRequest request)
    {
        remoteRequests.add(new RuntimeConstraintWiringReport.RemoteRequest(sourceFragmentIds, request));
    }

    public void bindLocalSource(Object exchange, RuntimeConstraintRequest request)
    {
        List<Consumer<List<RuntimeConstraintRequest>>> consumers;
        synchronized (this) {
            if (!localRequests.computeIfAbsent(requireNonNull(exchange, "exchange is null"), _ -> new LinkedHashSet<>()).add(requireNonNull(request, "request is null"))) {
                return;
            }
            consumers = ImmutableList.copyOf(localConsumers.getOrDefault(exchange, ImmutableList.of()));
        }
        consumers.forEach(consumer -> consumer.accept(ImmutableList.of(request)));
    }

    public void registerLocalConsumer(Object exchange, Consumer<List<RuntimeConstraintRequest>> consumer)
    {
        List<RuntimeConstraintRequest> pending;
        synchronized (this) {
            localConsumers.computeIfAbsent(requireNonNull(exchange, "exchange is null"), _ -> new ArrayList<>())
                    .add(requireNonNull(consumer, "consumer is null"));
            pending = ImmutableList.copyOf(localRequests.getOrDefault(exchange, Set.of()));
        }
        if (!pending.isEmpty()) {
            consumer.accept(pending);
        }
    }

    public void registerOutput(List<DriverFactory> outputDrivers)
    {
        List<DriverFactory> drivers = ImmutableList.copyOf(requireNonNull(outputDrivers, "outputDrivers is null"));
        if (manager != null && enabled) {
            manager.registerRuntimeConstraintWiring(requests -> {
                List<RuntimeConstraintRequest> applied = requests.stream()
                        .filter(request -> propagateOutputRequest(drivers, request))
                        .toList();
                synchronized (this) {
                    appliedOutputRequests.addAll(applied);
                    rejectedOutputRequests.addAll(requests.stream()
                            .filter(request -> !applied.contains(request))
                            .toList());
                }
            });
        }
    }

    private boolean propagateOutputRequest(List<DriverFactory> outputDrivers, RuntimeConstraintRequest request)
    {
        // Each output factory collects independently. With multiple output branches
        // (such as matched and unmatched outer-join rows), no factory covers the
        // complete fragment output. Reject before any partial collector is installed.
        if (outputDrivers.isEmpty() || (request.isCollection() && outputDrivers.size() != 1)) {
            stop("fragment output", request);
            return false;
        }
        boolean applied = true;
        for (DriverFactory outputDriver : outputDrivers) {
            applied &= outputDriver.propagateRuntimeConstraints(ImmutableList.of(request), this);
        }
        return applied;
    }

    public synchronized List<ScanBinding> getScanBindings()
    {
        return ImmutableList.copyOf(scanBindings);
    }

    public synchronized List<StoppedRequest> getStoppedRequests()
    {
        return ImmutableList.copyOf(stoppedRequests);
    }

    public synchronized RuntimeConstraintWiringReport getReport()
    {
        return new RuntimeConstraintWiringReport(completedScans.stream()
                .map(scanId -> new RuntimeConstraintWiringReport.ScanWiring(
                        scanId,
                        getBindings(scanId)))
                .toList(), ImmutableList.copyOf(sources), ImmutableList.copyOf(remoteRequests), ImmutableList.copyOf(appliedOutputRequests), ImmutableList.copyOf(rejectedOutputRequests), ImmutableList.copyOf(subscriptionNodes.values()), ImmutableList.copyOf(subscriptionInputs.values()));
    }

    private List<RuntimeConstraintWiringReport.Binding> getBindings(PlanNodeId scanId)
    {
        return scanBindings.stream()
                .filter(binding -> binding.scanId().equals(scanId))
                .map(binding -> new RuntimeConstraintWiringReport.Binding(binding.request(), binding.column()))
                .distinct()
                .toList();
    }

    public record ScanBinding(PlanNodeId scanId, ColumnHandle column, RuntimeConstraintRequest request)
    {
        public ScanBinding
        {
            requireNonNull(scanId, "scanId is null");
            requireNonNull(column, "column is null");
            requireNonNull(request, "request is null");
        }
    }

    public record StoppedRequest(String operatorType, RuntimeConstraintRequest request)
    {
        public StoppedRequest
        {
            requireNonNull(operatorType, "operatorType is null");
            requireNonNull(request, "request is null");
        }
    }

    private record ScanRegistration(List<ColumnHandle> columns, Consumer<DynamicFilter> installer)
    {
        private ScanRegistration
        {
            columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            requireNonNull(installer, "installer is null");
        }
    }
}
