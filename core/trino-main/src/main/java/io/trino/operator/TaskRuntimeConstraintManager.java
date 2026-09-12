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
import com.google.common.collect.ImmutableMap;
import io.trino.execution.TaskId;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.ProducerBindingId;
import io.trino.sql.planner.runtimeconstraint.ProducerGroupId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintBatchCollector;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintBatchCollector.Batch;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContribution;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContributionBatch;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintDerivation;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintDeriver;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintHub;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintKind;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintLane;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPayload;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProducerGroup;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProtocol;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSnapshot;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintSubscriptions;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintTransform;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintUpdateBatch;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload.Lane;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.EQUIVALENT_REPLICAS;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.DISABLED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.FINAL;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.PENDING;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public final class TaskRuntimeConstraintManager
        implements AutoCloseable
{
    private static final long MAX_CONTRIBUTION_BATCH_RETAINED_BYTES = 1 << 20;
    private static final int MAX_CONTRIBUTIONS_PER_BATCH = 128;

    private final TaskId taskId;
    private final long generation;
    private final Map<ProducerBindingId, ProducerGroupId> producerBindings = new HashMap<>();
    private final Map<ProducerGroupId, RuntimeConstraintProducerGroup> localProducerGroups = new HashMap<>();
    private final Set<RuntimeConstraintId> consumerIds;
    private final LocalMemoryContext memoryContext;
    private final Runnable notifyStatusChanged;
    private final RuntimeConstraintBatchCollector<ContributionKey, RuntimeConstraintContribution> contributions;
    private final Map<RuntimeConstraintId, RuntimeConstraintSnapshot> snapshots = new HashMap<>();
    private final Set<RuntimeConstraintId> locallyPublishedIds = new HashSet<>();
    private final Set<RuntimeConstraintRequest> runtimeConstraintWiringRequests = new HashSet<>();
    private final Map<RuntimeConstraintId, List<CompletableFuture<RuntimeConstraintSnapshot>>> waiters = new HashMap<>();

    private final List<RuntimeConstraintSubscriptions> subscriptionGraphs = new ArrayList<>();
    private long subscriptionRetainedBytes;

    private long updateAcknowledgement;
    private long snapshotRetainedBytes;
    private boolean consumerStateClosed;
    private boolean consumerStateReleased;
    private boolean closed;
    private Consumer<List<RuntimeConstraintRequest>> runtimeConstraintWiring;

    public TaskRuntimeConstraintManager(
            TaskId taskId,
            long generation,
            LocalMemoryContext memoryContext,
            Runnable notifyStatusChanged)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        checkArgument(generation >= 0, "generation is negative");
        this.generation = generation;
        this.consumerIds = new HashSet<>();
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.notifyStatusChanged = requireNonNull(notifyStatusChanged, "notifyStatusChanged is null");
        this.contributions = new RuntimeConstraintBatchCollector<>(generation, ContributionKey::new);
    }

    public synchronized long getGeneration()
    {
        return generation;
    }

    public synchronized long getUpdateAcknowledgement()
    {
        return updateAcknowledgement;
    }

    public synchronized long getContributionSequence()
    {
        return contributions.getSequence();
    }

    public synchronized long getRetainedBytes()
    {
        return snapshotRetainedBytes + contributions.getRetainedBytes() + subscriptionRetainedBytes;
    }

    public synchronized Map<RuntimeConstraintId, RuntimeConstraintSnapshot> getSnapshots()
    {
        return ImmutableMap.copyOf(snapshots);
    }

    public synchronized Optional<RuntimeConstraintSnapshot> getSnapshot(RuntimeConstraintId constraintId)
    {
        return Optional.ofNullable(snapshots.get(requireNonNull(constraintId, "constraintId is null")));
    }

    public TaskId taskId()
    {
        return taskId;
    }

    public synchronized void registerConsumer(RuntimeConstraintId id)
    {
        if (consumerIds.add(id)) {
            snapshots.put(id, consumerStateClosed
                    ? RuntimeConstraintSnapshot.terminal(id, generation, 1, CLOSED)
                    : RuntimeConstraintSnapshot.pending(id, generation));
        }
    }

    public RuntimeConstraintSubscriptions createSubscriptions(RuntimeConstraintTransform.Context context, long maxRetainedBytesPerConstraint)
    {
        RuntimeConstraintSubscriptions graph = new RuntimeConstraintSubscriptions(generation, maxRetainedBytesPerConstraint, context, this::reserveSubscriptionMemory, null);
        boolean finished;
        synchronized (this) {
            finished = consumerStateClosed;
            if (!finished) {
                subscriptionGraphs.add(graph);
            }
        }
        if (finished) {
            graph.close();
        }
        return graph;
    }

    private synchronized boolean reserveSubscriptionMemory(long delta)
    {
        if (delta == 0) {
            return true;
        }
        if (delta > 0 && consumerStateClosed) {
            return false;
        }
        long retained = subscriptionRetainedBytes + delta;
        checkArgument(retained >= 0, "negative subscription memory");
        if (!trySetMemoryBytes(snapshotRetainedBytes + contributions.getRetainedBytes() + retained)) {
            return false;
        }
        subscriptionRetainedBytes = retained;
        return true;
    }

    public synchronized void registerConsumers(List<RuntimeConstraintWiringReport.Binding> bindings)
    {
        for (RuntimeConstraintWiringReport.Binding binding : bindings) {
            registerConsumer(binding.constraintId());
        }
    }

    public synchronized void registerSource(RuntimeConstraintWiringReport.Source source)
    {
        requireNonNull(source, "source is null");
        ProducerGroupId groupId = RuntimeConstraintHub.dynamicGroupId(source.sourceId());
        ProducerBindingId bindingId = RuntimeConstraintHub.dynamicBindingId(source.sourceId());
        RuntimeConstraintProducerGroup group = new RuntimeConstraintProducerGroup(
                groupId,
                new PlanFragmentId(Integer.toString(taskId.stageId().id())),
                source.sourceId(),
                source.completionPolicy(),
                IntStream.range(0, source.types().size())
                        .mapToObj(index -> new RuntimeConstraintLane(index, source.types().get(index)))
                        .toList(),
                source.constraints().stream()
                        .map(constraint -> new RuntimeConstraintDerivation(
                                constraint.constraintId(),
                                RuntimeConstraintKind.MEMBERSHIP,
                                ImmutableList.of(constraint.collectedLaneIndex()),
                                constraint.operator(),
                                constraint.nullAllowed()))
                        .toList(),
                source.replicated());
        RuntimeConstraintProducerGroup previous = localProducerGroups.putIfAbsent(groupId, group);
        checkArgument(previous == null || previous.equals(group), "runtime constraint source was registered with a different contract: %s", source.sourceId());
        producerBindings.putIfAbsent(bindingId, groupId);
    }

    public void registerRuntimeConstraintWiring(Consumer<List<RuntimeConstraintRequest>> wiring)
    {
        List<RuntimeConstraintRequest> pending;
        synchronized (this) {
            checkState(runtimeConstraintWiring == null, "runtime constraint output wiring is already registered");
            runtimeConstraintWiring = requireNonNull(wiring, "wiring is null");
            pending = ImmutableList.copyOf(runtimeConstraintWiringRequests);
        }
        if (!pending.isEmpty()) {
            wiring.accept(pending);
        }
    }

    public void addRuntimeConstraintWiringRequests(List<RuntimeConstraintRequest> requests)
    {
        Consumer<List<RuntimeConstraintRequest>> wiring;
        List<RuntimeConstraintRequest> added;
        synchronized (this) {
            if (closed) {
                return;
            }
            added = requests.stream()
                    .filter(runtimeConstraintWiringRequests::add)
                    .toList();
            wiring = runtimeConstraintWiring;
        }
        if (wiring != null && !added.isEmpty()) {
            wiring.accept(added);
            notifyStatusChanged.run();
        }
    }

    public synchronized CompletableFuture<RuntimeConstraintSnapshot> waitForUpdate(RuntimeConstraintId constraintId, long afterVersion)
    {
        RuntimeConstraintSnapshot snapshot = requireNonNull(snapshots.get(requireNonNull(constraintId, "constraintId is null")), "constraint is not bound to task");
        if (snapshot.version() > afterVersion || snapshot.state() != PENDING) {
            return CompletableFuture.completedFuture(snapshot);
        }
        CompletableFuture<RuntimeConstraintSnapshot> future = new CompletableFuture<>();
        waiters.computeIfAbsent(constraintId, _ -> new ArrayList<>()).add(future);
        return future;
    }

    public boolean applyUpdates(RuntimeConstraintUpdateBatch batch)
    {
        requireNonNull(batch, "batch is null");
        List<CompletedWaiters> completedWaiters = new ArrayList<>();
        boolean changed = false;
        synchronized (this) {
            if (closed || consumerStateClosed || batch.generation() != generation || batch.sequence() <= updateAcknowledgement) {
                return false;
            }
            checkArgument(batch.formatVersion() == RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, "unsupported runtime constraint update version: %s", batch.formatVersion());
            for (RuntimeConstraintSnapshot snapshot : batch.snapshots()) {
                if (!consumerIds.contains(snapshot.constraintId())) {
                    continue;
                }
                RuntimeConstraintSnapshot current = snapshots.get(snapshot.constraintId());
                if (snapshot.version() < current.version()) {
                    continue;
                }
                if (snapshot.version() == current.version()) {
                    if (locallyPublishedIds.contains(snapshot.constraintId())) {
                        continue;
                    }
                    checkArgument(snapshot.equals(current), "conflicting runtime constraint snapshot version: %s", snapshot.constraintId());
                    continue;
                }
                checkState(current.state() == PENDING, "runtime constraint is already terminal: %s", snapshot.constraintId());
                checkArgument(snapshot.state() != PENDING, "transported runtime constraint snapshot is pending");
                snapshot = admitSnapshot(current, snapshot);
                snapshots.put(snapshot.constraintId(), snapshot);
                List<CompletableFuture<RuntimeConstraintSnapshot>> listeners = waiters.remove(snapshot.constraintId());
                if (listeners != null) {
                    completedWaiters.add(new CompletedWaiters(listeners, snapshot));
                }
                changed = true;
            }
            updateAcknowledgement = batch.sequence();
        }
        completedWaiters.forEach(CompletedWaiters::complete);
        notifyStatusChanged.run();
        return changed;
    }

    public boolean addContribution(ProducerBindingId bindingId, RuntimeConstraintPayload payload)
    {
        ProducerGroupId groupId;
        synchronized (this) {
            groupId = requireNonNull(producerBindings.get(requireNonNull(bindingId, "bindingId is null")), "producer binding is not registered");
        }
        return addContribution(groupId, bindingId, payload);
    }

    public boolean addContribution(PlanNodeId sourceId, RuntimeConstraintPayload payload)
    {
        return addContribution(
                RuntimeConstraintHub.dynamicGroupId(sourceId),
                RuntimeConstraintHub.dynamicBindingId(sourceId),
                payload);
    }

    public boolean addUnrestrictedContribution(PlanNodeId sourceId, Type type)
    {
        requireNonNull(type, "type is null");
        synchronized (this) {
            if (closed) {
                return false;
            }
        }
        return addContribution(
                sourceId,
                new RuntimeMembershipPayload(ImmutableList.of(Domain.all(type)), ORDINARY));
    }

    private boolean addContribution(ProducerGroupId groupId, ProducerBindingId bindingId, RuntimeConstraintPayload payload)
    {
        List<CompletedWaiters> completedWaiters = new ArrayList<>();
        boolean changed;
        synchronized (this) {
            checkState(!closed, "runtime constraint manager is closed");
            RuntimeConstraintContribution contribution = new RuntimeConstraintContribution(
                    groupId,
                    bindingId,
                    taskId.partitionId(),
                    taskId.attemptId(),
                    1,
                    requireNonNull(payload, "payload is null"));
            changed = contributions.update(contribution, contributionRetainedBytes(payload));
            if (changed) {
                if (!tryUpdateMemoryReservation()) {
                    payload = unrestricted(payload);
                    contribution = new RuntimeConstraintContribution(
                            groupId,
                            bindingId,
                            taskId.partitionId(),
                            taskId.attemptId(),
                            1,
                            payload);
                    contributions.update(contribution, 0);
                    checkState(tryUpdateMemoryReservation(), "failed to reserve memory for disabled runtime constraint contribution");
                }
                RuntimeConstraintProducerGroup localGroup = consumerStateClosed ? null : localProducerGroups.get(groupId);
                if (localGroup != null && localGroup.completionPolicy() != EQUIVALENT_REPLICAS) {
                    localGroup = null;
                }
                if (localGroup != null) {
                    RuntimeConstraintDeriver.derive(localGroup, List.of(payload)).forEach((constraintId, derived) -> {
                        if (!consumerIds.contains(constraintId)) {
                            return;
                        }
                        RuntimeConstraintSnapshot current = snapshots.get(constraintId);
                        if (current.state() == FINAL || current.state() == CLOSED) {
                            return;
                        }
                        checkState(current.state() == PENDING || current.state() == DISABLED, "unexpected runtime constraint state: %s", constraintId);
                        RuntimeConstraintSnapshot snapshot = derived.scalarDomains().stream().allMatch(Domain::isAll)
                                ? RuntimeConstraintSnapshot.terminal(constraintId, generation, current.version() + 1, DISABLED)
                                : RuntimeConstraintSnapshot.finalSnapshot(constraintId, generation, current.version() + 1, derived);
                        if (current.state() == DISABLED && snapshot.state() == DISABLED) {
                            locallyPublishedIds.add(constraintId);
                            return;
                        }
                        snapshot = admitSnapshot(current, snapshot);
                        snapshots.put(constraintId, snapshot);
                        locallyPublishedIds.add(constraintId);
                        List<CompletableFuture<RuntimeConstraintSnapshot>> listeners = waiters.remove(constraintId);
                        if (listeners != null) {
                            completedWaiters.add(new CompletedWaiters(listeners, snapshot));
                        }
                    });
                }
            }
        }
        completedWaiters.forEach(CompletedWaiters::complete);
        if (changed) {
            notifyStatusChanged.run();
        }
        return changed;
    }

    public synchronized RuntimeConstraintContributionBatch acknowledgeContributionsAndGetBatch(long acknowledgedSequence)
    {
        if (closed) {
            return new RuntimeConstraintContributionBatch(
                    RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                    min(acknowledgedSequence, contributions.getSequence()),
                    generation,
                    ImmutableList.of());
        }
        acknowledgeContributions(acknowledgedSequence);
        Batch<RuntimeConstraintContribution> batch = contributions.getPendingBatch(
                MAX_CONTRIBUTION_BATCH_RETAINED_BYTES,
                MAX_CONTRIBUTIONS_PER_BATCH);
        long responseSequence = batch.values().isEmpty()
                ? min(acknowledgedSequence, contributions.getSequence())
                : batch.sequence();
        return new RuntimeConstraintContributionBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                responseSequence,
                generation,
                batch.values());
    }

    public synchronized void acknowledgeContributions(long acknowledgedSequence)
    {
        if (closed) {
            return;
        }
        contributions.acknowledge(acknowledgedSequence);
        updateMemoryReservation();
    }

    public void taskFinished()
    {
        Map<RuntimeConstraintId, List<CompletableFuture<RuntimeConstraintSnapshot>>> listeners;
        Map<RuntimeConstraintId, RuntimeConstraintSnapshot> closedSnapshots = new HashMap<>();
        List<RuntimeConstraintSubscriptions> graphs;
        synchronized (this) {
            if (consumerStateClosed) {
                return;
            }
            consumerStateClosed = true;
            graphs = ImmutableList.copyOf(subscriptionGraphs);
            subscriptionGraphs.clear();
            for (Map.Entry<RuntimeConstraintId, RuntimeConstraintSnapshot> entry : snapshots.entrySet()) {
                RuntimeConstraintSnapshot current = entry.getValue();
                RuntimeConstraintSnapshot closedSnapshot = RuntimeConstraintSnapshot.terminal(
                        entry.getKey(),
                        generation,
                        current.version() + 1,
                        CLOSED);
                entry.setValue(closedSnapshot);
                closedSnapshots.put(entry.getKey(), closedSnapshot);
            }
            snapshotRetainedBytes = 0;
            updateMemoryReservation();
            listeners = new HashMap<>(waiters);
            waiters.clear();
        }
        graphs.forEach(RuntimeConstraintSubscriptions::close);
        listeners.forEach((id, futures) -> new CompletedWaiters(futures, closedSnapshots.get(id)).complete());
        synchronized (this) {
            consumerStateReleased = true;
            if (closed) {
                memoryContext.close();
            }
        }
    }

    @Override
    public void close()
    {
        taskFinished();
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            contributions.acknowledge(Long.MAX_VALUE);
            // A concurrent or reentrant taskFinished() can still be releasing graph memory.
            updateMemoryReservation();
            if (consumerStateReleased) {
                memoryContext.close();
            }
        }
    }

    private void updateMemoryReservation()
    {
        memoryContext.setBytes(snapshotRetainedBytes + contributions.getRetainedBytes() + subscriptionRetainedBytes);
    }

    private RuntimeConstraintSnapshot admitSnapshot(RuntimeConstraintSnapshot current, RuntimeConstraintSnapshot candidate)
    {
        long candidateRetainedBytes = snapshotRetainedBytes + candidate.retainedSizeInBytes() - current.retainedSizeInBytes();
        long totalRetainedBytes = candidateRetainedBytes + contributions.getRetainedBytes() + subscriptionRetainedBytes;
        if (trySetMemoryBytes(totalRetainedBytes)) {
            snapshotRetainedBytes = candidateRetainedBytes;
            return candidate;
        }
        checkState(candidate.state() == FINAL, "failed to reserve memory for terminal runtime constraint snapshot");
        RuntimeConstraintSnapshot disabled = RuntimeConstraintSnapshot.terminal(
                candidate.constraintId(),
                candidate.generation(),
                candidate.version(),
                DISABLED);
        long disabledRetainedBytes = snapshotRetainedBytes - current.retainedSizeInBytes();
        checkState(trySetMemoryBytes(disabledRetainedBytes + contributions.getRetainedBytes() + subscriptionRetainedBytes), "failed to reserve memory for disabled runtime constraint snapshot");
        snapshotRetainedBytes = disabledRetainedBytes;
        return disabled;
    }

    private boolean tryUpdateMemoryReservation()
    {
        return trySetMemoryBytes(snapshotRetainedBytes + contributions.getRetainedBytes() + subscriptionRetainedBytes);
    }

    private boolean trySetMemoryBytes(long bytes)
    {
        return memoryContext.getBytes() == bytes || memoryContext.trySetBytes(bytes);
    }

    private static RuntimeConstraintPayload unrestricted(RuntimeConstraintPayload payload)
    {
        checkArgument(payload instanceof RuntimeMembershipPayload, "unsupported runtime constraint payload: %s", payload.getClass().getSimpleName());
        RuntimeMembershipPayload membership = (RuntimeMembershipPayload) payload;
        return new RuntimeMembershipPayload(
                membership.lanes().stream()
                        .map(lane -> new Lane(Domain.all(lane.domain().getType()), lane.sawNull()))
                        .toList(),
                membership.nullMatchMode(),
                membership.sawInputRow());
    }

    private static long contributionRetainedBytes(RuntimeConstraintPayload payload)
    {
        if (payload instanceof RuntimeMembershipPayload membership && membership.scalarDomains().stream().allMatch(Domain::isAll)) {
            return 0;
        }
        return payload.getRetainedSizeInBytes();
    }

    private record ContributionKey(ProducerGroupId groupId, ProducerBindingId bindingId, int logicalPartitionId, int taskAttemptId)
    {
        private ContributionKey(RuntimeConstraintContribution contribution)
        {
            this(contribution.groupId(),
                    contribution.bindingId(),
                    contribution.logicalPartitionId(),
                    contribution.taskAttemptId());
        }
    }

    private record CompletedWaiters(List<CompletableFuture<RuntimeConstraintSnapshot>> waiters, RuntimeConstraintSnapshot snapshot)
    {
        private void complete()
        {
            waiters.forEach(waiter -> waiter.complete(snapshot));
        }
    }
}
