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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.execution.TaskId;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload.Lane;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.EQUIVALENT_REPLICAS;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.SINGLE;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProducerGroupState.CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProducerGroupState.COLLECTING;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProducerGroupState.DISABLED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProducerGroupState.FINAL;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProducerGroupState.REGISTERED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProducerGroupState.SEALED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintPublicationState.PENDING;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintUpdateResult.APPLIED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintUpdateResult.DUPLICATE;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintUpdateResult.HUB_CLOSED;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintUpdateResult.STALE_GENERATION;
import static java.util.Objects.requireNonNull;

public final class RuntimeConstraintHub
        implements AutoCloseable
{
    private final long generation;
    private final long maxRetainedBytes;
    private final boolean taskRetriesEnabled;
    private final int queryAttempt;
    private final Set<RuntimeConstraintId> lazyConstraintIds;
    private final Map<RuntimeConstraintId, GroupContext> constraintGroups;
    private final Map<RuntimeConstraintId, RuntimeConstraintSnapshot> snapshots = new HashMap<>();
    private final Map<ContributionKey, RuntimeConstraintContribution> contributions = new HashMap<>();
    private final Map<ProducerBindingId, Map<ContributionKey, PendingContribution>> pendingUnregisteredContributions = new HashMap<>();
    private final Map<ProducerBindingId, Long> pendingUnregisteredContributionBytes = new HashMap<>();
    private final Set<ProducerBindingId> overLimitUnregisteredBindings = new HashSet<>();
    private final Map<PlanFragmentId, Set<Integer>> sealedProducerStages = new HashMap<>();
    private final Set<TaskId> failedTasks = new HashSet<>();
    private final Map<TaskId, Long> acceptedContributionSequences = new HashMap<>();
    private final Map<TaskId, Long> successfulTaskFinalSequences = new HashMap<>();
    private final Map<ProducerBindingId, ProducerGroupId> producerBindings;
    private final Map<ProducerGroupId, GroupContext> groups;
    private final Map<RuntimeConstraintId, List<CompletableFuture<RuntimeConstraintSnapshot>>> waiters = new HashMap<>();
    private final Map<RuntimeConstraintId, List<CompletableFuture<Void>>> initialUnblockWaiters = new HashMap<>();
    private final Set<RuntimeConstraintId> initiallyUnblocked = new HashSet<>();
    private final Set<PlanFragmentId> initiallyUnblockedFragments = new HashSet<>();
    private final boolean sourceRegistrationBarrierEnabled;

    private boolean closed;
    private long retainedBytes;

    public RuntimeConstraintHub(long generation, long maxRetainedBytes)
    {
        this(generation, maxRetainedBytes, false, 0, false);
    }

    public RuntimeConstraintHub(long generation, long maxRetainedBytes, boolean taskRetriesEnabled)
    {
        this(generation, maxRetainedBytes, taskRetriesEnabled, 0, false);
    }

    public RuntimeConstraintHub(long generation, long maxRetainedBytes, boolean taskRetriesEnabled, boolean sourceRegistrationBarrierEnabled)
    {
        this(generation, maxRetainedBytes, taskRetriesEnabled, 0, sourceRegistrationBarrierEnabled);
    }

    private RuntimeConstraintHub(long generation, long maxRetainedBytes, boolean taskRetriesEnabled, int queryAttempt, boolean sourceRegistrationBarrierEnabled)
    {
        checkArgument(generation >= 0, "generation is negative");
        checkArgument(maxRetainedBytes >= 0, "maxRetainedBytes is negative");
        checkArgument(queryAttempt >= 0, "queryAttempt is negative");
        this.generation = generation;
        this.maxRetainedBytes = maxRetainedBytes;
        this.taskRetriesEnabled = taskRetriesEnabled;
        this.queryAttempt = queryAttempt;
        this.sourceRegistrationBarrierEnabled = sourceRegistrationBarrierEnabled;
        this.producerBindings = new HashMap<>();
        this.groups = new HashMap<>();
        this.constraintGroups = new HashMap<>();
        this.lazyConstraintIds = new HashSet<>();
    }

    public void registerSource(TaskId taskId, RuntimeConstraintWiringReport.Source source)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(source, "source is null");
        Set<Integer> sealedPartitions;
        List<CompletableFuture<Void>> unblockedWaiters = new ArrayList<>();
        List<Completion> completions = new ArrayList<>();
        PlanFragmentId sourceFragmentId = fragmentId(taskId);
        synchronized (this) {
            ProducerGroupId groupId = dynamicGroupId(source.sourceId());
            ProducerBindingId bindingId = dynamicBindingId(source.sourceId());
            GroupContext routedGroup = source.constraintIds().stream()
                    .map(constraintGroups::get)
                    .filter(group -> group != null)
                    .findFirst()
                    .orElse(null);
            if (routedGroup != null) {
                checkArgument(source.constraintIds().stream().allMatch(id -> constraintGroups.get(id) == routedGroup), "runtime constraint source spans multiple producer groups: %s", source.sourceId());
                producerBindings.put(bindingId, routedGroup.spec.groupId());
                routedGroup.sourceFragments.add(fragmentId(taskId));
            }
            else {
                RuntimeConstraintProducerGroup spec = new RuntimeConstraintProducerGroup(
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
                GroupContext existing = groups.get(groupId);
                if (existing != null) {
                    checkArgument(existing.spec.equals(spec), "runtime constraint source was registered with a different contract: %s", source.sourceId());
                }
                else {
                    GroupContext group = new GroupContext(spec);
                    group.sourceFragments.add(fragmentId(taskId));
                    groups.put(groupId, group);
                    spec.derivations().forEach(derivation -> {
                        constraintGroups.put(derivation.constraintId(), group);
                        snapshots.putIfAbsent(derivation.constraintId(), RuntimeConstraintSnapshot.pending(derivation.constraintId(), generation));
                        if (initiallyUnblockedFragments.contains(spec.originFragmentId())) {
                            initiallyUnblocked.add(derivation.constraintId());
                            List<CompletableFuture<Void>> listeners = initialUnblockWaiters.remove(derivation.constraintId());
                            if (listeners != null) {
                                unblockedWaiters.addAll(listeners);
                            }
                        }
                    });
                }
                producerBindings.put(bindingId, groupId);
            }
            GroupContext group = requireGroup(producerBindings.get(bindingId));
            group.sourceTasks.add(taskId);
            Map<ContributionKey, PendingContribution> pendingForBinding = removePendingUnregisteredContributionsLocked(bindingId);
            if (overLimitUnregisteredBindings.remove(bindingId) && group.state != FINAL && group.state != DISABLED && group.state != CLOSED) {
                disableGroupLocked(group, completions);
            }
            else if (pendingForBinding != null) {
                pendingForBinding.values().forEach(contribution -> acceptRegisteredContributionLocked(
                        contribution.taskId(),
                        contribution.contribution(),
                        completions));
            }
            successfulTaskFinalSequences.entrySet().stream()
                    .filter(entry -> fragmentId(entry.getKey()).equals(sourceFragmentId))
                    .filter(entry -> acceptedContributionSequences.getOrDefault(entry.getKey(), 0L) >= entry.getValue())
                    .map(Map.Entry::getKey)
                    .toList()
                    .forEach(successfulTask -> finishTaskLocked(successfulTask, completions));
            sealedPartitions = sealedProducerStages.get(sourceFragmentId);
        }
        complete(completions);
        unblockedWaiters.forEach(waiter -> waiter.complete(null));
        if (sealedPartitions != null) {
            sealProducerStage(sourceFragmentId, sealedPartitions);
        }
    }

    public synchronized void registerConsumer(RuntimeConstraintId constraintId)
    {
        lazyConstraintIds.add(constraintId);
        snapshots.putIfAbsent(constraintId, RuntimeConstraintSnapshot.pending(constraintId, generation));
    }

    public synchronized void registerConsumers(List<RuntimeConstraintWiringReport.Binding> bindings)
    {
        bindings.stream()
                .map(RuntimeConstraintWiringReport.Binding::constraintId)
                .forEach(constraintId -> {
                    lazyConstraintIds.add(constraintId);
                    snapshots.putIfAbsent(constraintId, RuntimeConstraintSnapshot.pending(constraintId, generation));
                });
    }

    public static ProducerGroupId dynamicGroupId(PlanNodeId sourceId)
    {
        return new ProducerGroupId("operator_" + sourceId);
    }

    public static ProducerBindingId dynamicBindingId(PlanNodeId sourceId)
    {
        return new ProducerBindingId("operator_" + sourceId);
    }

    public RuntimeConstraintHub forQueryRetry(int queryAttempt)
    {
        checkArgument(!taskRetriesEnabled, "query retry is incompatible with task retries");
        checkArgument(queryAttempt == this.queryAttempt + 1, "query attempt is not consecutive");
        return new RuntimeConstraintHub(generation + 1, maxRetainedBytes, false, queryAttempt, sourceRegistrationBarrierEnabled);
    }

    public int getQueryAttempt()
    {
        return queryAttempt;
    }

    public boolean acceptsTaskAttempt(int taskAttempt)
    {
        return taskAttempt >= queryAttempt && (taskRetriesEnabled || taskAttempt == queryAttempt);
    }

    public synchronized RuntimeConstraintSnapshot get(RuntimeConstraintId constraintId)
    {
        return requireConstraint(constraintId);
    }

    public synchronized Map<RuntimeConstraintId, RuntimeConstraintSnapshot> getAll()
    {
        return ImmutableMap.copyOf(snapshots);
    }

    public synchronized List<ConstraintStatistics> getStatistics()
    {
        ImmutableList.Builder<ConstraintStatistics> statistics = ImmutableList.builder();
        for (GroupContext group : groups.values()) {
            for (int lane = 0; lane < group.spec.collectedLanes().size(); lane++) {
                int collectedLane = lane;
                List<RuntimeConstraintDerivation> derivations = group.spec.derivations().stream()
                        .filter(derivation -> derivation.collectedLaneIndexes().contains(collectedLane))
                        .toList();
                if (derivations.isEmpty()) {
                    continue;
                }
                List<RuntimeConstraintSnapshot> derivedSnapshots = derivations.stream()
                        .map(RuntimeConstraintDerivation::constraintId)
                        .map(snapshots::get)
                        .toList();
                RuntimeConstraintPublicationState state = derivedSnapshots.stream().anyMatch(snapshot -> snapshot.state() == RuntimeConstraintPublicationState.PENDING)
                        ? RuntimeConstraintPublicationState.PENDING
                        : derivedSnapshots.stream().anyMatch(snapshot -> snapshot.state() == RuntimeConstraintPublicationState.FINAL)
                          ? RuntimeConstraintPublicationState.FINAL
                          : derivedSnapshots.getFirst().state();
                Optional<Domain> summary = compatibilitySummary(group, lane, derivations, derivedSnapshots);
                statistics.add(new ConstraintStatistics(
                        derivations.getFirst().constraintId(),
                        state,
                        summary,
                        derivations.stream().map(RuntimeConstraintDerivation::constraintId).anyMatch(lazyConstraintIds::contains),
                        group.spec.replicated(),
                        Optional.ofNullable(group.collectionDuration)));
            }
        }
        return statistics.build();
    }

    private Optional<Domain> compatibilitySummary(
            GroupContext group,
            int lane,
            List<RuntimeConstraintDerivation> derivations,
            List<RuntimeConstraintSnapshot> derivedSnapshots)
    {
        if (group.compatibilitySummaries != null) {
            return Optional.of(group.compatibilitySummaries.get(lane));
        }
        if (derivedSnapshots.stream().allMatch(snapshot -> snapshot.state() == RuntimeConstraintPublicationState.DISABLED)) {
            return Optional.of(Domain.all(group.spec.collectedLanes().get(lane).type()));
        }
        if (derivations.size() != 1) {
            return Optional.empty();
        }
        RuntimeConstraintSnapshot snapshot = derivedSnapshots.getFirst();
        if (snapshot.state() == RuntimeConstraintPublicationState.FINAL && snapshot.payload().orElseThrow() instanceof RuntimeMembershipPayload payload && payload.scalarDomains().size() == 1) {
            return Optional.of(payload.scalarDomains().getFirst());
        }
        if (snapshot.state() == RuntimeConstraintPublicationState.DISABLED) {
            return Optional.of(Domain.all(group.spec.collectedLanes().get(lane).type()));
        }
        return Optional.empty();
    }

    public synchronized long getRetainedBytes()
    {
        return retainedBytes;
    }

    public synchronized Map<ContributionKey, RuntimeConstraintContribution> getContributions()
    {
        return ImmutableMap.copyOf(contributions);
    }

    public synchronized boolean isSourceRegistered(RuntimeConstraintId constraintId, TaskId taskId)
    {
        GroupContext group = constraintGroups.get(requireNonNull(constraintId, "constraintId is null"));
        return group != null && group.sourceTasks.contains(requireNonNull(taskId, "taskId is null"));
    }

    public synchronized boolean isSourceRegistered(RuntimeConstraintId constraintId)
    {
        GroupContext group = constraintGroups.get(requireNonNull(constraintId, "constraintId is null"));
        return group != null && !group.sourceTasks.isEmpty();
    }

    public synchronized RuntimeConstraintProducerGroupState getGroupState(ProducerGroupId groupId)
    {
        return requireGroup(groupId).state;
    }

    public boolean isCollectingTaskNeeded(PlanFragmentId fragmentId)
    {
        synchronized (this) {
            return groups.values().stream()
                    .anyMatch(group -> group.sourceFragments.contains(fragmentId) &&
                            group.spec.completionPolicy() == EQUIVALENT_REPLICAS &&
                            group.spec.derivations().stream().map(RuntimeConstraintDerivation::constraintId).anyMatch(lazyConstraintIds::contains));
        }
    }

    public boolean isStageSchedulingNeeded(PlanFragmentId fragmentId)
    {
        synchronized (this) {
            return groups.values().stream()
                    .anyMatch(group -> group.sourceFragments.contains(fragmentId) &&
                            group.spec.completionPolicy() != EQUIVALENT_REPLICAS &&
                            group.spec.derivations().stream().map(RuntimeConstraintDerivation::constraintId).anyMatch(lazyConstraintIds::contains));
        }
    }

    public boolean acceptContributions(TaskId taskId, RuntimeConstraintContributionBatch batch)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(batch, "batch is null");
        List<Completion> completions = new ArrayList<>();
        boolean changed = false;
        synchronized (this) {
            if (closed || failedTasks.contains(taskId) || !acceptsTaskAttempt(taskId.attemptId()) || batch.generation() != generation) {
                return false;
            }
            checkArgument(batch.formatVersion() == RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, "unsupported runtime constraint contribution version: %s", batch.formatVersion());
            if (taskRetriesEnabled) {
                acceptedContributionSequences.merge(taskId, batch.sequence(), Math::max);
            }
            for (RuntimeConstraintContribution contribution : batch.contributions()) {
                checkArgument(contribution.logicalPartitionId() == taskId.partitionId(), "contribution logical partition does not match task");
                checkArgument(contribution.taskAttemptId() == taskId.attemptId(), "contribution attempt does not match task");
                ProducerGroupId registeredGroupId = producerBindings.get(contribution.bindingId());
                if (registeredGroupId == null) {
                    if (overLimitUnregisteredBindings.contains(contribution.bindingId())) {
                        continue;
                    }
                    PendingContribution pending = new PendingContribution(taskId, contribution);
                    Map<ContributionKey, PendingContribution> pendingForBinding = pendingUnregisteredContributions
                            .computeIfAbsent(contribution.bindingId(), _ -> new HashMap<>());
                    ContributionKey key = ContributionKey.from(taskId, contribution);
                    PendingContribution existing = pendingForBinding.get(key);
                    checkArgument(existing == null || existing.contribution().equals(contribution), "conflicting contribution for unregistered producer binding: %s", contribution.bindingId());
                    if (existing != null) {
                        continue;
                    }
                    long contributionBytes = contribution.payload().getRetainedSizeInBytes();
                    long pendingBytes = pendingUnregisteredContributionBytes.getOrDefault(contribution.bindingId(), 0L);
                    if (contributionBytes > maxRetainedBytes - pendingBytes) {
                        removePendingUnregisteredContributionsLocked(contribution.bindingId());
                        overLimitUnregisteredBindings.add(contribution.bindingId());
                        changed = true;
                        continue;
                    }
                    pendingForBinding.put(key, pending);
                    pendingUnregisteredContributionBytes.put(contribution.bindingId(), pendingBytes + contributionBytes);
                    retainedBytes += contributionBytes;
                    changed = true;
                    continue;
                }
                changed |= acceptRegisteredContributionLocked(taskId, contribution, completions);
            }
            if (taskRetriesEnabled) {
                Long finalSequence = successfulTaskFinalSequences.get(taskId);
                if (finalSequence != null && acceptedContributionSequences.getOrDefault(taskId, 0L) >= finalSequence) {
                    finishTaskLocked(taskId, completions);
                }
            }
        }
        complete(completions);
        return changed;
    }

    private boolean acceptRegisteredContributionLocked(TaskId taskId, RuntimeConstraintContribution contribution, List<Completion> completions)
    {
        ProducerGroupId registeredGroupId = producerBindings.get(contribution.bindingId());
        checkState(registeredGroupId != null, "producer binding is not registered: %s", contribution.bindingId());
        if (!contribution.groupId().equals(registeredGroupId)) {
            contribution = new RuntimeConstraintContribution(
                    registeredGroupId,
                    contribution.bindingId(),
                    contribution.logicalPartitionId(),
                    contribution.taskAttemptId(),
                    contribution.version(),
                    contribution.payload());
        }
        GroupContext group = requireGroup(contribution.groupId());
        if (group.state == FINAL || group.state == DISABLED || group.state == CLOSED) {
            return false;
        }
        ProducerPartition producerPartition = new ProducerPartition(fragmentId(taskId), contribution.logicalPartitionId());
        Set<Integer> expectedPartitions = group.expectedPartitions.get(producerPartition.fragmentId());
        if (expectedPartitions != null && !expectedPartitions.contains(contribution.logicalPartitionId())) {
            return false;
        }
        Integer successfulAttempt = group.successfulAttempts.get(producerPartition);
        if (successfulAttempt != null && successfulAttempt != contribution.taskAttemptId()) {
            return false;
        }
        ContributionKey key = ContributionKey.from(taskId, contribution);
        RuntimeConstraintContribution current = contributions.get(key);
        if (current != null) {
            checkArgument(current.equals(contribution), "conflicting contribution for producer binding: %s", contribution.bindingId());
            return false;
        }
        if (group.committedPartitions.contains(producerPartition)) {
            return false;
        }
        long contributionBytes = contribution.payload().getRetainedSizeInBytes();
        if (taskRetriesEnabled && successfulAttempt == null && contributionBytes > maxRetainedBytes - group.pendingContributionBytes) {
            disableGroupLocked(group, completions);
            return true;
        }
        contributions.put(key, contribution);
        retainedBytes += contributionBytes;
        group.pendingContributionBytes += contributionBytes;
        group.state = group.state == REGISTERED ? COLLECTING : group.state;
        if (!taskRetriesEnabled) {
            commitLocked(group, producerPartition, contribution, completions);
        }
        return true;
    }

    public void sealProducerStage(PlanFragmentId fragmentId, Set<Integer> logicalPartitionIds)
    {
        requireNonNull(fragmentId, "fragmentId is null");
        Set<Integer> partitions = ImmutableSet.copyOf(requireNonNull(logicalPartitionIds, "logicalPartitionIds is null"));
        checkArgument(partitions.stream().allMatch(partition -> partition >= 0), "logicalPartitionIds contains a negative partition");
        List<Completion> completions = new ArrayList<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            Set<Integer> existingPartitions = sealedProducerStages.putIfAbsent(fragmentId, partitions);
            checkArgument(existingPartitions == null || existingPartitions.equals(partitions), "producer stage sealed with a different partition set: %s", fragmentId);
            groups.values().stream()
                    .filter(group -> group.sourceFragments.contains(fragmentId))
                    .forEach(group -> {
                        Set<Integer> expectedPartitions = group.expectedPartitions.putIfAbsent(fragmentId, partitions);
                        if (expectedPartitions != null) {
                            checkArgument(expectedPartitions.equals(partitions), "producer stage sealed with a different partition set: %s", fragmentId);
                            return;
                        }
                        if (group.state == REGISTERED || group.state == COLLECTING) {
                            group.state = SEALED;
                        }
                        evaluateCompletionLocked(group, completions);
                    });
        }
        complete(completions);
    }

    public void completeSourceRegistration(Set<RuntimeConstraintId> constraintIds, Set<PlanFragmentId> sourceFragments)
    {
        requireNonNull(constraintIds, "constraintIds is null");
        Set<PlanFragmentId> expectedSourceFragments = ImmutableSet.copyOf(requireNonNull(sourceFragments, "sourceFragments is null"));
        List<Completion> completions = new ArrayList<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            constraintIds.stream()
                    .map(constraintGroups::get)
                    .filter(group -> group != null)
                    .distinct()
                    .forEach(group -> {
                        checkArgument(group.sourceFragments.equals(expectedSourceFragments), "runtime constraint producer fragments do not match registered sources for group %s: expected %s, registered %s", group.spec.groupId(), expectedSourceFragments, group.sourceFragments);
                        group.sourceRegistrationComplete = true;
                        evaluateCompletionLocked(group, completions);
                    });
        }
        complete(completions);
    }

    private record PendingContribution(TaskId taskId, RuntimeConstraintContribution contribution) {}

    public void taskFinished(TaskId taskId, boolean successful, long finalContributionSequence)
    {
        requireNonNull(taskId, "taskId is null");
        checkArgument(finalContributionSequence >= 0, "finalContributionSequence is negative");
        if (!taskRetriesEnabled) {
            return;
        }
        List<Completion> completions = new ArrayList<>();
        synchronized (this) {
            if (closed || failedTasks.contains(taskId) || !acceptsTaskAttempt(taskId.attemptId())) {
                return;
            }
            if (!successful) {
                failedTasks.add(taskId);
                successfulTaskFinalSequences.remove(taskId);
                acceptedContributionSequences.remove(taskId);
                discardPendingUnregisteredAttemptLocked(taskId);
                for (GroupContext group : groups.values()) {
                    discardAttemptLocked(group, fragmentId(taskId), taskId.partitionId(), taskId.attemptId());
                }
            }
            else {
                long finalSequence = successfulTaskFinalSequences.merge(taskId, finalContributionSequence, Math::max);
                if (acceptedContributionSequences.getOrDefault(taskId, 0L) >= finalSequence) {
                    finishTaskLocked(taskId, completions);
                }
            }
        }
        complete(completions);
    }

    private void finishTaskLocked(TaskId taskId, List<Completion> completions)
    {
        PlanFragmentId fragmentId = fragmentId(taskId);
        for (GroupContext group : groups.values()) {
            if (!group.sourceFragments.contains(fragmentId) || group.state == FINAL || group.state == DISABLED) {
                continue;
            }
            ProducerPartition producerPartition = new ProducerPartition(fragmentId, taskId.partitionId());
            Integer current = group.successfulAttempts.putIfAbsent(producerPartition, taskId.attemptId());
            if (current != null && current != taskId.attemptId()) {
                discardAttemptLocked(group, fragmentId, taskId.partitionId(), taskId.attemptId());
                continue;
            }
            discardOtherAttemptsLocked(group, fragmentId, taskId.partitionId(), taskId.attemptId());
            List<RuntimeConstraintContribution> successfulContributions = contributions.entrySet().stream()
                    .filter(entry -> entry.getKey().groupId().equals(group.spec.groupId()))
                    .filter(entry -> entry.getKey().fragmentId().equals(fragmentId))
                    .filter(entry -> entry.getKey().logicalPartitionId() == taskId.partitionId())
                    .filter(entry -> entry.getKey().taskAttemptId() == taskId.attemptId())
                    .map(Map.Entry::getValue)
                    .toList();
            if (successfulContributions.isEmpty() && !group.committedPartitions.contains(producerPartition)) {
                // A source report can arrive after task completion. Registration without
                // a contribution does not prove empty input: collection may have been
                // installed too late. Only an explicit collected NONE permits pruning.
                if (group.sourceTasks.contains(taskId)) {
                    disableGroupLocked(group, completions);
                }
            }
            else {
                successfulContributions.forEach(contribution -> commitLocked(group, producerPartition, contribution, completions));
            }
        }
    }

    public long getGeneration()
    {
        return generation;
    }

    public synchronized CompletableFuture<RuntimeConstraintSnapshot> waitForUpdate(RuntimeConstraintId constraintId, long afterVersion)
    {
        RuntimeConstraintSnapshot snapshot = requireConstraint(constraintId);
        if (snapshot.version() > afterVersion || snapshot.state() != PENDING) {
            return CompletableFuture.completedFuture(snapshot);
        }
        CompletableFuture<RuntimeConstraintSnapshot> future = new CompletableFuture<>();
        waiters.computeIfAbsent(constraintId, _ -> new ArrayList<>()).add(future);
        return future;
    }

    public synchronized CompletableFuture<Void> waitForInitialUnblock(RuntimeConstraintId constraintId)
    {
        RuntimeConstraintSnapshot snapshot = requireConstraint(constraintId);
        if (snapshot.state() != PENDING || initiallyUnblocked.contains(constraintId)) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        initialUnblockWaiters.computeIfAbsent(constraintId, _ -> new ArrayList<>()).add(future);
        return future;
    }

    public void unblockStageDynamicFilters(PlanFragmentId producerFragmentId)
    {
        requireNonNull(producerFragmentId, "producerFragmentId is null");
        List<CompletableFuture<Void>> listeners = new ArrayList<>();
        synchronized (this) {
            initiallyUnblockedFragments.add(producerFragmentId);
            groups.values().stream()
                    .filter(group -> group.spec.originFragmentId().equals(producerFragmentId))
                    .flatMap(group -> group.spec.derivations().stream())
                    .map(RuntimeConstraintDerivation::constraintId)
                    .filter(initiallyUnblocked::add)
                    .map(initialUnblockWaiters::remove)
                    .filter(listenersForConstraint -> listenersForConstraint != null)
                    .forEach(listeners::addAll);
        }
        listeners.forEach(listener -> listener.complete(null));
    }

    public RuntimeConstraintUpdateResult publishFinal(RuntimeConstraintId constraintId, long generation, RuntimeConstraintPayload payload)
    {
        requireNonNull(payload, "payload is null");
        return update(constraintId, generation, payload, false);
    }

    public RuntimeConstraintUpdateResult disable(RuntimeConstraintId constraintId, long generation)
    {
        return update(constraintId, generation, null, true);
    }

    public void disableIfPending(RuntimeConstraintId constraintId)
    {
        Completion completion;
        synchronized (this) {
            if (closed) {
                return;
            }
            RuntimeConstraintSnapshot current = snapshots.computeIfAbsent(
                    requireNonNull(constraintId, "constraintId is null"),
                    id -> RuntimeConstraintSnapshot.pending(id, generation));
            if (current.state() != PENDING) {
                return;
            }
            completion = finishConstraintLocked(constraintId, RuntimeConstraintPublicationState.DISABLED, null);
        }
        complete(List.of(completion));
    }

    private RuntimeConstraintUpdateResult update(RuntimeConstraintId constraintId, long updateGeneration, RuntimeConstraintPayload payload, boolean disable)
    {
        Completion completion;
        synchronized (this) {
            if (updateGeneration != generation) {
                return STALE_GENERATION;
            }
            if (closed) {
                return HUB_CLOSED;
            }
            RuntimeConstraintSnapshot current = requireConstraint(constraintId);
            RuntimeConstraintPayload boundedPayload = disable ? null : boundPayload(payload);
            RuntimeConstraintPublicationState targetState = disable || isUnrestricted(boundedPayload)
                    ? RuntimeConstraintPublicationState.DISABLED
                    : RuntimeConstraintPublicationState.FINAL;
            if (current.state() != PENDING) {
                boolean duplicate = current.state() == targetState &&
                        (targetState != RuntimeConstraintPublicationState.FINAL || current.payload().orElseThrow().equals(boundedPayload));
                checkState(duplicate, "constraint already completed with different state or payload: %s", constraintId);
                return DUPLICATE;
            }
            completion = finishConstraintLocked(constraintId, targetState, boundedPayload);
        }
        complete(List.of(completion));
        return APPLIED;
    }

    private void commitLocked(GroupContext group, ProducerPartition producerPartition, RuntimeConstraintContribution contribution, List<Completion> completions)
    {
        ContributionKey key = new ContributionKey(group.spec.groupId(), contribution.bindingId(), producerPartition.fragmentId(), contribution.logicalPartitionId(), contribution.taskAttemptId());
        if (!group.committedPartitions.add(producerPartition)) {
            removeContributionLocked(key);
            return;
        }
        RuntimeConstraintPayload payload = contribution.payload();
        checkArgument(payload instanceof RuntimeMembershipPayload, "unsupported membership contribution payload: %s", payload.getClass().getSimpleName());
        RuntimeMembershipPayload previous = group.aggregate;
        RuntimeMembershipPayload aggregate = mergePayloads(previous, (RuntimeMembershipPayload) payload);
        if (previous != null) {
            retainedBytes -= previous.getRetainedSizeInBytes();
        }
        group.aggregate = aggregate;
        retainedBytes += aggregate.getRetainedSizeInBytes();
        removeContributionLocked(key);
        disableUnrestrictedConstraintsLocked(group, completions);
        evaluateCompletionLocked(group, completions);
    }

    private void disableUnrestrictedConstraintsLocked(GroupContext group, List<Completion> completions)
    {
        RuntimeConstraintDeriver.derive(group.spec, ImmutableList.of(group.aggregate)).forEach((constraintId, payload) -> {
            if (isUnrestricted(payload) && snapshots.get(constraintId).state() == PENDING) {
                completions.add(finishConstraintLocked(constraintId, RuntimeConstraintPublicationState.DISABLED, null));
            }
        });
        if (group.spec.derivations().stream()
                .map(RuntimeConstraintDerivation::constraintId)
                .map(snapshots::get)
                .allMatch(snapshot -> snapshot.state() != PENDING)) {
            releaseGroupContributionsLocked(group);
            group.state = DISABLED;
        }
    }

    private void disableGroupLocked(GroupContext group, List<Completion> completions)
    {
        releaseGroupContributionsLocked(group);
        group.spec.derivations().stream()
                .map(RuntimeConstraintDerivation::constraintId)
                .filter(constraintId -> snapshots.get(constraintId).state() == PENDING)
                .map(constraintId -> finishConstraintLocked(constraintId, RuntimeConstraintPublicationState.DISABLED, null))
                .forEach(completions::add);
        group.state = DISABLED;
    }

    private void evaluateCompletionLocked(GroupContext group, List<Completion> completions)
    {
        if (group.state == FINAL || group.state == DISABLED || group.state == CLOSED) {
            return;
        }
        if (sourceRegistrationBarrierEnabled && !group.sourceRegistrationComplete) {
            return;
        }
        DistributedCompletionPolicy policy = group.spec.completionPolicy();
        if ((policy == EQUIVALENT_REPLICAS || policy == SINGLE) && !group.committedPartitions.isEmpty()) {
            finalizeGroupLocked(group, completions);
            return;
        }
        if (!group.expectedPartitions.keySet().containsAll(group.sourceFragments)) {
            return;
        }
        Set<ProducerPartition> expectedProducerPartitions = group.expectedPartitions.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(partitionId -> new ProducerPartition(entry.getKey(), partitionId)))
                .collect(toImmutableSet());
        if (!group.committedPartitions.containsAll(expectedProducerPartitions)) {
            return;
        }
        finalizeGroupLocked(group, completions);
    }

    private void finalizeGroupLocked(GroupContext group, List<Completion> completions)
    {
        List<Domain> compatibilitySummaries = group.aggregate == null
                ? group.spec.collectedLanes().stream().map(lane -> Domain.none(lane.type())).toList()
                : group.aggregate.scalarDomains();
        if (compatibilitySummaries.stream().anyMatch(domain -> !domain.isAll())) {
            group.compatibilitySummaries = compatibilitySummaries;
            retainedBytes += compatibilitySummaries.stream().mapToLong(Domain::getRetainedSizeInBytes).sum();
        }
        Map<RuntimeConstraintId, RuntimeMembershipPayload> derivedPayloads = RuntimeConstraintDeriver.derive(
                group.spec,
                group.aggregate == null ? ImmutableList.of() : ImmutableList.of(group.aggregate));
        releaseGroupContributionsLocked(group);
        for (Map.Entry<RuntimeConstraintId, RuntimeMembershipPayload> entry : derivedPayloads.entrySet()) {
            if (snapshots.get(entry.getKey()).state() != PENDING) {
                continue;
            }
            RuntimeMembershipPayload derived = (RuntimeMembershipPayload) boundPayload(entry.getValue());
            RuntimeConstraintPublicationState state = isUnrestricted(derived)
                    ? RuntimeConstraintPublicationState.DISABLED
                    : RuntimeConstraintPublicationState.FINAL;
            completions.add(finishConstraintLocked(entry.getKey(), state, derived));
        }
        group.state = group.spec.derivations().stream()
                .map(RuntimeConstraintDerivation::constraintId)
                .map(snapshots::get)
                .allMatch(snapshot -> snapshot.state() == RuntimeConstraintPublicationState.DISABLED)
                ? DISABLED
                : FINAL;
    }

    private Completion finishConstraintLocked(RuntimeConstraintId constraintId, RuntimeConstraintPublicationState state, RuntimeConstraintPayload payload)
    {
        RuntimeConstraintSnapshot current = requireConstraint(constraintId);
        checkState(current.state() == PENDING, "constraint is already terminal: %s", constraintId);
        RuntimeConstraintSnapshot updated = state == RuntimeConstraintPublicationState.FINAL
                ? RuntimeConstraintSnapshot.finalSnapshot(constraintId, generation, current.version() + 1, requireNonNull(payload, "payload is null"))
                : RuntimeConstraintSnapshot.terminal(constraintId, generation, current.version() + 1, state);
        snapshots.put(constraintId, updated);
        retainedBytes += updated.retainedSizeInBytes();
        GroupContext group = constraintGroups.get(constraintId);
        if (group != null && group.collectionDuration == null && group.spec.derivations().stream()
                .map(RuntimeConstraintDerivation::constraintId)
                .map(snapshots::get)
                .allMatch(snapshot -> snapshot.state() != PENDING)) {
            group.collectionDuration = Duration.succinctNanos(System.nanoTime() - group.startNanos);
        }
        return new Completion(waiters.remove(constraintId), initialUnblockWaiters.remove(constraintId), updated);
    }

    private void discardAttemptLocked(GroupContext group, PlanFragmentId fragmentId, int partitionId, int attemptId)
    {
        contributions.keySet().stream()
                .filter(key -> key.groupId().equals(group.spec.groupId()))
                .filter(key -> key.fragmentId().equals(fragmentId))
                .filter(key -> key.logicalPartitionId() == partitionId && key.taskAttemptId() == attemptId)
                .toList()
                .forEach(this::removeContributionLocked);
    }

    private void discardOtherAttemptsLocked(GroupContext group, PlanFragmentId fragmentId, int partitionId, int successfulAttemptId)
    {
        contributions.keySet().stream()
                .filter(key -> key.groupId().equals(group.spec.groupId()))
                .filter(key -> key.fragmentId().equals(fragmentId))
                .filter(key -> key.logicalPartitionId() == partitionId && key.taskAttemptId() != successfulAttemptId)
                .toList()
                .forEach(this::removeContributionLocked);
    }

    private Map<ContributionKey, PendingContribution> removePendingUnregisteredContributionsLocked(ProducerBindingId bindingId)
    {
        Map<ContributionKey, PendingContribution> removed = pendingUnregisteredContributions.remove(bindingId);
        long removedBytes = pendingUnregisteredContributionBytes.getOrDefault(bindingId, 0L);
        pendingUnregisteredContributionBytes.remove(bindingId);
        retainedBytes -= removedBytes;
        checkState(retainedBytes >= 0, "retained runtime constraint memory is negative");
        return removed;
    }

    private void discardPendingUnregisteredAttemptLocked(TaskId taskId)
    {
        pendingUnregisteredContributions.entrySet().removeIf(entry -> {
            long removedBytes = entry.getValue().entrySet().stream()
                    .filter(contribution -> contribution.getValue().taskId().equals(taskId))
                    .mapToLong(contribution -> contribution.getValue().contribution().payload().getRetainedSizeInBytes())
                    .sum();
            entry.getValue().entrySet().removeIf(contribution -> contribution.getValue().taskId().equals(taskId));
            if (removedBytes > 0) {
                retainedBytes -= removedBytes;
                pendingUnregisteredContributionBytes.compute(entry.getKey(), (_, bytes) -> {
                    checkState(bytes != null && bytes >= removedBytes, "pending unregistered contribution memory is negative for binding: %s", entry.getKey());
                    long remaining = bytes - removedBytes;
                    return remaining == 0 ? null : remaining;
                });
            }
            return entry.getValue().isEmpty();
        });
        checkState(retainedBytes >= 0, "retained runtime constraint memory is negative");
    }

    private void releaseGroupContributionsLocked(GroupContext group)
    {
        contributions.keySet().stream()
                .filter(key -> key.groupId().equals(group.spec.groupId()))
                .toList()
                .forEach(this::removeContributionLocked);
        group.committedPartitions.clear();
        if (group.aggregate != null) {
            retainedBytes -= group.aggregate.getRetainedSizeInBytes();
            group.aggregate = null;
        }
    }

    private RuntimeMembershipPayload mergePayloads(RuntimeMembershipPayload current, RuntimeMembershipPayload update)
    {
        if (current == null) {
            return (RuntimeMembershipPayload) boundPayload(update);
        }
        checkArgument(current.lanes().size() == update.lanes().size(), "membership contributions have different lane counts");
        checkArgument(current.nullMatchMode() == update.nullMatchMode(), "membership contributions have different null modes");
        return new RuntimeMembershipPayload(
                IntStream.range(0, current.lanes().size())
                        .mapToObj(index -> new Lane(
                                boundDomain(current.lanes().get(index).domain().union(update.lanes().get(index).domain())),
                                current.lanes().get(index).sawNull() || update.lanes().get(index).sawNull()))
                        .toList(),
                current.nullMatchMode(),
                current.sawInputRow() || update.sawInputRow());
    }

    private RuntimeConstraintPayload boundPayload(RuntimeConstraintPayload payload)
    {
        if (!(requireNonNull(payload, "payload is null") instanceof RuntimeMembershipPayload membership)) {
            return payload;
        }
        return new RuntimeMembershipPayload(
                membership.lanes().stream().map(lane -> new Lane(boundDomain(lane.domain()), lane.sawNull())).toList(),
                membership.nullMatchMode(),
                membership.sawInputRow());
    }

    private Domain boundDomain(Domain domain)
    {
        if (domain.getRetainedSizeInBytes() <= maxRetainedBytes) {
            return domain;
        }
        Domain simplified = domain.simplify(1);
        if (simplified.getRetainedSizeInBytes() <= maxRetainedBytes) {
            return simplified;
        }
        return Domain.all(domain.getType());
    }

    private static boolean isUnrestricted(RuntimeConstraintPayload payload)
    {
        return payload instanceof RuntimeMembershipPayload membership && membership.scalarDomains().stream().allMatch(Domain::isAll);
    }

    private void removeContributionLocked(ContributionKey key)
    {
        RuntimeConstraintContribution removed = contributions.remove(key);
        if (removed != null) {
            long removedBytes = removed.payload().getRetainedSizeInBytes();
            retainedBytes -= removedBytes;
            GroupContext group = requireGroup(removed.groupId());
            checkState(group.pendingContributionBytes >= removedBytes, "pending contribution memory is negative for group: %s", removed.groupId());
            group.pendingContributionBytes -= removedBytes;
        }
    }

    @Override
    public void close()
    {
        List<Completion> completions = new ArrayList<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            retainedBytes = 0;
            contributions.clear();
            pendingUnregisteredContributions.clear();
            pendingUnregisteredContributionBytes.clear();
            overLimitUnregisteredBindings.clear();
            sealedProducerStages.clear();
            failedTasks.clear();
            acceptedContributionSequences.clear();
            successfulTaskFinalSequences.clear();
            initiallyUnblocked.clear();
            groups.values().forEach(group -> {
                group.pendingContributionBytes = 0;
                group.committedPartitions.clear();
                group.aggregate = null;
                group.state = CLOSED;
            });
            for (Map.Entry<RuntimeConstraintId, RuntimeConstraintSnapshot> entry : snapshots.entrySet()) {
                RuntimeConstraintSnapshot current = entry.getValue();
                RuntimeConstraintSnapshot closedSnapshot = RuntimeConstraintSnapshot.terminal(entry.getKey(), generation, current.version() + 1, RuntimeConstraintPublicationState.CLOSED);
                entry.setValue(closedSnapshot);
                completions.add(new Completion(waiters.remove(entry.getKey()), initialUnblockWaiters.remove(entry.getKey()), closedSnapshot));
            }
            waiters.clear();
            initialUnblockWaiters.clear();
        }
        complete(completions);
    }

    private RuntimeConstraintSnapshot requireConstraint(RuntimeConstraintId constraintId)
    {
        return requireNonNull(snapshots.get(requireNonNull(constraintId, "constraintId is null")), "constraint is not registered");
    }

    private GroupContext requireGroup(ProducerGroupId groupId)
    {
        return requireNonNull(groups.get(requireNonNull(groupId, "groupId is null")), "producer group is not registered");
    }

    private static void complete(List<Completion> completions)
    {
        completions.forEach(Completion::complete);
    }

    private static final class GroupContext
    {
        private final RuntimeConstraintProducerGroup spec;
        private final long startNanos = System.nanoTime();
        private Duration collectionDuration;
        private RuntimeConstraintProducerGroupState state = REGISTERED;
        private final Set<PlanFragmentId> sourceFragments = new HashSet<>();
        private final Set<TaskId> sourceTasks = new HashSet<>();
        private final Map<PlanFragmentId, Set<Integer>> expectedPartitions = new HashMap<>();
        private final Map<ProducerPartition, Integer> successfulAttempts = new HashMap<>();
        private final Set<ProducerPartition> committedPartitions = new HashSet<>();
        private boolean sourceRegistrationComplete;
        private long pendingContributionBytes;
        private RuntimeMembershipPayload aggregate;
        private List<Domain> compatibilitySummaries;

        private GroupContext(RuntimeConstraintProducerGroup spec)
        {
            this.spec = requireNonNull(spec, "spec is null");
        }
    }

    private record Completion(
            List<CompletableFuture<RuntimeConstraintSnapshot>> listeners,
            List<CompletableFuture<Void>> initialUnblockListeners,
            RuntimeConstraintSnapshot snapshot)
    {
        private void complete()
        {
            if (listeners != null) {
                listeners.forEach(listener -> listener.complete(snapshot));
            }
            if (initialUnblockListeners != null) {
                initialUnblockListeners.forEach(listener -> listener.complete(null));
            }
        }
    }

    private record ProducerPartition(PlanFragmentId fragmentId, int logicalPartitionId)
    {
        private ProducerPartition
        {
            requireNonNull(fragmentId, "fragmentId is null");
            checkArgument(logicalPartitionId >= 0, "logicalPartitionId is negative");
        }
    }

    public record ContributionKey(ProducerGroupId groupId, ProducerBindingId bindingId, PlanFragmentId fragmentId, int logicalPartitionId, int taskAttemptId)
    {
        private static ContributionKey from(TaskId taskId, RuntimeConstraintContribution contribution)
        {
            return new ContributionKey(contribution.groupId(), contribution.bindingId(), RuntimeConstraintHub.fragmentId(taskId), contribution.logicalPartitionId(), contribution.taskAttemptId());
        }
    }

    private static PlanFragmentId fragmentId(TaskId taskId)
    {
        return new PlanFragmentId(Integer.toString(taskId.stageId().id()));
    }

    public record ConstraintStatistics(
            RuntimeConstraintId constraintId,
            RuntimeConstraintPublicationState state,
            Optional<Domain> summary,
            boolean lazy,
            boolean replicated,
            Optional<Duration> collectionDuration) {}
}
