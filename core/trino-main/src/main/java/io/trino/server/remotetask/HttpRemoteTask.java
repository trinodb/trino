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
package io.trino.server.remotetask;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.DynamicFiltersCollector;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.FutureStateChange;
import io.trino.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.trino.execution.PartitionedSplitsInfo;
import io.trino.execution.RemoteTask;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedBufferInfo;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.metadata.Split;
import io.trino.operator.TaskStats;
import io.trino.server.DynamicFilterService;
import io.trino.server.FailTaskRequest;
import io.trino.server.TaskUpdateRequest;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoTransportException;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.SystemSessionProperties.getMaxRemoteTaskRequestSize;
import static io.trino.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static io.trino.SystemSessionProperties.getRemoteTaskGuaranteedSplitsPerRequest;
import static io.trino.SystemSessionProperties.getRemoteTaskRequestSizeHeadroom;
import static io.trino.SystemSessionProperties.isRemoteTaskAdaptiveUpdateRequestSizeEnabled;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.execution.TaskInfo.createInitialTask;
import static io.trino.execution.TaskState.FAILED;
import static io.trino.execution.TaskStatus.failWith;
import static io.trino.server.remotetask.RequestErrorTracker.logError;
import static io.trino.spi.HostAddress.fromUri;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static io.trino.util.Failures.toFailure;
import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class HttpRemoteTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTask.class);

    private final TaskId taskId;

    private final Session session;
    private final String nodeId;
    private final PlanFragment planFragment;

    private final AtomicLong nextSplitId = new AtomicLong();

    private final RemoteTaskStats stats;
    private final TaskInfoFetcher taskInfoFetcher;
    private final ContinuousTaskStatusFetcher taskStatusFetcher;
    private final DynamicFiltersFetcher dynamicFiltersFetcher;

    private final DynamicFiltersCollector outboundDynamicFiltersCollector;
    // The version of dynamic filters that has been successfully sent to the worker
    private final AtomicLong sentDynamicFiltersVersion = new AtomicLong(INITIAL_DYNAMIC_FILTERS_VERSION);
    private final AtomicLong terminationStartedNanos = new AtomicLong();

    private final AtomicReference<Future<?>> currentRequest = new AtomicReference<>();

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
    private final int maxUnacknowledgedSplits;
    @GuardedBy("this")
    private volatile int pendingSourceSplitCount;
    @GuardedBy("this")
    private volatile long pendingSourceSplitsWeight;
    @GuardedBy("this")
    // The keys of this map represent all plan nodes that have "no more splits".
    // The boolean value of each entry represents whether the "no more splits" notification is pending delivery to workers.
    private final Map<PlanNodeId, Boolean> noMoreSplits = new HashMap<>();
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final FutureStateChange<Void> whenSplitQueueHasSpace = new FutureStateChange<>();
    @GuardedBy("this")
    private boolean splitQueueHasSpace = true;
    @GuardedBy("this")
    private OptionalLong whenSplitQueueHasSpaceThreshold = OptionalLong.empty();

    @VisibleForTesting
    final AtomicInteger splitBatchSize;

    private final boolean summarizeTaskInfo;

    private final HttpClient httpClient;
    private final Executor executor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final Duration maxErrorDuration;
    private final Duration taskTerminationTimeout;

    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<FailTaskRequest> failTaskRequestCodec;

    private final RequestErrorTracker updateErrorTracker;

    private final AtomicInteger pendingRequestsCounter = new AtomicInteger(0);
    private final AtomicBoolean sendPlan = new AtomicBoolean(true);

    private final PartitionedSplitCountTracker partitionedSplitCountTracker;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean terminating = new AtomicBoolean(false);
    private final AtomicBoolean cleanedUp = new AtomicBoolean(false);

    private final int guaranteedSplitsPerRequest;
    private final long maxRequestSizeInBytes;
    private final long requestSizeHeadroomInBytes;
    private final boolean adaptiveUpdateRequestSizeEnabled;

    public HttpRemoteTask(
            Session session,
            TaskId taskId,
            String nodeId,
            URI location,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            HttpClient httpClient,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            Duration maxErrorDuration,
            Duration taskStatusRefreshMaxWait,
            Duration taskInfoUpdateInterval,
            Duration taskTerminationTimeout,
            boolean summarizeTaskInfo,
            JsonCodec<TaskStatus> taskStatusCodec,
            JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            JsonCodec<FailTaskRequest> failTaskRequestCodec,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            RemoteTaskStats stats,
            DynamicFilterService dynamicFilterService,
            Set<DynamicFilterId> outboundDynamicFilterIds,
            Optional<DataSize> estimatedMemory)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(location, "location is null");
        requireNonNull(planFragment, "planFragment is null");
        requireNonNull(outputBuffers, "outputBuffers is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(taskStatusCodec, "taskStatusCodec is null");
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(outboundDynamicFilterIds, "outboundDynamicFilterIds is null");
        requireNonNull(estimatedMemory, "estimatedMemory is null");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            this.taskId = taskId;
            this.session = session;
            this.nodeId = nodeId;
            this.planFragment = planFragment;
            this.outputBuffers.set(outputBuffers);
            this.httpClient = httpClient;
            this.executor = executor;
            this.errorScheduledExecutor = errorScheduledExecutor;
            this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");
            this.taskTerminationTimeout = requireNonNull(taskTerminationTimeout, "taskTerminationTimeout is null");
            this.summarizeTaskInfo = summarizeTaskInfo;
            this.taskInfoCodec = taskInfoCodec;
            this.taskUpdateRequestCodec = taskUpdateRequestCodec;
            this.failTaskRequestCodec = failTaskRequestCodec;
            this.updateErrorTracker = new RequestErrorTracker(taskId, location, maxErrorDuration, errorScheduledExecutor, "updating task");
            this.partitionedSplitCountTracker = requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
            this.stats = stats;

            for (Entry<PlanNodeId, Split> entry : initialSplits.entries()) {
                ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), entry.getKey(), entry.getValue());
                pendingSplits.put(entry.getKey(), scheduledSplit);
            }
            maxUnacknowledgedSplits = getMaxUnacknowledgedSplitsPerTask(session);

            this.guaranteedSplitsPerRequest = getRemoteTaskGuaranteedSplitsPerRequest(session);
            this.maxRequestSizeInBytes = getMaxRemoteTaskRequestSize(session).toBytes();
            this.requestSizeHeadroomInBytes = getRemoteTaskRequestSizeHeadroom(session).toBytes();
            this.splitBatchSize = new AtomicInteger(maxUnacknowledgedSplits);
            long numOfPartitionedSources = planFragment.getPartitionedSources().size();
            // Currently it supports only when there is one partitioned source.
            // TODO. https://github.com/trinodb/trino/issues/15820
            this.adaptiveUpdateRequestSizeEnabled = numOfPartitionedSources == 1 && isRemoteTaskAdaptiveUpdateRequestSizeEnabled(session);
            if (numOfPartitionedSources > 1) {
                log.debug("%s - There are more than one partitioned sources: numOfPartitionedSources=%s",
                        taskId, planFragment.getPartitionedSources().size());
            }

            int pendingSourceSplitCount = 0;
            long pendingSourceSplitsWeight = 0;
            for (PlanNodeId planNodeId : planFragment.getPartitionedSources()) {
                Collection<Split> tableScanSplits = initialSplits.get(planNodeId);
                if (!tableScanSplits.isEmpty()) {
                    pendingSourceSplitCount += tableScanSplits.size();
                    pendingSourceSplitsWeight = addExact(pendingSourceSplitsWeight, SplitWeight.rawValueSum(tableScanSplits, Split::getSplitWeight));
                }
            }
            this.pendingSourceSplitCount = pendingSourceSplitCount;
            this.pendingSourceSplitsWeight = pendingSourceSplitsWeight;

            Optional<List<PipelinedBufferInfo>> pipelinedBufferStates = Optional.empty();
            if (outputBuffers instanceof PipelinedOutputBuffers buffers) {
                pipelinedBufferStates = Optional.of(
                        buffers.getBuffers()
                                .keySet().stream()
                                .map(outputId -> new PipelinedBufferInfo(outputId, 0, 0, 0, 0, 0, false))
                                .collect(toImmutableList()));
            }

            TaskInfo initialTask = createInitialTask(taskId, location, nodeId, pipelinedBufferStates, new TaskStats(DateTime.now(), null));

            this.dynamicFiltersFetcher = new DynamicFiltersFetcher(
                    this::fatalUnacknowledgedFailure,
                    taskId,
                    location,
                    taskStatusRefreshMaxWait,
                    dynamicFilterDomainsCodec,
                    executor,
                    httpClient,
                    maxErrorDuration,
                    errorScheduledExecutor,
                    stats,
                    dynamicFilterService);

            this.taskStatusFetcher = new ContinuousTaskStatusFetcher(
                    this::fatalUnacknowledgedFailure,
                    initialTask.getTaskStatus(),
                    taskStatusRefreshMaxWait,
                    taskStatusCodec,
                    dynamicFiltersFetcher,
                    executor,
                    httpClient,
                    maxErrorDuration,
                    errorScheduledExecutor,
                    stats);

            this.taskInfoFetcher = new TaskInfoFetcher(
                    this::fatalUnacknowledgedFailure,
                    taskStatusFetcher,
                    initialTask,
                    httpClient,
                    taskInfoUpdateInterval,
                    taskInfoCodec,
                    maxErrorDuration,
                    summarizeTaskInfo,
                    executor,
                    updateScheduledExecutor,
                    errorScheduledExecutor,
                    stats,
                    estimatedMemory);

            taskStatusFetcher.addStateChangeListener(newStatus -> {
                TaskState state = newStatus.getState();
                // cleanup when done or partially cleanup when terminating begins
                if (state.isTerminatingOrDone()) {
                    cleanUpTask(state);
                }
                else {
                    partitionedSplitCountTracker.setPartitionedSplits(getPartitionedSplitsInfo());
                    updateSplitQueueSpace();
                }
            });

            this.outboundDynamicFiltersCollector = new DynamicFiltersCollector(this::triggerUpdate);
            dynamicFilterService.registerDynamicFilterConsumer(
                    taskId.getQueryId(),
                    taskId.getAttemptId(),
                    outboundDynamicFilterIds,
                    outboundDynamicFiltersCollector::updateDomains);

            partitionedSplitCountTracker.setPartitionedSplits(getPartitionedSplitsInfo());
            updateSplitQueueSpace();
        }
    }

    @Override
    public TaskId getTaskId()
    {
        return taskId;
    }

    @Override
    public String getNodeId()
    {
        return nodeId;
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskInfoFetcher.getTaskInfo();
    }

    @Override
    public TaskStatus getTaskStatus()
    {
        return taskStatusFetcher.getTaskStatus();
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            // to start we just need to trigger an update
            started.set(true);
            triggerUpdate();

            dynamicFiltersFetcher.start();
            taskStatusFetcher.start();
            taskInfoFetcher.start();
        }
    }

    @Override
    public synchronized void addSplits(Multimap<PlanNodeId, Split> splitsBySource)
    {
        requireNonNull(splitsBySource, "splitsBySource is null");

        // only add pending split if not done
        if (getTaskStatus().getState().isTerminatingOrDone() || terminating.get()) {
            return;
        }

        boolean needsUpdate = false;
        for (Entry<PlanNodeId, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
            PlanNodeId sourceId = entry.getKey();
            Collection<Split> splits = entry.getValue();
            boolean isPartitionedSource = planFragment.isPartitionedSources(sourceId);

            checkState(!noMoreSplits.containsKey(sourceId), "noMoreSplits has already been set for %s", sourceId);
            int added = 0;
            long addedWeight = 0;
            for (Split split : splits) {
                if (pendingSplits.put(sourceId, new ScheduledSplit(nextSplitId.getAndIncrement(), sourceId, split))) {
                    if (isPartitionedSource) {
                        added++;
                        addedWeight = addExact(addedWeight, split.getSplitWeight().getRawValue());
                    }
                }
            }
            if (isPartitionedSource) {
                pendingSourceSplitCount += added;
                pendingSourceSplitsWeight = addExact(pendingSourceSplitsWeight, addedWeight);
                partitionedSplitCountTracker.setPartitionedSplits(getPartitionedSplitsInfo());
            }
            needsUpdate = true;
        }
        updateSplitQueueSpace();

        if (needsUpdate) {
            triggerUpdate();
        }
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        if (noMoreSplits.containsKey(sourceId) || terminating.get()) {
            return;
        }

        noMoreSplits.put(sourceId, true);
        triggerUpdate();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        if (getTaskStatus().getState().isTerminatingOrDone() || terminating.get()) {
            return;
        }

        long previousVersion = outputBuffers.getAndUpdate(previousOutputBuffers -> {
            if (newOutputBuffers.getVersion() > previousOutputBuffers.getVersion()) {
                return newOutputBuffers;
            }

            return previousOutputBuffers;
        }).getVersion();

        if (newOutputBuffers.getVersion() > previousVersion) {
            triggerUpdate();
        }
    }

    @Override
    public PartitionedSplitsInfo getPartitionedSplitsInfo()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return PartitionedSplitsInfo.forZeroSplits();
        }
        // Do not consider queued or unacknowledged splits if the task is in the process of terminating
        if (taskStatus.getState().isTerminating()) {
            return PartitionedSplitsInfo.forSplitCountAndWeightSum(taskStatus.getRunningPartitionedDrivers(), taskStatus.getRunningPartitionedSplitsWeight());
        }
        PartitionedSplitsInfo unacknowledgedSplitsInfo = getUnacknowledgedPartitionedSplitsInfo();
        int count = unacknowledgedSplitsInfo.getCount() + taskStatus.getQueuedPartitionedDrivers() + taskStatus.getRunningPartitionedDrivers();
        long weight = unacknowledgedSplitsInfo.getWeightSum() + taskStatus.getQueuedPartitionedSplitsWeight() + taskStatus.getRunningPartitionedSplitsWeight();
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public PartitionedSplitsInfo getUnacknowledgedPartitionedSplitsInfo()
    {
        int count = pendingSourceSplitCount;
        long weight = pendingSourceSplitsWeight;
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @Override
    public PartitionedSplitsInfo getQueuedPartitionedSplitsInfo()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isTerminatingOrDone()) {
            return PartitionedSplitsInfo.forZeroSplits();
        }
        PartitionedSplitsInfo unacknowledgedSplitsInfo = getUnacknowledgedPartitionedSplitsInfo();
        int count = unacknowledgedSplitsInfo.getCount() + taskStatus.getQueuedPartitionedDrivers();
        long weight = unacknowledgedSplitsInfo.getWeightSum() + taskStatus.getQueuedPartitionedSplitsWeight();
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @Override
    public int getUnacknowledgedPartitionedSplitCount()
    {
        return getPendingSourceSplitCount();
    }

    @Override
    public SpoolingOutputStats.Snapshot retrieveAndDropSpoolingOutputStats()
    {
        return taskInfoFetcher.retrieveAndDropSpoolingOutputStats();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getPendingSourceSplitCount()
    {
        return pendingSourceSplitCount;
    }

    private long getQueuedPartitionedSplitsWeight()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isTerminatingOrDone()) {
            return 0;
        }
        return getPendingSourceSplitsWeight() + taskStatus.getQueuedPartitionedSplitsWeight();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private long getPendingSourceSplitsWeight()
    {
        return pendingSourceSplitsWeight;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            taskStatusFetcher.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        taskInfoFetcher.addFinalTaskInfoListener(stateChangeListener);
    }

    @Override
    public synchronized ListenableFuture<Void> whenSplitQueueHasSpace(long weightThreshold)
    {
        if (whenSplitQueueHasSpaceThreshold.isPresent()) {
            checkArgument(weightThreshold == whenSplitQueueHasSpaceThreshold.getAsLong(), "Multiple split queue space notification thresholds not supported");
        }
        else {
            whenSplitQueueHasSpaceThreshold = OptionalLong.of(weightThreshold);
            updateSplitQueueSpace();
        }
        if (splitQueueHasSpace) {
            return immediateVoidFuture();
        }
        return whenSplitQueueHasSpace.createNewListener();
    }

    private synchronized void updateSplitQueueSpace()
    {
        // Must check whether the unacknowledged split count threshold is reached even without listeners registered yet
        splitQueueHasSpace = getUnacknowledgedPartitionedSplitCount() < maxUnacknowledgedSplits &&
                (whenSplitQueueHasSpaceThreshold.isEmpty() || getQueuedPartitionedSplitsWeight() < whenSplitQueueHasSpaceThreshold.getAsLong());
        // Only trigger notifications if a listener might be registered
        if (splitQueueHasSpace && whenSplitQueueHasSpaceThreshold.isPresent()) {
            whenSplitQueueHasSpace.complete(null, executor);
        }
    }

    private synchronized void processTaskUpdate(TaskInfo newValue, List<SplitAssignment> splitAssignments)
    {
        updateTaskInfo(newValue);

        // remove acknowledged splits, which frees memory
        for (SplitAssignment assignment : splitAssignments) {
            PlanNodeId planNodeId = assignment.getPlanNodeId();
            boolean isPartitionedSource = planFragment.isPartitionedSources(planNodeId);
            int removed = 0;
            long removedWeight = 0;
            for (ScheduledSplit split : assignment.getSplits()) {
                if (pendingSplits.remove(planNodeId, split)) {
                    if (isPartitionedSource) {
                        removed++;
                        removedWeight = addExact(removedWeight, split.getSplit().getSplitWeight().getRawValue());
                    }
                }
            }
            if (assignment.isNoMoreSplits()) {
                noMoreSplits.put(planNodeId, false);
            }
            if (isPartitionedSource) {
                pendingSourceSplitCount -= removed;
                pendingSourceSplitsWeight -= removedWeight;
            }
        }

        // increment pendingRequestsCounter by 1 when there are still pending splits
        if (pendingSourceSplitCount > 0) {
            pendingRequestsCounter.incrementAndGet();
        }

        // Update node level split tracker before split queue space to ensure it's up to date before waking up the scheduler
        partitionedSplitCountTracker.setPartitionedSplits(getPartitionedSplitsInfo());
        updateSplitQueueSpace();
    }

    private void updateTaskInfo(TaskInfo taskInfo)
    {
        taskStatusFetcher.updateTaskStatus(taskInfo.getTaskStatus());
        taskInfoFetcher.updateTaskInfo(taskInfo);
    }

    private void scheduleUpdate()
    {
        executor.execute(this::sendUpdate);
    }

    private void triggerUpdate()
    {
        if (!started.get()) {
            // task has not started yet
            return;
        }
        if (pendingRequestsCounter.getAndIncrement() == 0) {
            // schedule update if this is the first update requested
            scheduleUpdate();
        }
    }

    /**
     * Adaptively adjust batch size to meet expected request size:
     * If requestSize is not equal to expectedSize, this function will try to estimate and adjust the batch size proportionally based on
     * current nums of splits and size of request.
     */
    @VisibleForTesting
    boolean adjustSplitBatchSize(List<SplitAssignment> splitAssignments, long requestSize, int currentSplitBatchSize)
    {
        if ((requestSize > maxRequestSizeInBytes && currentSplitBatchSize > guaranteedSplitsPerRequest) || (requestSize < maxRequestSizeInBytes && currentSplitBatchSize < maxUnacknowledgedSplits)) {
            int newSplitBatchSize = currentSplitBatchSize;
            int numSplits = 0;
            for (SplitAssignment splitAssignment : splitAssignments) {
                // Adjustment applies only to partitioned sources.
                if (planFragment.isPartitionedSources(splitAssignment.getPlanNodeId())) {
                    numSplits = splitAssignment.getSplits().size();
                    break;
                }
            }
            if (numSplits != 0) {
                newSplitBatchSize = (int) ((numSplits * (maxRequestSizeInBytes - requestSizeHeadroomInBytes)) / requestSize);
                newSplitBatchSize = Math.max(guaranteedSplitsPerRequest, Math.min(maxUnacknowledgedSplits, newSplitBatchSize));
            }
            if (newSplitBatchSize != currentSplitBatchSize) {
                log.debug("%s - Split batch size changed: prevSize=%s, newSize=%s", taskId, currentSplitBatchSize, newSplitBatchSize);
                splitBatchSize.set(newSplitBatchSize);
            }
            // abandon current request and reschedule update if size of request body exceeds requestSizeLimit and splitBatchSize is updated
            if (numSplits > newSplitBatchSize && requestSize > maxRequestSizeInBytes) {
                log.debug("%s - current taskUpdateRequestJson exceeded limit: %d, currentSplitBatchSize: %d, newSplitBatchSize: %d",
                        taskId, requestSize, currentSplitBatchSize, newSplitBatchSize);
                return true; // reschedule needed
            }
        }
        return false;
    }

    private void sendUpdate()
    {
        TaskStatus taskStatus = getTaskStatus();
        // don't update if the task is already finishing or finished, or if we have sent a termination command
        if (taskStatus.getState().isTerminatingOrDone() || terminating.get()) {
            return;
        }
        checkState(started.get());

        int currentPendingRequestsCounter = pendingRequestsCounter.get();
        checkState(currentPendingRequestsCounter > 0, "sendUpdate shouldn't be called without pending requests");

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<Void> errorRateLimit = updateErrorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendUpdate, executor);
            return;
        }

        int currentSplitBatchSize = splitBatchSize.get();
        List<SplitAssignment> splitAssignments = getSplitAssignments(currentSplitBatchSize);
        VersionedDynamicFilterDomains dynamicFilterDomains = outboundDynamicFiltersCollector.acknowledgeAndGetNewDomains(sentDynamicFiltersVersion.get());

        // Workers don't need the embedded JSON representation when the fragment is sent
        Optional<PlanFragment> fragment = sendPlan.get() ? Optional.of(planFragment.withoutEmbeddedJsonRepresentation()) : Optional.empty();
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                splitAssignments,
                outputBuffers.get(),
                dynamicFilterDomains.getDynamicFilterDomains(),
                session.getExchangeEncryptionKey());
        byte[] taskUpdateRequestJson = taskUpdateRequestCodec.toJsonBytes(updateRequest);

        // try to adjust batch size to meet expected request size
        if (adaptiveUpdateRequestSizeEnabled && adjustSplitBatchSize(splitAssignments, taskUpdateRequestJson.length, currentSplitBatchSize)) {
            scheduleUpdate();
            return;
        }

        if (fragment.isPresent()) {
            stats.updateWithPlanBytes(taskUpdateRequestJson.length);
        }
        if (!dynamicFilterDomains.getDynamicFilterDomains().isEmpty()) {
            stats.updateWithDynamicFilterBytes(taskUpdateRequestJson.length);
        }

        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus);
        Request request = preparePost()
                .setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestJson))
                .build();

        updateErrorTracker.startRequest();

        ListenableFuture<JsonResponse<TaskInfo>> future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        checkState(currentRequest.getAndSet(future) == null, "There should be no previous request running");

        Futures.addCallback(
                future,
                new SimpleHttpResponseHandler<>(new UpdateResponseHandler(splitAssignments, dynamicFilterDomains.getVersion(), System.nanoTime(), currentPendingRequestsCounter), request.getUri(), stats),
                executor);
    }

    private synchronized List<SplitAssignment> getSplitAssignments(int currentSplitBatchSize)
    {
        return Stream.concat(planFragment.getPartitionedSourceNodes().stream(), planFragment.getRemoteSourceNodes().stream())
                .filter(Objects::nonNull)
                .map(PlanNode::getId)
                .map(planNodeId -> getSplitAssignment(planNodeId, currentSplitBatchSize))
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private synchronized SplitAssignment getSplitAssignment(PlanNodeId planNodeId, int currentSplitBatchSize)
    {
        Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
        boolean pendingNoMoreSplits = Boolean.TRUE.equals(this.noMoreSplits.get(planNodeId));
        boolean noMoreSplits = this.noMoreSplits.containsKey(planNodeId);

        // limit the number of splits only for a partitioned source
        if (planFragment.isPartitionedSources(planNodeId) && currentSplitBatchSize < splits.size()) {
            log.debug("%s - Splits are limited by splitBatchSize: splitBatchSize=%s, splits=%s, planNodeId=%s", taskId, currentSplitBatchSize, splits.size(), planNodeId);
            splits = splits.stream()
                    .sorted(Comparator.comparingLong(ScheduledSplit::getSequenceId))
                    .limit(currentSplitBatchSize)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            // if not last batch, we need to defer setting no more splits
            noMoreSplits = false;
        }

        SplitAssignment assignment = null;
        if (!splits.isEmpty() || pendingNoMoreSplits) {
            assignment = new SplitAssignment(planNodeId, splits, noMoreSplits);
        }
        return assignment;
    }

    @Override
    public void abort()
    {
        // Only trigger abort commands if we aren't already canceling or failing the task remotely
        if (!terminating.compareAndSet(false, true)) {
            return;
        }

        synchronized (this) {
            try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
                if (!getTaskStatus().getState().isTerminatingOrDone()) {
                    scheduleAsyncCleanupRequest("abort", true);
                }
            }
        }
    }

    @Override
    public void cancel()
    {
        // Only cancel the task if we aren't already attempting to abort or fail the task remotely
        if (!terminating.compareAndSet(false, true)) {
            return;
        }

        synchronized (this) {
            try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
                TaskStatus taskStatus = getTaskStatus();
                if (!taskStatus.getState().isTerminatingOrDone()) {
                    scheduleAsyncCleanupRequest("cancel", false);
                }
            }
        }
    }

    private void cleanUpTask(TaskState taskState)
    {
        checkState(taskState.isTerminatingOrDone(), "attempt to clean up a task that is not terminating or done: %s", taskState);

        // clear pending splits to free memory
        synchronized (this) {
            pendingSplits.clear();
            pendingSourceSplitCount = 0;
            pendingSourceSplitsWeight = 0;
            partitionedSplitCountTracker.setPartitionedSplits(getPartitionedSplitsInfo());
            splitQueueHasSpace = true;
            whenSplitQueueHasSpace.complete(null, executor);
        }

        // clear pending outbound dynamic filters to free memory
        outboundDynamicFiltersCollector.acknowledge(Long.MAX_VALUE);

        // only when termination is complete do we shut down status fetching
        if (taskState.isDone()) {
            // stop continuously fetching task status
            taskStatusFetcher.stop();
            // cancel pending request
            Future<?> request = currentRequest.getAndSet(null);
            if (request != null) {
                request.cancel(true);
            }

            // The remote task is likely to get a delete from the PageBufferClient first.
            // We send an additional delete anyway to get the final TaskInfo
            scheduleAsyncCleanupRequest("cleanup", true);
        }
        else {
            // check for termination timeout
            long terminationStartedNanos = this.terminationStartedNanos.get();
            if (terminationStartedNanos == 0) {
                long currentTimeNanos = System.nanoTime();
                // If reported time is exactly 0, increase it by 1 nanosecond so that "0" can be treated as "not set"
                if (currentTimeNanos == 0) {
                    currentTimeNanos = 1;
                }
                this.terminationStartedNanos.compareAndSet(0, currentTimeNanos);
            }
            else {
                Duration terminatingTime = nanosSince(terminationStartedNanos);
                if (terminatingTime.compareTo(taskTerminationTimeout) >= 0) {
                    // timeout and force cleanup locally
                    fatalUnacknowledgedFailure(new TrinoException(REMOTE_TASK_ERROR, format("Task %s failed to terminate after %s, last known state: %s", taskId, taskTerminationTimeout, taskState)));
                }
            }
        }
    }

    private void scheduleAsyncCleanupRequest(String action, boolean abort)
    {
        scheduleAsyncCleanupRequest(action, () -> buildDeleteTaskRequest(abort));
    }

    private void scheduleAsyncCleanupRequest(String action, FailTaskRequest failTaskRequest)
    {
        scheduleAsyncCleanupRequest(action, () -> buildFailTaskRequest(failTaskRequest));
    }

    private void scheduleAsyncCleanupRequest(String action, Supplier<Request> remoteRequestSupplier)
    {
        // Only allow a single final cleanup request once the task status sees a final state
        TaskState taskStatusState = getTaskStatus().getState();
        if (taskStatusState.isDone() && !cleanedUp.compareAndSet(false, true)) {
            return;
        }

        Request request = remoteRequestSupplier.get();
        doScheduleAsyncCleanupRequest(new Backoff(maxErrorDuration), request, action);
    }

    private Request buildDeleteTaskRequest(boolean abort)
    {
        HttpUriBuilder uriBuilder = getHttpUriBuilder(getTaskStatus()).addParameter("abort", "" + abort);
        return prepareDelete()
                .setUri(uriBuilder.build())
                .build();
    }

    private Request buildFailTaskRequest(FailTaskRequest failTaskRequest)
    {
        HttpUriBuilder uriBuilder = getHttpUriBuilder(getTaskStatus());
        uriBuilder = uriBuilder.appendPath("fail");
        return preparePost()
                .setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(createStaticBodyGenerator(failTaskRequestCodec.toJsonBytes(failTaskRequest)))
                .build();
    }

    private void doScheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        Futures.addCallback(httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec)), new FutureCallback<>()
        {
            @Override
            public void onSuccess(JsonResponse<TaskInfo> result)
            {
                try {
                    updateTaskInfo(result.getValue());
                }
                finally {
                    // if cleanup operation has not at least started task termination, mark the task failed
                    TaskState taskState = getTaskInfo().getTaskStatus().getState();
                    if (!taskState.isTerminatingOrDone()) {
                        fatalUnacknowledgedFailure(new TrinoTransportException(REMOTE_TASK_ERROR, fromUri(request.getUri()), format("Unable to %s task at %s, last known state was: %s", action, request.getUri(), taskState)));
                    }
                }
            }

            @Override
            @SuppressWarnings("FormatStringAnnotation") // we manipulate the format string and there's no way to make Error Prone accept the result
            public void onFailure(Throwable t)
            {
                // final task info has been received, no need to resend the request
                if (getTaskInfo().getTaskStatus().getState().isDone()) {
                    return;
                }

                if (t instanceof RejectedExecutionException && httpClient.isClosed()) {
                    String message = format("Unable to %s task at %s. HTTP client is closed.", action, request.getUri());
                    logError(t, message);
                    fatalUnacknowledgedFailure(new TrinoTransportException(REMOTE_TASK_ERROR, fromUri(request.getUri()), message));
                    return;
                }

                // record failure
                if (cleanupBackoff.failure()) {
                    String message = format("Unable to %s task at %s. Back off depleted.", action, request.getUri());
                    logError(t, message);
                    fatalUnacknowledgedFailure(new TrinoTransportException(REMOTE_TASK_ERROR, fromUri(request.getUri()), message));
                    return;
                }

                // reschedule
                long delayNanos = cleanupBackoff.getBackoffDelayNanos();
                if (delayNanos == 0) {
                    doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
                }
                else {
                    errorScheduledExecutor.schedule(() -> doScheduleAsyncCleanupRequest(cleanupBackoff, request, action), delayNanos, NANOSECONDS);
                }
            }
        }, executor);
    }

    /**
     * Move the task directly to the failed state if there was a failure in this task
     */
    private synchronized void fatalUnacknowledgedFailure(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            TaskStatus taskStatus = getTaskStatus();
            if (!taskStatus.getState().isDone()) {
                // Update the taskInfo with the new taskStatus.

                // Generally, we send a cleanup request to the worker, and update the TaskInfo on
                // the coordinator based on what we fetched from the worker. If we somehow cannot
                // get the cleanup request to the worker, the TaskInfo that we fetch for the worker
                // likely will not say the task is done however many times we try. In this case,
                // we have to set the local query info directly so that we stop trying to fetch
                // updated TaskInfo from the worker. This way, the task on the worker eventually
                // expires due to lack of activity.
                List<ExecutionFailureInfo> failures = ImmutableList.<ExecutionFailureInfo>builderWithExpectedSize(taskStatus.getFailures().size() + 1)
                        .add(toFailure(cause))
                        .addAll(taskStatus.getFailures())
                        .build();
                taskStatus = failWith(taskStatus, FAILED, failures);
                if (cause instanceof TrinoTransportException) {
                    // Since this TaskInfo is updated in the client without having received them from the
                    // worker, the stats may not reflect the actual final stats had we been able to reach the worker
                    // to get them.
                    updateTaskInfo(getTaskInfo().withTaskStatus(taskStatus));
                }
                else {
                    // Let the status callbacks trigger the cleanup command remotely after switching states
                    taskStatusFetcher.updateTaskStatus(taskStatus);
                }
            }
        }
    }

    /**
     * Trigger remote task failure. Task status will be updated only when request sent to remote node returns.
     */
    @Override
    public void failRemotely(Throwable cause)
    {
        if (!terminating.compareAndSet(false, true)) {
            return;
        }

        synchronized (this) {
            try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
                TaskStatus taskStatus = getTaskStatus();
                if (!taskStatus.getState().isTerminatingOrDone()) {
                    log.debug(cause, "Remote task %s failed with %s", taskStatus.getSelf(), cause);
                    scheduleAsyncCleanupRequest("fail", new FailTaskRequest(toFailure(cause)));
                }
            }
        }
    }

    @Override
    public void failLocallyImmediately(Throwable cause)
    {
        requireNonNull(cause, "cause is null");
        // Prevent concurrent abort commands after this point
        terminating.set(true);
        synchronized (this) {
            try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
                TaskStatus taskStatus = getTaskStatus();
                if (!taskStatus.getState().isDone()) {
                    // Record and force the task into a failed state immediately without waiting for the task to respond. A final cleanup
                    // command will be sent to the task, but will not await the response
                    List<ExecutionFailureInfo> failures = ImmutableList.<ExecutionFailureInfo>builderWithExpectedSize(taskStatus.getFailures().size() + 1)
                            .add(toFailure(cause))
                            .addAll(taskStatus.getFailures())
                            .build();
                    taskStatusFetcher.updateTaskStatus(failWith(taskStatus, FAILED, failures));
                }
            }
        }
    }

    private HttpUriBuilder getHttpUriBuilder(TaskStatus taskStatus)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(taskStatus.getSelf());
        if (summarizeTaskInfo) {
            uriBuilder.addParameter("summarize");
        }
        return uriBuilder;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(getTaskInfo())
                .toString();
    }

    private class UpdateResponseHandler
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        private final List<SplitAssignment> splitAssignments;
        private final long currentRequestDynamicFiltersVersion;
        private final long currentRequestStartNanos;
        private final int currentPendingRequestsCounter;

        private UpdateResponseHandler(List<SplitAssignment> splitAssignments, long currentRequestDynamicFiltersVersion, long currentRequestStartNanos, int currentPendingRequestsCounter)
        {
            this.splitAssignments = ImmutableList.copyOf(requireNonNull(splitAssignments, "splitAssignments is null"));
            this.currentRequestDynamicFiltersVersion = currentRequestDynamicFiltersVersion;
            this.currentRequestStartNanos = currentRequestStartNanos;
            this.currentPendingRequestsCounter = currentPendingRequestsCounter;
        }

        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                sentDynamicFiltersVersion.set(currentRequestDynamicFiltersVersion);
                // Remove dynamic filters which were successfully sent to free up memory
                outboundDynamicFiltersCollector.acknowledge(currentRequestDynamicFiltersVersion);
                sendPlan.set(value.isNeedsPlan());
                currentRequest.set(null);
                updateStats();
                updateErrorTracker.requestSucceeded();
                processTaskUpdate(value, splitAssignments);
                if (pendingRequestsCounter.addAndGet(-currentPendingRequestsCounter) > 0) {
                    // schedule an update because triggerUpdate was called in the meantime
                    scheduleUpdate();
                }
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    currentRequest.set(null);
                    updateStats();

                    // if task not already done, record error
                    TaskStatus taskStatus = getTaskStatus();
                    if (!taskStatus.getState().isDone()) {
                        updateErrorTracker.requestFailed(cause);
                    }

                    // on failure assume we need to update again
                    scheduleUpdate();
                }
                catch (Error e) {
                    fatalUnacknowledgedFailure(e);
                    throw e;
                }
                catch (RuntimeException e) {
                    fatalUnacknowledgedFailure(e);
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                fatalUnacknowledgedFailure(cause);
            }
        }

        private void updateStats()
        {
            Duration requestRoundTrip = nanosSince(currentRequestStartNanos);
            stats.updateRoundTripMillis(requestRoundTrip.toMillis());
        }
    }
}
