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
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.DynamicFilterSummary;
import io.trino.execution.FutureStateChange;
import io.trino.execution.Lifespan;
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
import io.trino.execution.VersionedSummaryInfoCollector;
import io.trino.execution.VersionedSummaryInfoCollector.VersionedSummaryInfo;
import io.trino.execution.buffer.BufferInfo;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PageBufferInfo;
import io.trino.metadata.Split;
import io.trino.operator.TaskStats;
import io.trino.server.DynamicFilterService;
import io.trino.server.TaskUpdateRequest;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoTransportException;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static io.trino.execution.SummaryInfo.Type.DYNAMIC_FILTER;
import static io.trino.execution.TaskInfo.createInitialTask;
import static io.trino.execution.TaskState.ABORTED;
import static io.trino.execution.TaskState.FAILED;
import static io.trino.execution.TaskStatus.failWith;
import static io.trino.execution.VersionedSummaryInfoCollector.INITIAL_SUMMARY_VERSION;
import static io.trino.server.remotetask.RequestErrorTracker.logError;
import static io.trino.util.Failures.toFailure;
import static java.lang.Math.addExact;
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
    private final SummaryInfoFetcher summaryInfoFetcher;

    private final VersionedSummaryInfoCollector outboundSummaryInfoCollector;
    @GuardedBy("this")
    // The version of summary stats that has been successfully sent to the worker
    private long sentSummaryStatsVersion = INITIAL_SUMMARY_VERSION;

    @GuardedBy("this")
    private Future<?> currentRequest;
    @GuardedBy("this")
    private long currentRequestStartNanos;

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
    private final int maxUnacknowledgedSplits;
    @GuardedBy("this")
    private volatile int pendingSourceSplitCount;
    @GuardedBy("this")
    private volatile long pendingSourceSplitsWeight;
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, Lifespan> pendingNoMoreSplitsForLifespan = HashMultimap.create();
    @GuardedBy("this")
    // The keys of this map represent all plan nodes that have "no more splits".
    // The boolean value of each entry represents whether the "no more splits" notification is pending delivery to workers.
    private final Map<PlanNodeId, Boolean> noMoreSplits = new HashMap<>();
    @GuardedBy("this")
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final FutureStateChange<Void> whenSplitQueueHasSpace = new FutureStateChange<>();
    @GuardedBy("this")
    private boolean splitQueueHasSpace = true;
    @GuardedBy("this")
    private OptionalLong whenSplitQueueHasSpaceThreshold = OptionalLong.empty();

    private final boolean summarizeTaskInfo;

    private final HttpClient httpClient;
    private final Executor executor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final Duration maxErrorDuration;

    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;

    private final RequestErrorTracker updateErrorTracker;

    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);
    private final AtomicBoolean sendPlan = new AtomicBoolean(true);

    private final PartitionedSplitCountTracker partitionedSplitCountTracker;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean aborting = new AtomicBoolean(false);

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
            boolean summarizeTaskInfo,
            JsonCodec<TaskStatus> taskStatusCodec,
            JsonCodec<VersionedSummaryInfo> versionedSummaryStatsJsonCodec,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            RemoteTaskStats stats,
            DynamicFilterService dynamicFilterService,
            Set<DynamicFilterId> outboundDynamicFilterIds)
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
            this.summarizeTaskInfo = summarizeTaskInfo;
            this.taskInfoCodec = taskInfoCodec;
            this.taskUpdateRequestCodec = taskUpdateRequestCodec;
            this.updateErrorTracker = new RequestErrorTracker(taskId, location, maxErrorDuration, errorScheduledExecutor, "updating task");
            this.partitionedSplitCountTracker = requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
            this.stats = stats;

            for (Entry<PlanNodeId, Split> entry : requireNonNull(initialSplits, "initialSplits is null").entries()) {
                ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), entry.getKey(), entry.getValue());
                pendingSplits.put(entry.getKey(), scheduledSplit);
            }
            maxUnacknowledgedSplits = getMaxUnacknowledgedSplitsPerTask(session);

            int pendingSourceSplitCount = 0;
            long pendingSourceSplitsWeight = 0;
            for (PlanNodeId planNodeId : planFragment.getPartitionedSources()) {
                Collection<Split> tableScanSplits = initialSplits.get(planNodeId);
                if (tableScanSplits != null && !tableScanSplits.isEmpty()) {
                    pendingSourceSplitCount += tableScanSplits.size();
                    pendingSourceSplitsWeight = addExact(pendingSourceSplitsWeight, SplitWeight.rawValueSum(tableScanSplits, Split::getSplitWeight));
                }
            }
            this.pendingSourceSplitCount = pendingSourceSplitCount;
            this.pendingSourceSplitsWeight = pendingSourceSplitsWeight;

            List<BufferInfo> bufferStates = outputBuffers.getBuffers()
                    .keySet().stream()
                    .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                    .collect(toImmutableList());

            TaskInfo initialTask = createInitialTask(taskId, location, nodeId, bufferStates, new TaskStats(DateTime.now(), null));

            this.summaryInfoFetcher = new SummaryInfoFetcher(
                    this::fail,
                    taskId,
                    location,
                    taskStatusRefreshMaxWait,
                    versionedSummaryStatsJsonCodec,
                    executor,
                    httpClient,
                    maxErrorDuration,
                    errorScheduledExecutor,
                    stats,
                    dynamicFilterService);

            this.taskStatusFetcher = new ContinuousTaskStatusFetcher(
                    this::fail,
                    initialTask.getTaskStatus(),
                    taskStatusRefreshMaxWait,
                    taskStatusCodec,
                    summaryInfoFetcher,
                    executor,
                    httpClient,
                    maxErrorDuration,
                    errorScheduledExecutor,
                    stats);

            this.taskInfoFetcher = new TaskInfoFetcher(
                    this::fail,
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
                    stats);

            taskStatusFetcher.addStateChangeListener(newStatus -> {
                TaskState state = newStatus.getState();
                if (state.isDone()) {
                    cleanUpTask();
                }
                else {
                    partitionedSplitCountTracker.setPartitionedSplits(getPartitionedSplitsInfo());
                    updateSplitQueueSpace();
                }
            });

            this.outboundSummaryInfoCollector = new VersionedSummaryInfoCollector(this::triggerUpdate);
            dynamicFilterService.registerDynamicFilterConsumer(
                    taskId.getQueryId(),
                    taskId.getAttemptId(),
                    outboundDynamicFilterIds,
                    dynamicFilterDomains -> outboundSummaryInfoCollector.updateSummary(new DynamicFilterSummary(dynamicFilterDomains)));

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
            scheduleUpdate();

            summaryInfoFetcher.start();
            taskStatusFetcher.start();
            taskInfoFetcher.start();
        }
    }

    @Override
    public synchronized void addSplits(Multimap<PlanNodeId, Split> splitsBySource)
    {
        requireNonNull(splitsBySource, "splitsBySource is null");

        // only add pending split if not done
        if (getTaskStatus().getState().isDone()) {
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
        if (noMoreSplits.containsKey(sourceId)) {
            return;
        }

        noMoreSplits.put(sourceId, true);
        triggerUpdate();
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId, Lifespan lifespan)
    {
        if (pendingNoMoreSplitsForLifespan.put(sourceId, lifespan)) {
            triggerUpdate();
        }
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        if (newOutputBuffers.getVersion() > outputBuffers.get().getVersion()) {
            outputBuffers.set(newOutputBuffers);
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
        if (taskStatus.getState().isDone()) {
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

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getPendingSourceSplitCount()
    {
        return pendingSourceSplitCount;
    }

    private long getQueuedPartitionedSplitsWeight()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
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
            for (Lifespan lifespan : assignment.getNoMoreSplitsForLifespan()) {
                pendingNoMoreSplitsForLifespan.remove(planNodeId, lifespan);
            }
            if (isPartitionedSource) {
                pendingSourceSplitCount -= removed;
                pendingSourceSplitsWeight -= removedWeight;
            }
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

    private synchronized void triggerUpdate()
    {
        // synchronized so that needsUpdate is not cleared in sendUpdate before actual request is sent
        needsUpdate.set(true);
        sendUpdate();
    }

    private synchronized void sendUpdate()
    {
        TaskStatus taskStatus = getTaskStatus();
        // don't update if the task hasn't been started yet or if it is already finished
        if (!started.get() || !needsUpdate.get() || taskStatus.getState().isDone()) {
            return;
        }

        // if there is a request already running, wait for it to complete
        // currentRequest is always cleared when request is complete
        if (currentRequest != null) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<Void> errorRateLimit = updateErrorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendUpdate, executor);
            return;
        }

        List<SplitAssignment> splitAssignments = getSplitAssignments();
        VersionedSummaryInfo versionedSummaryInfo = outboundSummaryInfoCollector.acknowledgeAndGetNewSummaryInfo(sentSummaryStatsVersion);
        DynamicFilterSummary dynamicFilterSummary = (DynamicFilterSummary) versionedSummaryInfo.getSummaryInfo().stream()
                .filter(summaryInfo -> summaryInfo.getType() == DYNAMIC_FILTER)
                .collect(onlyElement());

        // Workers don't need the embedded JSON representation when the fragment is sent
        Optional<PlanFragment> fragment = sendPlan.get() ? Optional.of(planFragment.withoutEmbeddedJsonRepresentation()) : Optional.empty();
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                splitAssignments,
                outputBuffers.get(),
                dynamicFilterSummary.getDynamicFilterDomains());
        byte[] taskUpdateRequestJson = taskUpdateRequestCodec.toJsonBytes(updateRequest);
        if (fragment.isPresent()) {
            stats.updateWithPlanBytes(taskUpdateRequestJson.length);
        }
        if (!dynamicFilterSummary.isEmpty()) {
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
        currentRequest = future;
        currentRequestStartNanos = System.nanoTime();

        // The needsUpdate flag needs to be set to false BEFORE adding the Future callback since callback might change the flag value
        // and does so without grabbing the instance lock.
        needsUpdate.set(false);

        Futures.addCallback(
                future,
                new SimpleHttpResponseHandler<>(new UpdateResponseHandler(splitAssignments, versionedSummaryInfo.getVersion()), request.getUri(), stats),
                executor);
    }

    private synchronized List<SplitAssignment> getSplitAssignments()
    {
        return Stream.concat(planFragment.getPartitionedSourceNodes().stream(), planFragment.getRemoteSourceNodes().stream())
                .filter(Objects::nonNull)
                .map(PlanNode::getId)
                .map(this::getSplitAssignment)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private synchronized SplitAssignment getSplitAssignment(PlanNodeId planNodeId)
    {
        Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
        boolean pendingNoMoreSplits = Boolean.TRUE.equals(this.noMoreSplits.get(planNodeId));
        boolean noMoreSplits = this.noMoreSplits.containsKey(planNodeId);
        Set<Lifespan> noMoreSplitsForLifespan = pendingNoMoreSplitsForLifespan.get(planNodeId);

        SplitAssignment assignment = null;
        if (!splits.isEmpty() || !noMoreSplitsForLifespan.isEmpty() || pendingNoMoreSplits) {
            assignment = new SplitAssignment(planNodeId, splits, noMoreSplitsForLifespan, noMoreSplits);
        }
        return assignment;
    }

    @Override
    public synchronized void cancel()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                return;
            }

            // send cancel to task and ignore response
            scheduleAsyncCleanupRequest(new Backoff(maxErrorDuration), "cancel", false);
        }
    }

    private synchronized void cleanUpTask()
    {
        checkState(getTaskStatus().getState().isDone(), "attempt to clean up a task that is not done yet");

        // clear pending splits to free memory
        pendingSplits.clear();
        pendingSourceSplitCount = 0;
        pendingSourceSplitsWeight = 0;
        partitionedSplitCountTracker.setPartitionedSplits(PartitionedSplitsInfo.forZeroSplits());
        splitQueueHasSpace = true;
        whenSplitQueueHasSpace.complete(null, executor);

        // clear pending outbound summary stats to free memory
        outboundSummaryInfoCollector.acknowledge(Long.MAX_VALUE);

        // cancel pending request
        if (currentRequest != null) {
            currentRequest.cancel(true);
            currentRequest = null;
            currentRequestStartNanos = 0;
        }

        taskStatusFetcher.stop();
        summaryInfoFetcher.stop();

        // The remote task is likely to get a delete from the PageBufferClient first.
        // We send an additional delete anyway to get the final TaskInfo
        scheduleAsyncCleanupRequest(new Backoff(maxErrorDuration), "cleanup", true);
    }

    @Override
    public synchronized void abort()
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        TaskStatus status = failWith(getTaskStatus(), ABORTED, ImmutableList.of());
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            taskStatusFetcher.updateTaskStatus(status);
            // send abort to task
            scheduleAsyncCleanupRequest(new Backoff(maxErrorDuration), "abort", true);
        }
    }

    private void scheduleAsyncCleanupRequest(Backoff cleanupBackoff, String action, boolean abort)
    {
        if (!aborting.compareAndSet(false, true)) {
            // Do not initiate another round of cleanup requests if one had been initiated.
            // Otherwise, we can get into an asynchronous recursion here. For example, when aborting a task after REMOTE_TASK_MISMATCH.
            return;
        }

        HttpUriBuilder uriBuilder = getHttpUriBuilder(getTaskStatus()).addParameter("abort", "" + abort);
        Request request = prepareDelete()
                .setUri(uriBuilder.build())
                .build();
        doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
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
                    if (!getTaskInfo().getTaskStatus().getState().isDone()) {
                        cleanUpLocally();
                    }
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                if (t instanceof RejectedExecutionException && httpClient.isClosed()) {
                    logError(t, "Unable to %s task at %s. HTTP client is closed.", action, request.getUri());
                    cleanUpLocally();
                    return;
                }

                // record failure
                if (cleanupBackoff.failure()) {
                    logError(t, "Unable to %s task at %s. Back off depleted.", action, request.getUri());
                    cleanUpLocally();
                    return;
                }

                // final task info is set
                if (taskInfoFetcher.getTaskInfo().getTaskStatus().getState().isDone()) {
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
    @Override
    public synchronized void fail(Throwable cause)
    {
        TaskStatus taskStatus = getTaskStatus();
        if (!taskStatus.getState().isDone()) {
            log.debug(cause, "Remote task %s failed with %s", taskStatus.getSelf(), cause);
        }

        TaskStatus status = failWith(getTaskStatus(), FAILED, ImmutableList.of(toFailure(cause)));
        taskStatusFetcher.updateTaskStatus(status);

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            if (cause instanceof TrinoTransportException) {
                // task is unreachable
                cleanUpLocally();
            }
            else {
                // send abort to task
                scheduleAsyncCleanupRequest(new Backoff(maxErrorDuration), "abort", true);
            }
        }
    }

    private void cleanUpLocally()
    {
        // Update the taskInfo with the new taskStatus.

        // Generally, we send a cleanup request to the worker, and update the TaskInfo on
        // the coordinator based on what we fetched from the worker. If we somehow cannot
        // get the cleanup request to the worker, the TaskInfo that we fetch for the worker
        // likely will not say the task is done however many times we try. In this case,
        // we have to set the local query info directly so that we stop trying to fetch
        // updated TaskInfo from the worker. This way, the task on the worker eventually
        // expires due to lack of activity.

        // This is required because the query state machine depends on TaskInfo (instead of task status)
        // to transition its own state.
        // TODO: Update the query state machine and stage state machine to depend on TaskStatus instead

        // Since this TaskInfo is updated in the client the "complete" flag will not be set,
        // indicating that the stats may not reflect the final stats on the worker.
        updateTaskInfo(getTaskInfo().withTaskStatus(getTaskStatus()));
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

        private UpdateResponseHandler(List<SplitAssignment> splitAssignments, long currentRequestDynamicFiltersVersion)
        {
            this.splitAssignments = ImmutableList.copyOf(requireNonNull(splitAssignments, "splitAssignments is null"));
            this.currentRequestDynamicFiltersVersion = currentRequestDynamicFiltersVersion;
        }

        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    long currentRequestStartNanos;
                    synchronized (HttpRemoteTask.this) {
                        currentRequest = null;
                        sendPlan.set(value.isNeedsPlan());
                        currentRequestStartNanos = HttpRemoteTask.this.currentRequestStartNanos;
                        sentSummaryStatsVersion = currentRequestDynamicFiltersVersion;
                    }
                    // Remove summary stats which were successfully sent to free up memory
                    outboundSummaryInfoCollector.acknowledge(currentRequestDynamicFiltersVersion);
                    updateStats(currentRequestStartNanos);
                    processTaskUpdate(value, splitAssignments);
                    updateErrorTracker.requestSucceeded();
                }
                finally {
                    sendUpdate();
                }
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    long currentRequestStartNanos;
                    synchronized (HttpRemoteTask.this) {
                        currentRequest = null;
                        currentRequestStartNanos = HttpRemoteTask.this.currentRequestStartNanos;
                    }
                    updateStats(currentRequestStartNanos);

                    // on failure assume we need to update again
                    needsUpdate.set(true);

                    // if task not already done, record error
                    TaskStatus taskStatus = getTaskStatus();
                    if (!taskStatus.getState().isDone()) {
                        updateErrorTracker.requestFailed(cause);
                    }
                }
                catch (Error e) {
                    fail(e);
                    throw e;
                }
                catch (RuntimeException e) {
                    fail(e);
                }
                finally {
                    sendUpdate();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                fail(cause);
            }
        }

        private void updateStats(long currentRequestStartNanos)
        {
            Duration requestRoundTrip = Duration.nanosSince(currentRequestStartNanos);
            stats.updateRoundTripMillis(requestRoundTrip.toMillis());
        }
    }
}
