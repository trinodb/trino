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
package io.trino.server;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.Session;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.LocationFactory;
import io.trino.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.operator.ForScheduler;
import io.trino.server.remotetask.HttpRemoteTask;
import io.trino.server.remotetask.RemoteTaskStats;
import io.trino.server.remotetask.TaskStatusStreamClient;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final Optional<NonEvictableLoadingCache<Worker, TaskStatusStreamClient>> statusStreams;
    private final HttpClient httpClient;
    private final LocationFactory locationFactory;
    private final JsonCodec<TaskStatus> taskStatusCodec;
    private final JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<FailTaskRequest> failTaskRequestCoded;
    private final Duration maxErrorDuration;
    private final Duration taskStatusRefreshMaxWait;
    private final Duration taskInfoUpdateInterval;
    private final Duration taskTerminationTimeout;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final Tracer tracer;
    private final RemoteTaskStats stats;
    private final DynamicFilterService dynamicFilterService;

    @Inject
    public HttpRemoteTaskFactory(
            QueryManagerConfig config,
            TaskManagerConfig taskConfig,
            @ForScheduler HttpClient httpClient,
            @ForScheduler HttpClientConfig httpConfig,
            LocationFactory locationFactory,
            JsonCodec<TaskStatus> taskStatusCodec,
            JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            JsonCodec<FailTaskRequest> failTaskRequestCoded,
            Tracer tracer,
            RemoteTaskStats stats,
            DynamicFilterService dynamicFilterService)
    {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;
        this.taskStatusCodec = taskStatusCodec;
        this.dynamicFilterDomainsCodec = dynamicFilterDomainsCodec;
        this.taskInfoCodec = taskInfoCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.failTaskRequestCoded = failTaskRequestCoded;
        this.maxErrorDuration = config.getRemoteTaskMaxErrorDuration();
        this.taskStatusRefreshMaxWait = taskConfig.getStatusRefreshMaxWait();
        this.taskInfoUpdateInterval = taskConfig.getInfoUpdateInterval();
        this.taskTerminationTimeout = taskConfig.getTaskTerminationTimeout();
        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = new BoundedExecutor(coreExecutor, config.getRemoteTaskMaxCallbackThreads());
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) coreExecutor);
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");

        // Keep at least one connection available for commands when using HTTP/1.1.
        if (taskConfig.isStatusStreamingEnabled() && httpConfig.getMaxConnectionsPerServer() > 1) {
            statusStreams = Optional.of(buildNonEvictableCache(CacheBuilder.newBuilder().weakValues(), CacheLoader.from(
                    worker -> new TaskStatusStreamClient(worker.uri(), worker.nodeInstanceId(), httpClient, taskStatusRefreshMaxWait, taskInfoUpdateInterval, taskStatusCodec, stats))));
        }
        else {
            statusStreams = Optional.empty();
        }

        this.updateScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("task-info-update-scheduler-%s"));
        this.errorScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("remote-task-error-delay-%s"));
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @PreDestroy
    public void stop()
    {
        statusStreams.ifPresent(streams -> streams.asMap().values().forEach(TaskStatusStreamClient::close));
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
        errorScheduledExecutor.shutdownNow();
    }

    @Override
    public HttpRemoteTask createRemoteTask(
            Session session,
            Span stageSpan,
            TaskId taskId,
            InternalNode node,
            boolean speculative,
            PlanFragment fragment,
            Map<PlanNodeId, ConnectorTableCredentials> tableCredentials,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            Set<DynamicFilterId> outboundDynamicFilterIds,
            Optional<DataSize> estimatedMemory,
            boolean summarizeTaskInfo)
    {
        URI location = locationFactory.createTaskLocation(node, taskId);
        return new HttpRemoteTask(
                session,
                stageSpan,
                taskId,
                node,
                speculative,
                location,
                fragment,
                tableCredentials,
                initialSplits,
                outputBuffers,
                httpClient,
                executor,
                updateScheduledExecutor,
                errorScheduledExecutor,
                maxErrorDuration,
                taskStatusRefreshMaxWait,
                taskInfoUpdateInterval,
                taskTerminationTimeout,
                summarizeTaskInfo,
                taskStatusCodec,
                statusStreams.map(streams -> streams.getUnchecked(new Worker(location.resolve("status-stream/"), node.getInstanceId()))),
                dynamicFilterDomainsCodec,
                taskInfoCodec,
                taskUpdateRequestCodec,
                failTaskRequestCoded,
                partitionedSplitCountTracker,
                tracer,
                stats,
                dynamicFilterService,
                outboundDynamicFilterIds,
                estimatedMemory);
    }

    private record Worker(URI uri, String nodeInstanceId) {}
}
