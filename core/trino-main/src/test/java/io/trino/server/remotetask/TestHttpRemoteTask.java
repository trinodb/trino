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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.jaxrs.JsonMapper;
import io.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.block.BlockJsonSerde;
import io.trino.client.NodeVersion;
import io.trino.execution.BaseTestSqlTaskManager;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.RemoteTask;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.TaskTestUtils;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.Split;
import io.trino.server.DynamicFilterService;
import io.trino.server.FailTaskRequest;
import io.trino.server.HttpRemoteTaskFactory;
import io.trino.server.TaskUpdateRequest;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingSplit;
import io.trino.type.TypeDeserializer;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriInfo;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.tracing.SpanSerialization.SpanDeserializer;
import static io.airlift.tracing.SpanSerialization.SpanSerializer;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.REMOTE_TASK_ADAPTIVE_UPDATE_REQUEST_SIZE_ENABLED;
import static io.trino.SystemSessionProperties.REMOTE_TASK_GUARANTEED_SPLITS_PER_REQUEST;
import static io.trino.SystemSessionProperties.REMOTE_TASK_MAX_REQUEST_SIZE;
import static io.trino.SystemSessionProperties.REMOTE_TASK_REQUEST_SIZE_HEADROOM;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.BROADCAST;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.server.InternalHeaders.TRINO_CURRENT_VERSION;
import static io.trino.server.InternalHeaders.TRINO_MAX_WAIT;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHttpRemoteTask
{
    // This 30 sec per-test timeout should never be reached because the test should fail and do proper cleanup after 20 sec.
    private static final Duration POLL_TIMEOUT = new Duration(100, MILLISECONDS);
    private static final Duration IDLE_TIMEOUT = new Duration(3, SECONDS);
    private static final Duration FAIL_TIMEOUT = new Duration(20, SECONDS);
    private static final TaskManagerConfig TASK_MANAGER_CONFIG = new TaskManagerConfig()
            // Shorten status refresh wait and info update interval so that we can have a shorter test timeout
            .setStatusRefreshMaxWait(new Duration(IDLE_TIMEOUT.roundTo(MILLISECONDS) / 100.0, MILLISECONDS))
            .setInfoUpdateInterval(new Duration(IDLE_TIMEOUT.roundTo(MILLISECONDS) / 10.0, MILLISECONDS));

    private static final boolean TRACE_HTTP = false;

    @Test(timeOut = 30000)
    public void testRemoteTaskMismatch()
            throws Exception
    {
        runTest(FailureScenario.TASK_MISMATCH);
    }

    @Test(timeOut = 30000)
    public void testRejectedExecutionWhenVersionIsHigh()
            throws Exception
    {
        runTest(FailureScenario.TASK_MISMATCH_WHEN_VERSION_IS_HIGH);
    }

    @Test(timeOut = 30000)
    public void testRejectedExecution()
            throws Exception
    {
        runTest(FailureScenario.REJECTED_EXECUTION);
    }

    @Test(timeOut = 30000)
    public void testRegular()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource);

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, ImmutableSet.of());

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        remoteTask.addSplits(ImmutableMultimap.of(TABLE_SCAN_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit())));
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID) != null);
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).getSplits().size() == 1);

        remoteTask.noMoreSplits(TABLE_SCAN_NODE_ID);
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).isNoMoreSplits());

        remoteTask.cancel();
        poll(() -> remoteTask.getTaskStatus().getState().isDone());
        poll(() -> remoteTask.getTaskInfo().getTaskStatus().getState().isDone());

        httpRemoteTaskFactory.stop();
    }

    @Test(timeOut = 30000)
    public void testDynamicFilters()
            throws Exception
    {
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", BIGINT);
        Symbol symbol2 = symbolAllocator.newSymbol("DF_SYMBOL2", BIGINT);
        SymbolReference df1 = symbol1.toSymbolReference();
        SymbolReference df2 = symbol2.toSymbolReference();
        ColumnHandle handle1 = new TestingColumnHandle("column1");
        ColumnHandle handle2 = new TestingColumnHandle("column2");
        QueryId queryId = new QueryId("test");

        TestingTaskResource testingTaskResource = new TestingTaskResource(new AtomicLong(System.nanoTime()), FailureScenario.NO_FAILURE);
        DynamicFilterService dynamicFilterService = new DynamicFilterService(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                new TypeOperators(),
                new DynamicFilterConfig());
        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource, dynamicFilterService);
        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, ImmutableSet.of());

        Map<DynamicFilterId, Domain> initialDomain = ImmutableMap.of(
                filterId1,
                Domain.singleValue(BIGINT, 1L));
        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        testingTaskResource.setDynamicFilterDomains(new VersionedDynamicFilterDomains(1L, initialDomain));
        dynamicFilterService.registerQuery(
                queryId,
                TEST_SESSION,
                ImmutableSet.of(filterId1, filterId2),
                ImmutableSet.of(filterId1, filterId2),
                ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(new StageId(queryId, 1), 0, 1);

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId2, df2)),
                ImmutableMap.of(
                        symbol1, handle1,
                        symbol2, handle2),
                symbolAllocator.getTypes());

        // make sure initial dynamic filters are collected
        CompletableFuture<?> future = dynamicFilter.isBlocked();
        remoteTask.start();
        future.get();

        assertEquals(
                dynamicFilter.getCurrentPredicate(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        handle1, Domain.singleValue(BIGINT, 1L))));
        assertEquals(testingTaskResource.getDynamicFiltersFetchCounter(), 1);

        // make sure dynamic filters are not collected for every status update
        assertEventually(
                new Duration(15, SECONDS),
                () -> assertGreaterThanOrEqual(testingTaskResource.getStatusFetchCounter(), 3L));
        assertEquals(testingTaskResource.getDynamicFiltersFetchCounter(), 1L, testingTaskResource.getDynamicFiltersFetchRequests().toString());

        future = dynamicFilter.isBlocked();
        testingTaskResource.setDynamicFilterDomains(new VersionedDynamicFilterDomains(
                2L,
                ImmutableMap.of(filterId2, Domain.singleValue(BIGINT, 2L))));
        future.get();
        assertEquals(
                dynamicFilter.getCurrentPredicate(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        handle1, Domain.singleValue(BIGINT, 1L),
                        handle2, Domain.singleValue(BIGINT, 2L))));
        assertEquals(testingTaskResource.getDynamicFiltersFetchCounter(), 2L, testingTaskResource.getDynamicFiltersFetchRequests().toString());
        assertGreaterThanOrEqual(testingTaskResource.getStatusFetchCounter(), 4L);

        httpRemoteTaskFactory.stop();
    }

    @Test(timeOut = 30_000)
    public void testOutboundDynamicFilters()
            throws Exception
    {
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", BIGINT);
        Symbol symbol2 = symbolAllocator.newSymbol("DF_SYMBOL2", BIGINT);
        SymbolReference df1 = symbol1.toSymbolReference();
        SymbolReference df2 = symbol2.toSymbolReference();
        ColumnHandle handle1 = new TestingColumnHandle("column1");
        ColumnHandle handle2 = new TestingColumnHandle("column2");
        QueryId queryId = new QueryId("test");

        TestingTaskResource testingTaskResource = new TestingTaskResource(new AtomicLong(System.nanoTime()), FailureScenario.NO_FAILURE);
        DynamicFilterService dynamicFilterService = new DynamicFilterService(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                new TypeOperators(),
                new DynamicFilterConfig());
        dynamicFilterService.registerQuery(
                queryId,
                TEST_SESSION,
                ImmutableSet.of(filterId1, filterId2),
                ImmutableSet.of(filterId1, filterId2),
                ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(new StageId(queryId, 1), 0, 1);

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId2, df2)),
                ImmutableMap.of(
                        symbol1, handle1,
                        symbol2, handle2),
                symbolAllocator.getTypes());

        // make sure initial dynamic filter is collected
        CompletableFuture<?> future = dynamicFilter.isBlocked();
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(new StageId(queryId.getId(), 1), 1, 0),
                ImmutableMap.of(filterId1, Domain.singleValue(BIGINT, 1L)));
        future.get();
        assertEquals(
                dynamicFilter.getCurrentPredicate(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        handle1, Domain.singleValue(BIGINT, 1L))));

        // Create remote task after dynamic filter is created to simulate new nodes joining
        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource, dynamicFilterService);
        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, ImmutableSet.of(filterId1, filterId2));
        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();
        assertEventually(
                new Duration(10, SECONDS),
                () -> assertEquals(testingTaskResource.getDynamicFiltersSentCounter(), 1L));
        assertEquals(testingTaskResource.getCreateOrUpdateCounter(), 1L);

        // schedule a couple of splits to trigger task updates
        addSplit(remoteTask, testingTaskResource, 1);
        addSplit(remoteTask, testingTaskResource, 2);
        // make sure dynamic filter was sent in task updates only once
        assertEquals(testingTaskResource.getDynamicFiltersSentCounter(), 1L);
        assertEquals(testingTaskResource.getCreateOrUpdateCounter(), 3L);
        assertEquals(
                testingTaskResource.getLatestDynamicFilterFromCoordinator(),
                ImmutableMap.of(filterId1, Domain.singleValue(BIGINT, 1L)));

        future = dynamicFilter.isBlocked();
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(new StageId(queryId.getId(), 1), 1, 0),
                ImmutableMap.of(filterId2, Domain.singleValue(BIGINT, 2L)));
        future.get();
        assertEquals(
                dynamicFilter.getCurrentPredicate(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        handle1, Domain.singleValue(BIGINT, 1L),
                        handle2, Domain.singleValue(BIGINT, 2L))));

        // dynamic filter should be sent even though there were no further splits scheduled
        assertEventually(
                new Duration(10, SECONDS),
                () -> assertEquals(testingTaskResource.getDynamicFiltersSentCounter(), 2L));
        assertEquals(testingTaskResource.getCreateOrUpdateCounter(), 4L);
        // previously sent dynamic filter should not be repeated
        assertEquals(
                testingTaskResource.getLatestDynamicFilterFromCoordinator(),
                ImmutableMap.of(filterId2, Domain.singleValue(BIGINT, 2L)));

        httpRemoteTaskFactory.stop();
    }

    @Test(timeOut = 300000)
    public void testAdaptiveRemoteTaskRequestSize()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);

        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(REMOTE_TASK_ADAPTIVE_UPDATE_REQUEST_SIZE_ENABLED, "true")
                .setSystemProperty(REMOTE_TASK_MAX_REQUEST_SIZE, "10kB")
                .setSystemProperty(REMOTE_TASK_REQUEST_SIZE_HEADROOM, "1kB")
                .setSystemProperty(REMOTE_TASK_GUARANTEED_SPLITS_PER_REQUEST, "1")
                .build();
        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource);

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, ImmutableSet.of(), session);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        Multimap<PlanNodeId, Split> splits = HashMultimap.create();
        for (int i = 0; i < 100; i++) {
            splits.put(TABLE_SCAN_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        }
        remoteTask.addSplits(splits);

        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID) != null);

        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).getSplits().size() == 100); // to check whether all the splits are sent or not
        assertTrue(testingTaskResource.getCreateOrUpdateCounter() > 1); // to check whether the splits are divided or not

        remoteTask.noMoreSplits(TABLE_SCAN_NODE_ID);
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).isNoMoreSplits());

        remoteTask.cancel();
        poll(() -> remoteTask.getTaskStatus().getState().isDone());
        poll(() -> remoteTask.getTaskInfo().getTaskStatus().getState().isDone());

        httpRemoteTaskFactory.stop();
    }

    @Test
    public void testAdjustSplitBatchSize()
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);

        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(REMOTE_TASK_ADAPTIVE_UPDATE_REQUEST_SIZE_ENABLED, "true")
                .setSystemProperty(REMOTE_TASK_MAX_REQUEST_SIZE, "100kB")
                .setSystemProperty(REMOTE_TASK_REQUEST_SIZE_HEADROOM, "10kB")
                .setSystemProperty(REMOTE_TASK_GUARANTEED_SPLITS_PER_REQUEST, "1")
                .build();
        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource);

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, ImmutableSet.of(), session);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());

        Set<ScheduledSplit> splits = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            splits.add(new ScheduledSplit(i, TABLE_SCAN_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit())));
        }

        // decrease splitBatchSize
        assertTrue(((HttpRemoteTask) remoteTask).adjustSplitBatchSize(ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, splits, true)), 1000000, 500));
        assertLessThan(((HttpRemoteTask) remoteTask).splitBatchSize.get(), 250);

        // increase splitBatchSize
        assertFalse(((HttpRemoteTask) remoteTask).adjustSplitBatchSize(ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, splits, true)), 1000, 100));
        assertGreaterThan(((HttpRemoteTask) remoteTask).splitBatchSize.get(), 250);
    }

    private void runTest(FailureScenario failureScenario)
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, failureScenario);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource);
        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, ImmutableSet.of());

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        waitUntilIdle(lastActivityNanos);

        httpRemoteTaskFactory.stop();
        assertTrue(remoteTask.getTaskStatus().getState().isDone(), format("TaskStatus is not in a done state: %s", remoteTask.getTaskStatus()));

        ErrorCode actualErrorCode = getOnlyElement(remoteTask.getTaskStatus().getFailures()).getErrorCode();
        switch (failureScenario) {
            case TASK_MISMATCH:
            case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                assertTrue(remoteTask.getTaskInfo().getTaskStatus().getState().isDone(), format("TaskInfo is not in a done state: %s", remoteTask.getTaskInfo()));
                assertEquals(actualErrorCode, REMOTE_TASK_MISMATCH.toErrorCode());
                break;
            case REJECTED_EXECUTION:
                // for a rejection to occur, the http client must be shutdown, which means we will not be able to ge the final task info
                assertEquals(actualErrorCode, REMOTE_TASK_ERROR.toErrorCode());
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void addSplit(RemoteTask remoteTask, TestingTaskResource testingTaskResource, int expectedSplitsCount)
            throws InterruptedException
    {
        remoteTask.addSplits(ImmutableMultimap.of(TABLE_SCAN_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit())));
        // wait for splits to be received by remote task
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID) != null);
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).getSplits().size() == expectedSplitsCount);
    }

    private RemoteTask createRemoteTask(HttpRemoteTaskFactory httpRemoteTaskFactory, Set<DynamicFilterId> outboundDynamicFilterIds)
    {
        return createRemoteTask(httpRemoteTaskFactory, outboundDynamicFilterIds, TEST_SESSION);
    }

    private RemoteTask createRemoteTask(HttpRemoteTaskFactory httpRemoteTaskFactory, Set<DynamicFilterId> outboundDynamicFilterIds, Session session)
    {
        return httpRemoteTaskFactory.createRemoteTask(
                session,
                Span.getInvalid(),
                new TaskId(new StageId("test", 1), 2, 0),
                new InternalNode("node-id", URI.create("http://fake.invalid/"), new NodeVersion("version"), false),
                false,
                TaskTestUtils.PLAN_FRAGMENT,
                ImmutableMultimap.of(),
                PipelinedOutputBuffers.createInitial(BROADCAST),
                new NodeTaskMap.PartitionedSplitCountTracker(i -> {}),
                outboundDynamicFilterIds,
                Optional.empty(),
                true);
    }

    private static HttpRemoteTaskFactory createHttpRemoteTaskFactory(TestingTaskResource testingTaskResource)
    {
        return createHttpRemoteTaskFactory(testingTaskResource, new DynamicFilterService(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                new TypeOperators(),
                new DynamicFilterConfig()));
    }

    private static HttpRemoteTaskFactory createHttpRemoteTaskFactory(TestingTaskResource testingTaskResource, DynamicFilterService dynamicFilterService)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new HandleJsonModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(JsonMapper.class).in(SINGLETON);
                        binder.bind(Metadata.class).toInstance(createTestMetadataManager());
                        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
                        jsonCodecBinder(binder).bindJsonCodec(VersionedDynamicFilterDomains.class);
                        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
                        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
                        jsonCodecBinder(binder).bindJsonCodec(FailTaskRequest.class);

                        binder.bind(TypeManager.class).toInstance(TESTING_TYPE_MANAGER);
                        binder.bind(BlockEncodingManager.class).in(SINGLETON);
                        binder.bind(BlockEncodingSerde.class).to(InternalBlockEncodingSerde.class).in(SINGLETON);

                        binder.bind(OpenTelemetry.class).toInstance(OpenTelemetry.noop());
                        jsonBinder(binder).addSerializerBinding(Span.class).to(SpanSerializer.class);
                        jsonBinder(binder).addDeserializerBinding(Span.class).to(SpanDeserializer.class);
                    }

                    @Provides
                    private HttpRemoteTaskFactory createHttpRemoteTaskFactory(
                            JsonMapper jsonMapper,
                            JsonCodec<TaskStatus> taskStatusCodec,
                            JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec,
                            JsonCodec<TaskInfo> taskInfoCodec,
                            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
                            JsonCodec<FailTaskRequest> failTaskRequestCodec)
                    {
                        JaxrsTestingHttpProcessor jaxrsTestingHttpProcessor = new JaxrsTestingHttpProcessor(URI.create("http://fake.invalid/"), testingTaskResource, jsonMapper);
                        TestingHttpClient testingHttpClient = new TestingHttpClient(jaxrsTestingHttpProcessor.setTrace(TRACE_HTTP));
                        testingTaskResource.setHttpClient(testingHttpClient);
                        return new HttpRemoteTaskFactory(
                                new QueryManagerConfig(),
                                TASK_MANAGER_CONFIG,
                                testingHttpClient,
                                new BaseTestSqlTaskManager.MockLocationFactory(),
                                taskStatusCodec,
                                dynamicFilterDomainsCodec,
                                taskInfoCodec,
                                taskUpdateRequestCodec,
                                failTaskRequestCodec,
                                noopTracer(),
                                new RemoteTaskStats(),
                                dynamicFilterService);
                    }
                });
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(HttpRemoteTaskFactory.class);
    }

    private static void poll(BooleanSupplier success)
            throws InterruptedException
    {
        long failAt = System.nanoTime() + FAIL_TIMEOUT.roundTo(NANOSECONDS);

        while (!success.getAsBoolean()) {
            long millisUntilFail = (failAt - System.nanoTime()) / 1_000_000;
            if (millisUntilFail <= 0) {
                throw new AssertionError(format("Timeout of %s reached", FAIL_TIMEOUT));
            }
            Thread.sleep(min(POLL_TIMEOUT.toMillis(), millisUntilFail));
        }
    }

    private static void waitUntilIdle(AtomicLong lastActivityNanos)
            throws InterruptedException
    {
        long startTimeNanos = System.nanoTime();

        while (true) {
            long millisSinceLastActivity = (System.nanoTime() - lastActivityNanos.get()) / 1_000_000L;
            long millisSinceStart = (System.nanoTime() - startTimeNanos) / 1_000_000L;
            long millisToIdleTarget = IDLE_TIMEOUT.toMillis() - millisSinceLastActivity;
            long millisToFailTarget = FAIL_TIMEOUT.toMillis() - millisSinceStart;
            if (millisToFailTarget < millisToIdleTarget) {
                throw new AssertionError(format("Activity doesn't stop after %s", FAIL_TIMEOUT));
            }
            if (millisToIdleTarget < 0) {
                return;
            }
            Thread.sleep(millisToIdleTarget);
        }
    }

    private enum FailureScenario
    {
        NO_FAILURE,
        TASK_MISMATCH,
        TASK_MISMATCH_WHEN_VERSION_IS_HIGH,
        REJECTED_EXECUTION,
    }

    @Path("/task/{nodeId}")
    public static class TestingTaskResource
    {
        private static final String INITIAL_TASK_INSTANCE_ID = "task-instance-id";
        private static final String NEW_TASK_INSTANCE_ID = "task-instance-id-x";

        private final AtomicLong lastActivityNanos;
        private final FailureScenario failureScenario;

        private final AtomicReference<TestingHttpClient> httpClient = new AtomicReference<>();

        private TaskInfo initialTaskInfo;
        private TaskStatus initialTaskStatus;
        private Optional<VersionedDynamicFilterDomains> dynamicFilterDomains = Optional.empty();
        private long version;
        private TaskState taskState;
        private String taskInstanceId = INITIAL_TASK_INSTANCE_ID;
        private Map<DynamicFilterId, Domain> latestDynamicFilterFromCoordinator = ImmutableMap.of();

        private long statusFetchCounter;
        private long createOrUpdateCounter;
        private long dynamicFiltersFetchCounter;
        private long dynamicFiltersSentCounter;
        private final List<DynamicFiltersFetchRequest> dynamicFiltersFetchRequests = new ArrayList<>();

        public TestingTaskResource(AtomicLong lastActivityNanos, FailureScenario failureScenario)
        {
            this.lastActivityNanos = requireNonNull(lastActivityNanos, "lastActivityNanos is null");
            this.failureScenario = requireNonNull(failureScenario, "failureScenario is null");
        }

        public void setHttpClient(TestingHttpClient newValue)
        {
            httpClient.set(newValue);
        }

        @GET
        @Path("{taskId}")
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskInfo getTaskInfo(
                @PathParam("taskId") TaskId taskId,
                @HeaderParam(TRINO_CURRENT_VERSION) Long currentVersion,
                @HeaderParam(TRINO_MAX_WAIT) Duration maxWait,
                @Context UriInfo uriInfo)
        {
            lastActivityNanos.set(System.nanoTime());
            return buildTaskInfo();
        }

        Map<PlanNodeId, SplitAssignment> taskSplitAssignmentMap = new HashMap<>();

        @POST
        @Path("{taskId}")
        @Consumes(MediaType.APPLICATION_JSON)
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskInfo createOrUpdateTask(
                @PathParam("taskId") TaskId taskId,
                TaskUpdateRequest taskUpdateRequest,
                @Context UriInfo uriInfo)
        {
            for (SplitAssignment splitAssignment : taskUpdateRequest.getSplitAssignments()) {
                taskSplitAssignmentMap.compute(splitAssignment.getPlanNodeId(), (planNodeId, taskSplitAssignment) -> taskSplitAssignment == null ? splitAssignment : taskSplitAssignment.update(splitAssignment));
            }
            if (!taskUpdateRequest.getDynamicFilterDomains().isEmpty()) {
                dynamicFiltersSentCounter++;
                latestDynamicFilterFromCoordinator = taskUpdateRequest.getDynamicFilterDomains();
            }
            createOrUpdateCounter++;
            lastActivityNanos.set(System.nanoTime());
            return buildTaskInfo();
        }

        public synchronized SplitAssignment getTaskSplitAssignment(PlanNodeId planNodeId)
        {
            SplitAssignment assignment = taskSplitAssignmentMap.get(planNodeId);
            if (assignment == null) {
                return null;
            }
            return new SplitAssignment(assignment.getPlanNodeId(), assignment.getSplits(), assignment.isNoMoreSplits());
        }

        @GET
        @Path("{taskId}/status")
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskStatus getTaskStatus(
                @PathParam("taskId") TaskId taskId,
                @HeaderParam(TRINO_CURRENT_VERSION) Long currentVersion,
                @HeaderParam(TRINO_MAX_WAIT) Duration maxWait,
                @Context UriInfo uriInfo)
                throws InterruptedException
        {
            lastActivityNanos.set(System.nanoTime());

            wait(maxWait.roundTo(MILLISECONDS));
            return buildTaskStatus();
        }

        @GET
        @Path("{taskId}/dynamicfilters")
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized VersionedDynamicFilterDomains acknowledgeAndGetNewDynamicFilterDomains(
                @PathParam("taskId") TaskId taskId,
                @HeaderParam(TRINO_CURRENT_VERSION) Long currentDynamicFiltersVersion,
                @Context UriInfo uriInfo)
        {
            dynamicFiltersFetchCounter++;
            // keep incoming dynamicfilters request log for debugging purposes
            dynamicFiltersFetchRequests.add(new DynamicFiltersFetchRequest(
                    uriInfo.getRequestUri().toString(),
                    taskId,
                    currentDynamicFiltersVersion,
                    dynamicFilterDomains
                            .map(VersionedDynamicFilterDomains::getVersion)
                            .orElse(-1L)));
            return dynamicFilterDomains.orElse(null);
        }

        @DELETE
        @Path("{taskId}")
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskInfo deleteTask(
                @PathParam("taskId") TaskId taskId,
                @QueryParam("abort") @DefaultValue("true") boolean abort,
                @Context UriInfo uriInfo)
        {
            lastActivityNanos.set(System.nanoTime());

            taskState = abort ? TaskState.ABORTED : TaskState.CANCELED;
            return buildTaskInfo();
        }

        public void setInitialTaskInfo(TaskInfo initialTaskInfo)
        {
            this.initialTaskInfo = initialTaskInfo;
            this.initialTaskStatus = initialTaskInfo.getTaskStatus();
            this.taskState = initialTaskStatus.getState();
            this.version = initialTaskStatus.getVersion();
            switch (failureScenario) {
                case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                    // Make the initial version large enough.
                    // This way, the version number can't be reached if it is reset to 0.
                    version = 1_000_000;
                    break;
                case TASK_MISMATCH:
                case REJECTED_EXECUTION:
                case NO_FAILURE:
                    break; // do nothing
                default:
                    throw new UnsupportedOperationException();
            }
        }

        public synchronized void setDynamicFilterDomains(VersionedDynamicFilterDomains dynamicFilterDomains)
        {
            this.dynamicFilterDomains = Optional.of(dynamicFilterDomains);
        }

        public Map<DynamicFilterId, Domain> getLatestDynamicFilterFromCoordinator()
        {
            return latestDynamicFilterFromCoordinator;
        }

        public synchronized long getStatusFetchCounter()
        {
            return statusFetchCounter;
        }

        public synchronized long getCreateOrUpdateCounter()
        {
            return createOrUpdateCounter;
        }

        public synchronized long getDynamicFiltersFetchCounter()
        {
            return dynamicFiltersFetchCounter;
        }

        public synchronized long getDynamicFiltersSentCounter()
        {
            return dynamicFiltersSentCounter;
        }

        public synchronized List<DynamicFiltersFetchRequest> getDynamicFiltersFetchRequests()
        {
            return ImmutableList.copyOf(dynamicFiltersFetchRequests);
        }

        private TaskInfo buildTaskInfo()
        {
            return new TaskInfo(
                    buildTaskStatus(),
                    initialTaskInfo.getLastHeartbeat(),
                    initialTaskInfo.getOutputBuffers(),
                    initialTaskInfo.getNoMoreSplits(),
                    initialTaskInfo.getStats(),
                    initialTaskInfo.getEstimatedMemory(),
                    initialTaskInfo.isNeedsPlan());
        }

        private TaskStatus buildTaskStatus()
        {
            statusFetchCounter++;
            // Change the task instance id after 10th fetch to simulate worker restart
            switch (failureScenario) {
                case TASK_MISMATCH:
                case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                    if (statusFetchCounter == 10) {
                        taskInstanceId = NEW_TASK_INSTANCE_ID;
                        version = 0;
                    }
                    break;
                case REJECTED_EXECUTION:
                    if (statusFetchCounter >= 10) {
                        httpClient.get().close();
                        throw new RejectedExecutionException();
                    }
                    break;
                case NO_FAILURE:
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            return new TaskStatus(
                    initialTaskStatus.getTaskId(),
                    taskInstanceId,
                    ++version,
                    taskState,
                    initialTaskStatus.getSelf(),
                    "fake",
                    false,
                    initialTaskStatus.getFailures(),
                    initialTaskStatus.getQueuedPartitionedDrivers(),
                    initialTaskStatus.getRunningPartitionedDrivers(),
                    initialTaskStatus.getOutputBufferStatus(),
                    initialTaskStatus.getOutputDataSize(),
                    initialTaskStatus.getWriterInputDataSize(),
                    initialTaskStatus.getPhysicalWrittenDataSize(),
                    initialTaskStatus.getMaxWriterCount(),
                    initialTaskStatus.getMemoryReservation(),
                    initialTaskStatus.getPeakMemoryReservation(),
                    initialTaskStatus.getRevocableMemoryReservation(),
                    initialTaskStatus.getFullGcCount(),
                    initialTaskStatus.getFullGcTime(),
                    dynamicFilterDomains.map(VersionedDynamicFilterDomains::getVersion).orElse(INITIAL_DYNAMIC_FILTERS_VERSION),
                    initialTaskStatus.getQueuedPartitionedSplitsWeight(),
                    initialTaskStatus.getRunningPartitionedSplitsWeight());
        }

        private static class DynamicFiltersFetchRequest
        {
            private final String uriInfo;
            private final TaskId taskId;
            private final Long currentDynamicFiltersVersion;
            private final long storedDynamicFiltersVersion;

            private DynamicFiltersFetchRequest(
                    String uriInfo,
                    TaskId taskId,
                    Long currentDynamicFiltersVersion,
                    long storedDynamicFiltersVersion)
            {
                this.uriInfo = requireNonNull(uriInfo, "uriInfo is null");
                this.taskId = requireNonNull(taskId, "taskId is null");
                this.currentDynamicFiltersVersion = requireNonNull(currentDynamicFiltersVersion, "currentDynamicFiltersVersion is null");
                this.storedDynamicFiltersVersion = storedDynamicFiltersVersion;
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("uriInfo", uriInfo)
                        .add("taskId", taskId)
                        .add("currentDynamicFiltersVersion", currentDynamicFiltersVersion)
                        .add("storedDynamicFiltersVersion", storedDynamicFiltersVersion)
                        .toString();
            }
        }
    }
}
