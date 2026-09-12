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
import io.airlift.jaxrs.JaxRsJsonMapper;
import io.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.tracing.SpanSerialization.SpanDeserializer;
import io.airlift.tracing.SpanSerialization.SpanSerializer;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.block.BlockJsonSerde;
import io.trino.connector.TestingColumnHandle;
import io.trino.cost.StatsAndCosts;
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
import io.trino.metadata.Metadata;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.server.DynamicFilterService;
import io.trino.server.FailTaskRequest;
import io.trino.server.HttpRemoteTaskFactory;
import io.trino.server.TaskUpdateRequest;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.spi.ErrorCode;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContribution;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContributionBatch;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintHub;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProtocol;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload;
import io.trino.testing.TestingSplit;
import io.trino.type.TypeDescriptorDeserializer;
import io.trino.type.TypeDescriptorKeyDeserializer;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.REMOTE_TASK_ADAPTIVE_UPDATE_REQUEST_SIZE_ENABLED;
import static io.trino.SystemSessionProperties.REMOTE_TASK_GUARANTEED_SPLITS_PER_REQUEST;
import static io.trino.SystemSessionProperties.REMOTE_TASK_MAX_REQUEST_SIZE;
import static io.trino.SystemSessionProperties.REMOTE_TASK_REQUEST_SIZE_HEADROOM;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.BROADCAST;
import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.server.InternalHeaders.TRINO_CURRENT_VERSION;
import static io.trino.server.InternalHeaders.TRINO_MAX_WAIT;
import static io.trino.server.InternalHeaders.TRINO_RUNTIME_CONSTRAINT_SEQUENCE;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
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
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    @Timeout(30)
    public void testRemoteTaskMismatch()
            throws Exception
    {
        runTest(FailureScenario.TASK_MISMATCH);
    }

    @Test
    @Timeout(30)
    public void testRejectedExecutionWhenVersionIsHigh()
            throws Exception
    {
        runTest(FailureScenario.TASK_MISMATCH_WHEN_VERSION_IS_HIGH);
    }

    @Test
    @Timeout(30)
    public void testRejectedExecution()
            throws Exception
    {
        runTest(FailureScenario.REJECTED_EXECUTION);
    }

    @Test
    @Timeout(30)
    public void testRegular()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource);

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        remoteTask.addSplits(ImmutableMultimap.of(TABLE_SCAN_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit())));
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID) != null);
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).getSplits().size() == 1);

        remoteTask.noMoreSplits(TABLE_SCAN_NODE_ID);
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).isNoMoreSplits());

        remoteTask.cancel();
        poll(() -> remoteTask.getTaskStatus().state().isDone());
        poll(() -> remoteTask.getTaskInfo().taskStatus().state().isDone());

        httpRemoteTaskFactory.stop();
    }

    @Test
    @Timeout(30)
    public void testRuntimeConstraintContributionIsAcknowledged()
    {
        TestingTaskResource testingTaskResource = new TestingTaskResource(new AtomicLong(System.nanoTime()), FailureScenario.NO_FAILURE);
        DynamicFilterService dynamicFilterService = new DynamicFilterService(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                new TypeOperators(),
                new DynamicFilterConfig());
        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource, dynamicFilterService);
        HttpRemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);
        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        testingTaskResource.setRuntimeConstraintContributions(new RuntimeConstraintContributionBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                0,
                ImmutableList.of()));

        remoteTask.start();
        remoteTask.getRuntimeConstraintFetcher().updateSequenceAndFetchIfNecessary(1);

        assertEventually(new Duration(10, SECONDS), () -> {
            assertThat(testingTaskResource.getRuntimeConstraintFetchRequests())
                    .extracting(TestingTaskResource.RuntimeConstraintFetchRequest::currentSequence)
                    .hasSizeGreaterThanOrEqualTo(2)
                    .startsWith(0L)
                    .endsWith(1L)
                    .isSorted();
            assertThat(remoteTask.getRuntimeConstraintFetcher().getSequence()).isEqualTo(1);
        });

        httpRemoteTaskFactory.stop();
    }

    @ParameterizedTest
    @CsvSource({
            "false, EQUIVALENT_REPLICAS",
            "false, UNION_ALL_PARTITIONS",
            "true, EQUIVALENT_REPLICAS",
            "true, UNION_ALL_PARTITIONS",
    })
    @Timeout(30)
    public void testRejectedCollectionPrecedesBufferedContribution(boolean finalTaskInfoOnly, DistributedCompletionPolicy completionPolicy)
            throws Exception
    {
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty("legacy_dynamic_filtering", "false")
                .setQueryId(new QueryId("test"))
                .build();
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId("1"),
                TaskTestUtils.PLAN_FRAGMENT.getRoot(),
                ImmutableSet.of(TaskTestUtils.SYMBOL),
                SOURCE_DISTRIBUTION,
                OptionalInt.empty(),
                ImmutableList.of(TABLE_SCAN_NODE_ID),
                TaskTestUtils.PLAN_FRAGMENT.getOutputPartitioningScheme(),
                OptionalInt.empty(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
        TaskId taskId = new TaskId(new StageId(session.getQueryId(), 1), 2, 0);
        PlanNodeId sourceId = new PlanNodeId("collection");
        RuntimeConstraintId constraintId = new RuntimeConstraintId("constraint");
        TestingColumnHandle column = new TestingColumnHandle("column");
        RuntimeConstraintWiringReport report = new RuntimeConstraintWiringReport(
                ImmutableList.of(new RuntimeConstraintWiringReport.ScanWiring(
                        TABLE_SCAN_NODE_ID,
                        ImmutableList.of(new RuntimeConstraintWiringReport.Binding(constraintId, column)))),
                ImmutableList.of(new RuntimeConstraintWiringReport.Source(
                        sourceId,
                        ImmutableList.of(new CollectedConstraint(constraintId, 0)),
                        ImmutableList.of(BIGINT),
                        completionPolicy)),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(RuntimeConstraintRequest.collection(constraintId, 0, ComparisonOperator.EQUAL, false, BIGINT, false)));
        DynamicFilterService service = new DynamicFilterService(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                new TypeOperators(),
                new DynamicFilterConfig());
        service.registerQuery(session, new SubPlan(fragment, ImmutableList.of()));
        service.stageCannotScheduleMoreTasks(taskId.stageId(), taskId.attemptId(), ImmutableSet.of(taskId.partitionId()));
        service.addTaskRuntimeConstraintContributions(taskId, new RuntimeConstraintContributionBatch(
                RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                1,
                0,
                ImmutableList.of(new RuntimeConstraintContribution(
                        RuntimeConstraintHub.dynamicGroupId(sourceId),
                        RuntimeConstraintHub.dynamicBindingId(sourceId),
                        taskId.partitionId(),
                        taskId.attemptId(),
                        1,
                        new RuntimeMembershipPayload(ImmutableList.of(singleValue(BIGINT, 11L)), ORDINARY)))));
        service.taskFinished(taskId, true, 1);

        TestingTaskResource resource = new TestingTaskResource(new AtomicLong(System.nanoTime()), FailureScenario.NO_FAILURE);
        HttpRemoteTaskFactory factory = createHttpRemoteTaskFactory(resource, service);
        try {
            HttpRemoteTask task = factory.createRemoteTask(
                    session,
                    Span.getInvalid(),
                    taskId,
                    new InternalNode("node-id", URI.create("http://fake.invalid/"), NodeVersion.UNKNOWN, false),
                    false,
                    fragment,
                    ImmutableMap.of(),
                    ImmutableMultimap.of(),
                    PipelinedOutputBuffers.createInitial(BROADCAST),
                    new NodeTaskMap.PartitionedSplitCountTracker(_ -> {}),
                    Optional.empty(),
                    true);
            resource.setInitialTaskInfo(task.getTaskInfo());
            resource.setRuntimeConstraintWiringReport(report, finalTaskInfoOnly);
            task.start();
            poll(() -> resource.getCreateOrUpdateCounter() > 0);
            if (finalTaskInfoOnly) {
                resource.finishTask();
            }
            assertEventually(new Duration(10, SECONDS), () -> {
                assertThat(service.discoverRuntimeConstraintDynamicFilter(session, TABLE_SCAN_NODE_ID, ImmutableList.of(column))).isDone();
                DynamicFilter filter = service.discoverRuntimeConstraintDynamicFilter(session, TABLE_SCAN_NODE_ID, ImmutableList.of(column)).join();
                assertThat(filter.isComplete()).isTrue();
                assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());
            });
        }
        finally {
            factory.stop();
        }
    }

    @Test
    @Timeout(300)
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

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, session);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        Multimap<PlanNodeId, Split> splits = HashMultimap.create();
        for (int i = 0; i < 100; i++) {
            splits.put(TABLE_SCAN_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        }
        remoteTask.addSplits(splits);

        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID) != null);

        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).getSplits().size() == 100); // to check whether all the splits are sent or not
        assertThat(testingTaskResource.getCreateOrUpdateCounter() > 1).isTrue(); // to check whether the splits are divided or not

        remoteTask.noMoreSplits(TABLE_SCAN_NODE_ID);
        poll(() -> testingTaskResource.getTaskSplitAssignment(TABLE_SCAN_NODE_ID).isNoMoreSplits());

        remoteTask.cancel();
        poll(() -> remoteTask.getTaskStatus().state().isDone());
        poll(() -> remoteTask.getTaskInfo().taskStatus().state().isDone());

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

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory, session);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());

        Set<ScheduledSplit> splits = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            splits.add(new ScheduledSplit(i, TABLE_SCAN_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit())));
        }

        // decrease splitBatchSize
        assertThat(((HttpRemoteTask) remoteTask).adjustSplitBatchSize(ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, splits, true)), 1000000, 500)).isTrue();
        assertThat(((HttpRemoteTask) remoteTask).splitBatchSize.get()).isLessThan(250);

        // increase splitBatchSize
        assertThat(((HttpRemoteTask) remoteTask).adjustSplitBatchSize(ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, splits, true)), 1000, 100)).isFalse();
        assertThat(((HttpRemoteTask) remoteTask).splitBatchSize.get()).isGreaterThan(250);
    }

    private void runTest(FailureScenario failureScenario)
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, failureScenario);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource);
        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        waitUntilIdle(lastActivityNanos);

        httpRemoteTaskFactory.stop();
        assertThat(remoteTask.getTaskStatus().state().isDone())
                .describedAs(format("TaskStatus is not in a done state: %s", remoteTask.getTaskStatus()))
                .isTrue();

        ErrorCode actualErrorCode = getOnlyElement(remoteTask.getTaskStatus().failures()).errorCode();
        switch (failureScenario) {
            case TASK_MISMATCH, TASK_MISMATCH_WHEN_VERSION_IS_HIGH -> {
                assertThat(remoteTask.getTaskInfo().taskStatus().state().isDone())
                        .describedAs(format("TaskInfo is not in a done state: %s", remoteTask.getTaskInfo()))
                        .isTrue();
                assertThat(actualErrorCode).isEqualTo(REMOTE_TASK_MISMATCH.toErrorCode());
            }
            case REJECTED_EXECUTION -> {
                // for a rejection to occur, the http client must be shutdown, which means we will not be able to ge the final task info
                assertThat(actualErrorCode).isEqualTo(REMOTE_TASK_ERROR.toErrorCode());
            }
            default -> throw new UnsupportedOperationException();
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

    private HttpRemoteTask createRemoteTask(HttpRemoteTaskFactory httpRemoteTaskFactory)
    {
        return createRemoteTask(httpRemoteTaskFactory, Session.builder(TEST_SESSION)
                .setSystemProperty("legacy_dynamic_filtering", "false")
                .build());
    }

    private HttpRemoteTask createRemoteTask(HttpRemoteTaskFactory httpRemoteTaskFactory, Session session)
    {
        return httpRemoteTaskFactory.createRemoteTask(
                session,
                Span.getInvalid(),
                new TaskId(new StageId("test", 1), 2, 0),
                new InternalNode("node-id", URI.create("http://fake.invalid/"), NodeVersion.UNKNOWN, false),
                false,
                TaskTestUtils.PLAN_FRAGMENT,
                ImmutableMap.of(),
                ImmutableMultimap.of(),
                PipelinedOutputBuffers.createInitial(BROADCAST),
                new NodeTaskMap.PartitionedSplitCountTracker(_ -> {}),
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
        return createHttpRemoteTaskFactory(testingTaskResource, dynamicFilterService, new QueryManagerConfig());
    }

    private static HttpRemoteTaskFactory createHttpRemoteTaskFactory(TestingTaskResource testingTaskResource, DynamicFilterService dynamicFilterService, QueryManagerConfig config)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new HandleJsonModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(JaxRsJsonMapper.class).in(SINGLETON);
                        binder.bind(Metadata.class).toInstance(createTestingMetadataManager());
                        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                        jsonBinder(binder).addDeserializerBinding(TypeDescriptor.class).to(TypeDescriptorDeserializer.class);
                        jsonBinder(binder).addKeyDeserializerBinding(TypeDescriptor.class).to(TypeDescriptorKeyDeserializer.class);
                        jsonBinder(binder).addKeyDeserializerBinding(Symbol.class).to(SymbolKeyDeserializer.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
                        jsonCodecBinder(binder).bindJsonCodec(RuntimeConstraintContributionBatch.class);
                        jsonCodecBinder(binder).bindJsonCodec(VersionedDynamicFilterDomains.class);
                        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
                        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
                        jsonCodecBinder(binder).bindJsonCodec(FailTaskRequest.class);

                        binder.bind(BlockEncodingSimdSupport.class).toInstance(new BlockEncodingSimdSupport(true));
                        binder.bind(TypeManager.class).toInstance(TESTING_TYPE_MANAGER);
                        binder.bind(BlockEncodingManager.class).in(SINGLETON);
                        binder.bind(BlockEncodingSerde.class).to(InternalBlockEncodingSerde.class).in(SINGLETON);

                        binder.bind(OpenTelemetry.class).toInstance(OpenTelemetry.noop());
                        jsonBinder(binder).addSerializerBinding(Span.class).to(SpanSerializer.class);
                        jsonBinder(binder).addDeserializerBinding(Span.class).to(SpanDeserializer.class);
                    }

                    @Provides
                    private HttpRemoteTaskFactory createHttpRemoteTaskFactory(
                            JaxRsJsonMapper jsonMapper,
                            JsonCodec<TaskStatus> taskStatusCodec,
                            JsonCodec<RuntimeConstraintContributionBatch> runtimeConstraintContributionCodec,
                            JsonCodec<VersionedDynamicFilterDomains> legacyDynamicFilterDomainsCodec,
                            JsonCodec<TaskInfo> taskInfoCodec,
                            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
                            JsonCodec<FailTaskRequest> failTaskRequestCodec)
                    {
                        JaxrsTestingHttpProcessor jaxrsTestingHttpProcessor = new JaxrsTestingHttpProcessor(URI.create("http://fake.invalid/"), testingTaskResource, jsonMapper);
                        TestingHttpClient testingHttpClient = new TestingHttpClient(jaxrsTestingHttpProcessor.setTrace(TRACE_HTTP));
                        testingTaskResource.setHttpClient(testingHttpClient);
                        return new HttpRemoteTaskFactory(
                                config,
                                TASK_MANAGER_CONFIG,
                                testingHttpClient,
                                new BaseTestSqlTaskManager.MockLocationFactory(),
                                taskStatusCodec,
                                runtimeConstraintContributionCodec,
                                legacyDynamicFilterDomainsCodec,
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
        private static final long INITIAL_TASK_INSTANCE_ID = -1;
        private static final long NEW_TASK_INSTANCE_ID = 1;

        private final AtomicLong lastActivityNanos;
        private final FailureScenario failureScenario;

        private final AtomicReference<TestingHttpClient> httpClient = new AtomicReference<>();

        private TaskInfo initialTaskInfo;
        private TaskStatus initialTaskStatus;
        private Optional<RuntimeConstraintContributionBatch> runtimeConstraintContributions = Optional.empty();
        private RuntimeConstraintWiringReport updateWiringReport = RuntimeConstraintWiringReport.EMPTY;
        private RuntimeConstraintWiringReport finalWiringReport = RuntimeConstraintWiringReport.EMPTY;
        private long version;
        private TaskState taskState;
        private long taskInstanceId = INITIAL_TASK_INSTANCE_ID;

        private long statusFetchCounter;
        private long createOrUpdateCounter;
        private long runtimeConstraintFetchCounter;
        private final List<RuntimeConstraintFetchRequest> runtimeConstraintFetchRequests = new ArrayList<>();

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
            return buildTaskInfo(true);
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
            for (SplitAssignment splitAssignment : taskUpdateRequest.splitAssignments()) {
                taskSplitAssignmentMap.compute(splitAssignment.getPlanNodeId(), (_, taskSplitAssignment) -> taskSplitAssignment == null ? splitAssignment : taskSplitAssignment.update(splitAssignment));
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
        @Path("{taskId}/runtimeconstraints")
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized RuntimeConstraintContributionBatch acknowledgeAndGetRuntimeConstraintContributions(
                @PathParam("taskId") TaskId taskId,
                @HeaderParam(TRINO_RUNTIME_CONSTRAINT_SEQUENCE) @DefaultValue("0") long currentSequence,
                @Context UriInfo uriInfo)
        {
            runtimeConstraintFetchCounter++;
            runtimeConstraintFetchRequests.add(new RuntimeConstraintFetchRequest(
                    uriInfo.getRequestUri().toString(),
                    taskId,
                    currentSequence));
            return runtimeConstraintContributions.orElse(null);
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

            if (!taskState.isDone()) {
                taskState = abort ? TaskState.ABORTED : TaskState.CANCELED;
            }
            return buildTaskInfo(true);
        }

        public void setInitialTaskInfo(TaskInfo initialTaskInfo)
        {
            this.initialTaskInfo = initialTaskInfo;
            this.initialTaskStatus = initialTaskInfo.taskStatus();
            this.taskState = initialTaskStatus.state();
            this.version = initialTaskStatus.version();
            switch (failureScenario) {
                case TASK_MISMATCH_WHEN_VERSION_IS_HIGH -> {
                    // Make the initial version large enough.
                    // This way, the version number can't be reached if it is reset to 0.
                    version = 1_000_000;
                }
                case TASK_MISMATCH, REJECTED_EXECUTION, NO_FAILURE -> {}
                default -> throw new UnsupportedOperationException();
            }
        }

        public synchronized void setRuntimeConstraintContributions(RuntimeConstraintContributionBatch runtimeConstraintContributions)
        {
            this.runtimeConstraintContributions = Optional.of(runtimeConstraintContributions);
        }

        public synchronized void setRuntimeConstraintWiringReport(RuntimeConstraintWiringReport report, boolean finalTaskInfoOnly)
        {
            if (!finalTaskInfoOnly) {
                updateWiringReport = report;
            }
            finalWiringReport = report;
        }

        public synchronized void finishTask()
        {
            taskState = TaskState.FINISHED;
        }

        public synchronized long getStatusFetchCounter()
        {
            return statusFetchCounter;
        }

        public synchronized long getCreateOrUpdateCounter()
        {
            return createOrUpdateCounter;
        }

        public synchronized long getRuntimeConstraintFetchCounter()
        {
            return runtimeConstraintFetchCounter;
        }

        public synchronized List<RuntimeConstraintFetchRequest> getRuntimeConstraintFetchRequests()
        {
            return ImmutableList.copyOf(runtimeConstraintFetchRequests);
        }

        private TaskInfo buildTaskInfo()
        {
            return buildTaskInfo(false);
        }

        private TaskInfo buildTaskInfo(boolean finalTaskInfo)
        {
            return new TaskInfo(
                    buildTaskStatus(),
                    initialTaskInfo.lastHeartbeat(),
                    initialTaskInfo.outputBuffers(),
                    initialTaskInfo.noMoreSplits(),
                    initialTaskInfo.stats(),
                    initialTaskInfo.estimatedMemory(),
                    initialTaskInfo.needsPlan(),
                    finalTaskInfo && taskState.isDone() ? finalWiringReport : updateWiringReport);
        }

        private TaskStatus buildTaskStatus()
        {
            statusFetchCounter++;
            // Change the task instance id after 10th fetch to simulate worker restart
            switch (failureScenario) {
                case TASK_MISMATCH, TASK_MISMATCH_WHEN_VERSION_IS_HIGH -> {
                    if (statusFetchCounter == 10) {
                        taskInstanceId = NEW_TASK_INSTANCE_ID;
                        version = 0;
                    }
                }
                case REJECTED_EXECUTION -> {
                    if (statusFetchCounter >= 10) {
                        httpClient.get().close();
                        throw new RejectedExecutionException();
                    }
                }
                case NO_FAILURE -> {}
                default -> throw new UnsupportedOperationException();
            }

            return new TaskStatus(
                    initialTaskStatus.taskId(),
                    taskInstanceId,
                    ++version,
                    taskState,
                    initialTaskStatus.self(),
                    "fake",
                    false,
                    initialTaskStatus.failures(),
                    initialTaskStatus.queuedPartitionedDrivers(),
                    initialTaskStatus.runningPartitionedDrivers(),
                    initialTaskStatus.outputBufferStatus(),
                    initialTaskStatus.outputDataSize(),
                    initialTaskStatus.writerInputDataSize(),
                    initialTaskStatus.physicalWrittenDataSize(),
                    initialTaskStatus.maxWriterCount(),
                    initialTaskStatus.memoryReservation(),
                    initialTaskStatus.peakMemoryReservation(),
                    initialTaskStatus.revocableMemoryReservation(),
                    initialTaskStatus.fullGcCount(),
                    initialTaskStatus.fullGcTime(),
                    runtimeConstraintContributions.map(RuntimeConstraintContributionBatch::sequence).orElse(0L),
                    0,
                    initialTaskStatus.queuedPartitionedSplitsWeight(),
                    initialTaskStatus.runningPartitionedSplitsWeight());
        }

        private record RuntimeConstraintFetchRequest(
                String uriInfo,
                TaskId taskId,
                long currentSequence)
        {
            private RuntimeConstraintFetchRequest
            {
                requireNonNull(uriInfo, "uriInfo is null");
                requireNonNull(taskId, "taskId is null");
            }
        }
    }
}
