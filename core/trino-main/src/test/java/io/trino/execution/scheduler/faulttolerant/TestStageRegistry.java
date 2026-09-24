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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.cost.StatsAndCosts;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.MockRemoteTaskFactory;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.StageRegistry;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.NodeVersion;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.SecureExpression;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.transaction.TransactionManager;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.StageState.ABORTED;
import static io.trino.execution.StageState.PLANNED;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.AbstractMockMetadata.dummyMetadata;
import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestStageRegistry
{
    private static final Symbol SYMBOL = new Symbol(BIGINT, "value");

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(1, daemonThreadsNamed(getClass().getSimpleName() + "-scheduled-%s"));

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    void testInitialReportingFragmentIsReusedAcrossStatusRequests()
    {
        QueryStateMachine query = query();
        SubPlan plan = plan("initial");
        StageRegistry registry = new StageRegistry(query, plan);

        StageInfo initial = registry.getStages().getOutputStage();
        assertThat(initial.state()).isEqualTo(PLANNED);
        assertThat(initial.plan()).isNotSameAs(plan.getFragment());
        assertThat(((FilterNode) initial.plan().getRoot()).getPredicate())
                .isEqualTo(new SecureExpression(new Constant(BOOLEAN, null)));
        assertThat(registry.getStages().getOutputStage().plan()).isSameAs(initial.plan());

        // An unstarted stage becomes ABORTED when the query finishes, without redoing redaction
        query.transitionToFailed(new RuntimeException("query is done"));
        StageInfo aborted = registry.getStages().getOutputStage();
        assertThat(aborted.state()).isEqualTo(ABORTED);
        assertThat(aborted.plan()).isSameAs(initial.plan());
        assertThat(initial.state()).isEqualTo(PLANNED);
        assertThat(registry.getStages().getOutputStage().plan()).isSameAs(initial.plan());
    }

    @Test
    void testPlanReplacementInvalidatesInitialReportingFragment()
    {
        StageRegistry registry = new StageRegistry(query(), plan("original"));
        StageInfo original = registry.getStages().getOutputStage();

        // Adaptive planning can replace a fragment while preserving its ID
        SubPlan replacement = plan("replacement");
        assertThat(replacement.getFragment().getId()).isEqualTo(original.plan().getId());
        registry.updatePlan(replacement);
        StageInfo updated = registry.getStages().getOutputStage();

        assertThat(updated.plan()).isNotSameAs(original.plan());
        assertThat(updated.plan().getRoot().getId()).isEqualTo(new PlanNodeId("replacement"));
        assertThat(original.plan().getRoot().getId()).isEqualTo(new PlanNodeId("original"));
        assertThat(registry.getStages().getOutputStage().plan()).isSameAs(updated.plan());
    }

    @Test
    void testConcurrentStatusRequestsShareInitialReportingFragment()
            throws Exception
    {
        StageRegistry registry = new StageRegistry(query(), plan("concurrent"));
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService requestExecutor = newFixedThreadPool(8)) {
            List<Future<PlanFragment>> requests = IntStream.range(0, 8)
                    .mapToObj(_ -> requestExecutor.submit(() -> {
                        start.await();
                        return registry.getStages().getOutputStage().plan();
                    }))
                    .toList();
            start.countDown();
            PlanFragment first = requests.getFirst().get(10, SECONDS);
            for (Future<PlanFragment> request : requests) {
                assertThat(request.get(10, SECONDS)).isSameAs(first);
            }
        }
    }

    @Test
    void testCreatedStageTakesPrecedenceOverInitialStageInfo()
    {
        SubPlan plan = plan("started");
        QueryStateMachine query = query();
        StageRegistry registry = new StageRegistry(query, plan);
        StageInfo initial = registry.getStages().getOutputStage();
        SqlStage stage = SqlStage.createSqlStage(
                dummyMetadata(),
                StageId.create(query.getQueryId(), plan.getFragment().getId()),
                plan.getFragment(),
                ImmutableMap.of(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                new NodeTaskMap(new FinalizerService()),
                Runnable::run,
                noopTracer(),
                Span.getInvalid(),
                new SplitSchedulerStats(),
                (_, _) -> OptionalInt.empty());
        registry.add(stage);
        stage.abort();

        StageInfo reported = registry.getStages().getOutputStage();
        assertThat(reported.state()).isEqualTo(ABORTED);
        assertThat(reported.plan()).isSameAs(stage.getStageInfo().plan());
        assertThat(reported.plan()).isNotSameAs(initial.plan());
    }

    private QueryStateMachine query()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        return QueryStateMachine.begin(
                Optional.empty(),
                "SELECT 1",
                Optional.empty(),
                TEST_SESSION,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                new AccessControlManager(NodeVersion.UNKNOWN, transactionManager, emptyEventListenerManager(), new AccessControlConfig(), OpenTelemetry.noop(), new SecretsResolver(ImmutableMap.of()), DefaultSystemAccessControl.NAME),
                executor,
                createTestingMetadataManager(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                new ExchangeMetricsCollector(ImmutableList::of, Duration.ofMillis(1)),
                Optional.empty(),
                true,
                Optional.empty(),
                new NodeVersion("test"));
    }

    private static SubPlan plan(String rootId)
    {
        FilterNode root = new FilterNode(
                new PlanNodeId(rootId),
                new ValuesNode(
                        new PlanNodeId("values"),
                        ImmutableList.of(SYMBOL),
                        ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 1L))))),
                new SecureExpression(comparison(LESS_THAN, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L))));
        return new SubPlan(new PlanFragment(
                new PlanFragmentId("0"),
                root,
                ImmutableSet.of(SYMBOL),
                SINGLE_DISTRIBUTION,
                OptionalInt.empty(),
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(SYMBOL)),
                OptionalInt.empty(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty()), ImmutableList.of());
    }
}
