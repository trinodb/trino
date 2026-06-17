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
package io.trino.execution.admission;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.connector.CatalogHandle;
import io.trino.connector.ConnectorCatalogServiceProvider;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.dispatcher.LocalDispatchQuery;
import io.trino.event.QueryMonitor;
import io.trino.event.QueryMonitorConfig;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.ClusterSizeMonitor;
import io.trino.execution.DataDefinitionExecution;
import io.trino.execution.DataDefinitionTask;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.StagesInfo;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.LanguageFunctionProvider;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.node.InternalNodeManager;
import io.trino.node.TestingInternalNodeManager;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.server.protocol.Slug;
import io.trino.spi.NodeVersion;
import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.WaitDecision;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.opentelemetry.api.OpenTelemetry.noop;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.sql.tree.SaveMode.FAIL;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Variant-dispatch and failure-handling tests for the
 * {@link LocalDispatchQuery} ↔ {@link AdmissionPolicy} integration.
 *
 * <p>Validates: Requirements 1.9, 4.1, 4.2, 4.3, 4.4, 4.5 / Design:
 * §LocalDispatchQuery integration, §Correctness Properties — Properties 3
 * (variant dispatch) and 4 (failure handling).
 *
 * <p>Test framework choice: this file uses JUnit 5 {@code @ParameterizedTest}
 * with {@code @MethodSource} rather than jqwik's {@code @Property}. jqwik is
 * not currently a dependency of {@code core/trino-main}, and adding it for
 * this single test exceeds the scope of this change. The deterministic
 * generators below cover the four variant cases ({@code ProceedNow},
 * {@code Wait}, {@code throw}, {@code null}) across at least 100 cases
 * combined, with reproducible seeded inputs.
 */
public class TestLocalDispatchQueryAdmissionPolicy
{
    private static final int CASES_PER_VARIANT = 25;

    /**
     * For {@code ProceedNow}: engine transitions to {@code DISPATCHING} and
     * does NOT call {@code clusterSizeMonitor.waitForMinimumWorkers}.
     */
    @ParameterizedTest(name = "[{index}] ProceedNow reason={0}")
    @MethodSource("proceedNowCases")
    @DisplayName("Feature: admission-policy-spi, ProceedNow → engine starts execution without invoking ClusterSizeMonitor")
    public void testProceedNowSkipsClusterSizeMonitor(String reason)
            throws InterruptedException
    {
        WaitDecision decision = new WaitDecision.ProceedNow(reason);
        AdmissionPolicy policy = _ -> decision;

        Fixture fixture = Fixture.create();
        fixture.run(policy);

        assertThat(fixture.clusterSizeMonitor.waitCallCount.get())
                .as("ClusterSizeMonitor.waitForMinimumWorkers must not be called for ProceedNow")
                .isZero();
        assertThat(fixture.observedDispatching.get())
                .as("query state machine must reach DISPATCHING for ProceedNow")
                .isTrue();
        assertThat(fixture.observedFailed.get())
                .as("ProceedNow must not lead to FAILED")
                .isFalse();
    }

    /**
     * For {@code Wait}: engine calls
     * {@code clusterSizeMonitor.waitForMinimumWorkers(_, timeout)} where
     * {@code timeout} is the policy-supplied {@code wait.maxWait()} (the
     * stub policy is not {@link MinWorkersAdmissionPolicy}, so the engine
     * does not override the timeout).
     */
    @ParameterizedTest(name = "[{index}] Wait timeoutMillis={0}")
    @MethodSource("waitCases")
    @DisplayName("Feature: admission-policy-spi, Wait → engine calls ClusterSizeMonitor with policy-supplied timeout")
    public void testWaitInvokesClusterSizeMonitorWithPolicyTimeout(long timeoutMillis)
            throws InterruptedException
    {
        Duration policyTimeout = new Duration(timeoutMillis, TimeUnit.MILLISECONDS);
        AdmissionPolicy policy = _ -> new WaitDecision.Wait(policyTimeout, "stub-wait");

        Fixture fixture = Fixture.create();
        fixture.run(policy);

        assertThat(fixture.clusterSizeMonitor.waitCallCount.get())
                .as("ClusterSizeMonitor.waitForMinimumWorkers must be called once for Wait")
                .isEqualTo(1);
        Duration observedTimeout = fixture.clusterSizeMonitor.lastTimeout.get();
        assertThat(observedTimeout)
                .as("engine must pass through the policy-supplied wait.maxWait() when the policy is not MinWorkersAdmissionPolicy")
                .isNotNull();
        assertThat(observedTimeout.toMillis()).isEqualTo(timeoutMillis);
    }

    /**
     * For an unchecked exception thrown by the policy: engine fails the query
     * with {@code GENERIC_INSUFFICIENT_RESOURCES} and preserves the cause.
     */
    @ParameterizedTest(name = "[{index}] throw {0}")
    @MethodSource("throwCases")
    @DisplayName("Feature: admission-policy-spi, throw → engine fails with GENERIC_INSUFFICIENT_RESOURCES and preserves cause")
    public void testThrowFailsQueryWithCausePreserved(String marker)
            throws InterruptedException
    {
        RuntimeException thrown = new RuntimeException("policy bug: " + marker);
        AdmissionPolicy policy = _ -> {
            throw thrown;
        };

        Fixture fixture = Fixture.create();
        fixture.run(policy);

        assertThat(fixture.clusterSizeMonitor.waitCallCount.get()).isZero();
        assertThat(fixture.queryStateMachine.getQueryState()).isEqualTo(QueryState.FAILED);
        Optional<ExecutionFailureInfo> failureInfo = fixture.queryStateMachine.getFailureInfo();
        assertThat(failureInfo).isPresent();
        assertThat(failureInfo.get().errorCode())
                .isEqualTo(GENERIC_INSUFFICIENT_RESOURCES.toErrorCode());
        // The TrinoException wraps the original; the original's message is preserved
        // on the inner cause node of ExecutionFailureInfo.
        ExecutionFailureInfo cause = failureInfo.get().cause();
        assertThat(cause)
                .as("the thrown RuntimeException must be preserved as the failure cause")
                .isNotNull();
        assertThat(cause.message()).contains("policy bug: " + marker);
    }

    /**
     * For a {@code null} return: engine fails the query with
     * {@code GENERIC_INSUFFICIENT_RESOURCES} and the message names "null decision".
     */
    @ParameterizedTest(name = "[{index}]")
    @MethodSource("nullCases")
    @DisplayName("Feature: admission-policy-spi, null → engine fails with GENERIC_INSUFFICIENT_RESOURCES naming 'null decision'")
    public void testNullDecisionFailsQuery(int seed)
            throws InterruptedException
    {
        AdmissionPolicy policy = _ -> null;

        Fixture fixture = Fixture.create();
        fixture.run(policy);

        assertThat(fixture.clusterSizeMonitor.waitCallCount.get()).isZero();
        assertThat(fixture.queryStateMachine.getQueryState()).isEqualTo(QueryState.FAILED);
        Optional<ExecutionFailureInfo> failureInfo = fixture.queryStateMachine.getFailureInfo();
        assertThat(failureInfo).isPresent();
        assertThat(failureInfo.get().errorCode())
                .isEqualTo(GENERIC_INSUFFICIENT_RESOURCES.toErrorCode());
        assertThat(failureInfo.get().message())
                .as("null-decision failure message must name 'null decision' (Req 1.9)")
                .contains("null decision");
    }

    static Stream<String> proceedNowCases()
    {
        Random random = new Random(0xC011BACEL);
        ImmutableList.Builder<String> reasons = ImmutableList.builder();
        reasons.add("admission-policy-spi-test/proceed");
        for (int i = 0; i < CASES_PER_VARIANT - 1; i++) {
            reasons.add("proceed/" + Integer.toHexString(random.nextInt()));
        }
        return reasons.build().stream();
    }

    static Stream<Long> waitCases()
    {
        Random random = new Random(0xC011BACEL ^ 0x1L);
        ImmutableList.Builder<Long> timeouts = ImmutableList.builder();
        // Boundary values + randomized cases.
        timeouts.add(0L);
        timeouts.add(1L);
        timeouts.add(1_000L);
        for (int i = 0; i < CASES_PER_VARIANT - 3; i++) {
            timeouts.add((long) random.nextInt(60_000));
        }
        return timeouts.build().stream();
    }

    static Stream<String> throwCases()
    {
        Random random = new Random(0xC011BACEL ^ 0x2L);
        ImmutableList.Builder<String> markers = ImmutableList.builder();
        for (int i = 0; i < CASES_PER_VARIANT; i++) {
            markers.add(Integer.toHexString(random.nextInt()));
        }
        return markers.build().stream();
    }

    static Stream<Integer> nullCases()
    {
        ImmutableList.Builder<Integer> seeds = ImmutableList.builder();
        for (int i = 0; i < CASES_PER_VARIANT; i++) {
            seeds.add(i);
        }
        return seeds.build().stream();
    }

    /**
     * Test fixture wiring a {@link LocalDispatchQuery} with a stub
     * {@link ClusterSizeMonitor} that records invocations and an injected
     * {@link AdmissionPolicy}. Mirrors the construction pattern in
     * {@code TestLocalDispatchQuery}.
     */
    private static final class Fixture
    {
        final QueryStateMachine queryStateMachine;
        final RecordingClusterSizeMonitor clusterSizeMonitor;
        final AtomicBoolean observedDispatching = new AtomicBoolean();
        final AtomicBoolean observedFailed = new AtomicBoolean();
        final CountDownLatch terminalLatch = new CountDownLatch(1);
        final QueryMonitor queryMonitor;
        final DataDefinitionExecution<?> dataDefinitionExecution;
        final Executor executor;

        private Fixture(
                QueryStateMachine queryStateMachine,
                RecordingClusterSizeMonitor clusterSizeMonitor,
                QueryMonitor queryMonitor,
                DataDefinitionExecution<?> dataDefinitionExecution,
                Executor executor)
        {
            this.queryStateMachine = queryStateMachine;
            this.clusterSizeMonitor = clusterSizeMonitor;
            this.queryMonitor = queryMonitor;
            this.dataDefinitionExecution = dataDefinitionExecution;
            this.executor = executor;

            queryStateMachine.addStateChangeListener(state -> {
                if (state == QueryState.DISPATCHING || state.ordinal() >= QueryState.PLANNING.ordinal()) {
                    if (state != QueryState.FAILED) {
                        observedDispatching.set(true);
                    }
                }
                if (state == QueryState.FAILED) {
                    observedFailed.set(true);
                }
                if (state.isDone() || state == QueryState.DISPATCHING || state.ordinal() >= QueryState.PLANNING.ordinal()) {
                    terminalLatch.countDown();
                }
            });
        }

        static Fixture create()
        {
            Executor executor = newCachedThreadPool(daemonThreadsNamed("admission-policy-test-%s"));
            Metadata metadata = createTestingMetadataManager();
            TransactionManager transactionManager = createTestTransactionManager();
            AccessControlManager accessControl = new AccessControlManager(
                    NodeVersion.UNKNOWN,
                    transactionManager,
                    emptyEventListenerManager(),
                    new AccessControlConfig(),
                    noop(),
                    new SecretsResolver(ImmutableMap.of()),
                    DefaultSystemAccessControl.NAME);
            accessControl.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));

            QueryStateMachine queryStateMachine = QueryStateMachine.begin(
                    Optional.empty(),
                    "sql",
                    Optional.empty(),
                    TEST_SESSION,
                    URI.create("fake://fake-query"),
                    new ResourceGroupId("test"),
                    false,
                    transactionManager,
                    accessControl,
                    executor,
                    metadata,
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector(),
                    new ExchangeMetricsCollector(ImmutableList::of, java.time.Duration.ofMillis(1)),
                    Optional.of(QueryType.DATA_DEFINITION),
                    true,
                    Optional.empty(),
                    new NodeVersion("test"));

            QueryMonitor queryMonitor = new QueryMonitor(
                    jsonCodec(StagesInfo.class),
                    jsonCodec(OperatorStats.class),
                    jsonCodec(ExecutionFailureInfo.class),
                    jsonCodec(StatsAndCosts.class),
                    new EventListenerManager(new EventListenerConfig(), new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test")),
                    new NodeInfo("node"),
                    new NodeVersion("version"),
                    new SessionPropertyManager(),
                    metadata,
                    new FunctionManager(
                            new ConnectorCatalogServiceProvider<>("function provider", new NoConnectorServicesProvider(), ConnectorServices::getFunctionProvider),
                            new GlobalFunctionCatalog(
                                    () -> { throw new UnsupportedOperationException(); },
                                    () -> { throw new UnsupportedOperationException(); },
                                    () -> { throw new UnsupportedOperationException(); }),
                            LanguageFunctionProvider.DISABLED),
                    new QueryMonitorConfig());

            CreateTable createTable = new CreateTable(new NodeLocation(1, 1), QualifiedName.of("table"), ImmutableList.of(), FAIL, ImmutableList.of(), Optional.empty());
            QueryPreparer.PreparedQuery preparedQuery = new QueryPreparer.PreparedQuery(createTable, ImmutableList.of(), Optional.empty());
            DataDefinitionExecution.DataDefinitionExecutionFactory dataDefinitionExecutionFactory =
                    new DataDefinitionExecution.DataDefinitionExecutionFactory(
                            ImmutableMap.<Class<? extends Statement>, DataDefinitionTask<?>>of(CreateTable.class, new SleepingCreateTableTask()));
            DataDefinitionExecution<?> dataDefinitionExecution = dataDefinitionExecutionFactory.createQueryExecution(
                    preparedQuery,
                    queryStateMachine,
                    Slug.createNew(),
                    WarningCollector.NOOP,
                    null);

            RecordingClusterSizeMonitor clusterSizeMonitor = new RecordingClusterSizeMonitor(
                    TestingInternalNodeManager.createDefault(),
                    new NodeSchedulerConfig());

            return new Fixture(queryStateMachine, clusterSizeMonitor, queryMonitor, dataDefinitionExecution, executor);
        }

        void run(AdmissionPolicy policy)
                throws InterruptedException
        {
            LocalDispatchQuery localDispatchQuery = new LocalDispatchQuery(
                    queryStateMachine,
                    Futures.immediateFuture(dataDefinitionExecution),
                    queryMonitor,
                    clusterSizeMonitor,
                    policy,
                    executor,
                    _ -> dataDefinitionExecution.start());
            localDispatchQuery.startWaitingForResources();
            // Wait for the dispatcher thread to observe the decision and either
            // start execution, fail the query, or invoke the cluster-size monitor.
            terminalLatch.await(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Stub {@link ClusterSizeMonitor} that records invocations of
     * {@link #waitForMinimumWorkers(int, Duration)} and immediately satisfies
     * the wait so the rest of the dispatcher pipeline runs to completion.
     */
    private static final class RecordingClusterSizeMonitor
            extends ClusterSizeMonitor
    {
        final AtomicInteger waitCallCount = new AtomicInteger();
        final AtomicReference<Duration> lastTimeout = new AtomicReference<>();
        final AtomicInteger lastMinCount = new AtomicInteger();

        RecordingClusterSizeMonitor(InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig)
        {
            super(nodeManager, nodeSchedulerConfig);
        }

        @Override
        public synchronized ListenableFuture<Void> waitForMinimumWorkers(int executionMinCount, Duration executionMaxWait)
        {
            waitCallCount.incrementAndGet();
            lastMinCount.set(executionMinCount);
            lastTimeout.set(executionMaxWait);
            return immediateVoidFuture();
        }
    }

    private static final class NoConnectorServicesProvider
            implements ConnectorServicesProvider
    {
        @Override
        public void loadInitialCatalogs() {}

        @Override
        public void ensureCatalogsLoaded(List<CatalogProperties> catalogs) {}

        @Override
        public PrunableState getPrunableState()
        {
            return PrunableState.empty();
        }

        @Override
        public void pruneCatalogs(PrunableState prunableState, Set<CatalogHandle> catalogsInUse)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class SleepingCreateTableTask
            implements DataDefinitionTask<CreateTable>
    {
        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public ListenableFuture<Void> execute(
                CreateTable statement,
                QueryStateMachine stateMachine,
                List<Expression> parameters,
                WarningCollector warningCollector)
        {
            // Block briefly so the test can observe DISPATCHING / PLANNING before the
            // query advances further. Short enough not to delay the suite materially.
            try {
                Thread.sleep(50L);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return immediateVoidFuture();
        }
    }
}
