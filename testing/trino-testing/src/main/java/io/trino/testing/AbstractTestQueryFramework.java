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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.execution.warnings.WarningCollector;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.MemoryPool;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.server.BasicQueryInfo;
import io.trino.server.DynamicFilterService.DynamicFiltersStats;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.TestingAccessControlManager.TestingPrivilege;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class AbstractTestQueryFramework
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private AutoCloseableCloser afterClassCloser;
    private QueryRunner queryRunner;
    private H2QueryRunner h2QueryRunner;
    private io.trino.sql.query.QueryAssertions queryAssertions;

    @BeforeAll
    public void init()
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.testcontainers", Level.WARN);

        afterClassCloser = AutoCloseableCloser.create();
        queryRunner = afterClassCloser.register(createQueryRunner());
        h2QueryRunner = afterClassCloser.register(new H2QueryRunner());
        queryAssertions = new io.trino.sql.query.QueryAssertions(queryRunner);
    }

    protected abstract QueryRunner createQueryRunner()
            throws Exception;

    @AfterAll
    public final void close()
            throws Exception
    {
        try (AutoCloseable ignored = afterClassCloser) {
            checkQueryMemoryReleased();
            checkQueryInfosFinal();
            checkTasksDone();
        }
        finally {
            afterClassCloser = null;
            queryRunner = null;
            h2QueryRunner = null;
            queryAssertions = null;
        }
    }

    private void checkQueryMemoryReleased()
    {
        tryGetDistributedQueryRunner().ifPresent(runner -> assertEventually(
                new Duration(30, SECONDS),
                new Duration(1, SECONDS),
                () -> {
                    List<TestingTrinoServer> servers = runner.getServers();
                    for (int serverId = 0; serverId < servers.size(); ++serverId) {
                        TestingTrinoServer server = servers.get(serverId);
                        assertMemoryPoolReleased(runner.getCoordinator(), server, serverId);
                    }

                    assertThat(runner.getCoordinator().getClusterMemoryManager().getClusterTotalMemoryReservation())
                            .describedAs("cluster memory reservation")
                            .isZero();
                }));
    }

    private void assertMemoryPoolReleased(TestingTrinoServer coordinator, TestingTrinoServer server, long serverId)
    {
        String serverName = format("server_%d(%s)", serverId, server.isCoordinator() ? "coordinator" : "worker");
        long reservedBytes = server.getLocalMemoryManager().getMemoryPool().getReservedBytes();

        if (reservedBytes != 0) {
            fail("Expected memory reservation on " + serverName + "to be 0 but was " + reservedBytes + "; detailed memory usage:\n" + describeMemoryPool(coordinator, server));
        }
    }

    private String describeMemoryPool(TestingTrinoServer coordinator, TestingTrinoServer server)
    {
        LocalMemoryManager memoryManager = server.getLocalMemoryManager();
        MemoryPool memoryPool = memoryManager.getMemoryPool();
        Map<QueryId, Long> queryReservations = memoryPool.getQueryMemoryReservations();
        Map<QueryId, Map<String, Long>> queryTaggedReservations = memoryPool.getTaggedMemoryAllocations();

        List<BasicQueryInfo> queriesWithMemory = coordinator.getQueryManager().getQueries().stream()
                .filter(query -> queryReservations.containsKey(query.getQueryId()))
                .collect(toImmutableList());

        StringBuilder result = new StringBuilder();
        queriesWithMemory.forEach(queryInfo -> {
            QueryId queryId = queryInfo.getQueryId();
            String querySql = queryInfo.getQuery();
            QueryState queryState = queryInfo.getState();
            Long memoryReservation = queryReservations.getOrDefault(queryId, 0L);
            Map<String, Long> taggedMemoryReservation = queryTaggedReservations.getOrDefault(queryId, ImmutableMap.of());

            result.append(" " + queryId + ":\n");
            result.append("   SQL: " + querySql + "\n");
            result.append("   state: " + queryState + "\n");
            result.append("   memoryReservation: " + memoryReservation + "\n");
            result.append("   taggedMemoryReservaton: " + taggedMemoryReservation + "\n");
        });

        return result.toString();
    }

    private void checkQueryInfosFinal()
    {
        tryGetDistributedQueryRunner().ifPresent(runner -> assertEventually(
                new Duration(30, SECONDS),
                new Duration(1, SECONDS),
                () -> {
                    TestingTrinoServer coordinator = runner.getCoordinator();
                    QueryManager queryManager = coordinator.getQueryManager();
                    for (BasicQueryInfo basicQueryInfo : queryManager.getQueries()) {
                        QueryId queryId = basicQueryInfo.getQueryId();
                        if (!basicQueryInfo.getState().isDone()) {
                            fail("query is expected to be in a done state\n\n" + createQueryDebuggingSummary(basicQueryInfo, queryManager.getFullQueryInfo(queryId)));
                        }
                        QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
                        if (!queryInfo.isFinalQueryInfo()) {
                            fail("QueryInfo for is expected to be final\n\n" + createQueryDebuggingSummary(basicQueryInfo, queryInfo));
                        }
                    }
                }));
    }

    private void checkTasksDone()
    {
        tryGetDistributedQueryRunner().ifPresent(runner -> assertEventually(
                new Duration(30, SECONDS),
                new Duration(1, SECONDS),
                () -> {
                    QueryManager queryManager = runner.getCoordinator().getQueryManager();
                    List<TestingTrinoServer> servers = runner.getServers();
                    for (TestingTrinoServer server : servers) {
                        SqlTaskManager taskManager = server.getTaskManager();
                        List<TaskInfo> taskInfos = taskManager.getAllTaskInfo();
                        for (TaskInfo taskInfo : taskInfos) {
                            TaskId taskId = taskInfo.taskStatus().getTaskId();
                            QueryId queryId = taskId.getQueryId();
                            TaskState taskState = taskInfo.taskStatus().getState();
                            if (!taskState.isDone()) {
                                try {
                                    BasicQueryInfo basicQueryInfo = queryManager.getQueryInfo(queryId);
                                    QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
                                    String querySummary = createQueryDebuggingSummary(basicQueryInfo, queryInfo);
                                    fail("Task is expected to be in done state, found: %s - TaskId: %s, QueryId: %s".formatted(taskState, taskId, queryId) + "\n\n" + querySummary);
                                }
                                catch (NoSuchElementException _) {
                                }
                                fail("Task is expected to be in done state, found: %s - TaskId: %s, QueryId: %s, Query: unknown".formatted(taskState, taskId, queryId));
                            }
                        }
                    }
                }));
    }

    private static String createQueryDebuggingSummary(BasicQueryInfo basicQueryInfo, QueryInfo queryInfo)
    {
        String queryDetails = format("Query %s [%s]: %s", basicQueryInfo.getQueryId(), basicQueryInfo.getState(), basicQueryInfo.getQuery());
        if (queryInfo.getOutputStage().isEmpty()) {
            return queryDetails + " -- <no output stage present>";
        }
        else {
            return queryDetails + getAllStages(queryInfo.getOutputStage()).stream()
                    .map(stageInfo -> {
                        String stageDetail = format("Stage %s [%s]", stageInfo.getStageId(), stageInfo.getState());
                        if (stageInfo.getTasks().isEmpty()) {
                            return stageDetail;
                        }
                        return stageDetail + stageInfo.getTasks().stream()
                                .map(TaskInfo::taskStatus)
                                .map(task -> {
                                    String taskDetail = format("Task %s [%s]", task.getTaskId(), task.getState());
                                    if (task.getFailures().isEmpty()) {
                                        return taskDetail;
                                    }
                                    return " -- Failures: " + task.getFailures().stream()
                                            .map(failure -> format("%s %s: %s", failure.getErrorCode(), failure.getType(), failure.getMessage()))
                                            .collect(Collectors.joining(", ", "[", "]"));
                                })
                                .collect(Collectors.joining("\n\t\t", ":\n\t\t", ""));
                    })
                    .collect(Collectors.joining("\n\n\t", "\nStages:\n\t", ""));
        }
    }

    @Test
    public void ensureTestNamingConvention()
    {
        // Enforce a naming convention to make code navigation easier.
        assertThat(getClass().getName())
                .doesNotEndWith("ConnectorTest")
                .doesNotEndWith("ConnectorSmokeTest");
    }

    protected Session getSession()
    {
        return queryRunner.getDefaultSession();
    }

    protected final int getNodeCount()
    {
        return queryRunner.getNodeCount();
    }

    protected TransactionBuilder newTransaction()
    {
        return transaction(queryRunner.getTransactionManager(), queryRunner.getPlannerContext().getMetadata(), queryRunner.getAccessControl());
    }

    protected void inTransaction(Consumer<Session> callback)
    {
        newTransaction().execute(getSession(), callback);
    }

    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return computeActual(getSession(), sql);
    }

    protected MaterializedResult computeActual(Session session, @Language("SQL") String sql)
    {
        return queryRunner.execute(session, sql).toTestTypes();
    }

    protected Object computeScalar(@Language("SQL") String sql)
    {
        return computeScalar(getSession(), sql);
    }

    protected Object computeScalar(Session session, @Language("SQL") String sql)
    {
        return computeActual(session, sql).getOnlyValue();
    }

    @CheckReturnValue
    protected AssertProvider<QueryAssert> query(@Language("SQL") String sql)
    {
        return query(getSession(), sql);
    }

    @CheckReturnValue
    protected AssertProvider<QueryAssert> query(Session session, @Language("SQL") String sql)
    {
        return queryAssertions.query(session, sql);
    }

    protected void assertQuery(@Language("SQL") String sql)
    {
        assertQuery(getSession(), sql);
    }

    protected void assertQuery(Session session, @Language("SQL") String sql)
    {
        QueryAssertions.assertQuery(queryRunner, session, sql, h2QueryRunner, sql, false, false);
    }

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), actual, h2QueryRunner, expected, false, false);
    }

    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, false, false);
    }

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected, Consumer<Plan> planAssertion)
    {
        assertQuery(getSession(), actual, expected, planAssertion);
    }

    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected, Consumer<Plan> planAssertion)
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, false, false, planAssertion);
    }

    protected void assertQueryEventually(Session session, @Language("SQL") String actual, @Language("SQL") String expected, Duration timeout)
    {
        QueryAssertions.assertQueryEventually(queryRunner, session, actual, h2QueryRunner, expected, false, false, Optional.empty(), timeout);
    }

    protected void assertQueryOrdered(@Language("SQL") String sql)
    {
        assertQueryOrdered(getSession(), sql);
    }

    protected void assertQueryOrdered(Session session, @Language("SQL") String sql)
    {
        assertQueryOrdered(session, sql, sql);
    }

    protected void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertQueryOrdered(getSession(), actual, expected);
    }

    protected void assertQueryOrdered(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, true, false);
    }

    protected void assertUpdate(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertUpdate(getSession(), actual, expected);
    }

    protected void assertUpdate(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        QueryAssertions.assertQuery(queryRunner, session, actual, h2QueryRunner, expected, false, true);
    }

    protected void assertUpdate(@Language("SQL") String sql)
    {
        assertUpdate(getSession(), sql);
    }

    protected void assertUpdate(Session session, @Language("SQL") String sql)
    {
        QueryAssertions.assertUpdate(queryRunner, session, sql, OptionalLong.empty(), Optional.empty());
    }

    protected void assertUpdate(@Language("SQL") String sql, long count)
    {
        assertUpdate(getSession(), sql, count);
    }

    protected void assertUpdate(Session session, @Language("SQL") String sql, long count)
    {
        QueryAssertions.assertUpdate(queryRunner, session, sql, OptionalLong.of(count), Optional.empty());
    }

    protected void assertUpdate(Session session, @Language("SQL") String sql, long count, Consumer<Plan> planAssertion)
    {
        QueryAssertions.assertUpdate(queryRunner, session, sql, OptionalLong.of(count), Optional.of(planAssertion));
    }

    protected void assertQuerySucceeds(@Language("SQL") String sql)
    {
        assertQuerySucceeds(getSession(), sql);
    }

    protected void assertQuerySucceeds(Session session, @Language("SQL") String sql)
    {
        QueryAssertions.assertQuerySucceeds(queryRunner, session, sql);
    }

    protected void assertQueryFailsEventually(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp, Duration timeout)
    {
        QueryAssertions.assertQueryFailsEventually(queryRunner, getSession(), sql, expectedMessageRegExp, timeout);
    }

    protected void assertQueryFails(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        QueryAssertions.assertQueryFails(queryRunner, getSession(), sql, expectedMessageRegExp);
    }

    protected void assertQueryFails(Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        QueryAssertions.assertQueryFails(queryRunner, session, sql, expectedMessageRegExp);
    }

    protected void assertQueryReturnsEmptyResult(@Language("SQL") String sql)
    {
        assertThat(query(sql))
                .returnsEmptyResult();
    }

    protected void assertQueryReturnsEmptyResult(Session session, @Language("SQL") String sql)
    {
        assertThat(query(session, sql))
                .returnsEmptyResult();
    }

    protected void assertAccessAllowed(@Language("SQL") String sql, TestingPrivilege... deniedPrivileges)
    {
        assertAccessAllowed(getSession(), sql, deniedPrivileges);
    }

    protected void assertAccessAllowed(Session session, @Language("SQL") String sql, TestingPrivilege... deniedPrivileges)
    {
        executeExclusively(session, sql, deniedPrivileges);
    }

    private void executeExclusively(Session session, @Language("SQL") String sql, TestingPrivilege[] deniedPrivileges)
    {
        executeExclusively(() -> {
            try {
                queryRunner.getAccessControl().deny(deniedPrivileges);
                queryRunner.execute(session, sql);
            }
            finally {
                queryRunner.getAccessControl().reset();
            }
        });
    }

    protected void assertAccessDenied(@Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingPrivilege... deniedPrivileges)
    {
        assertAccessDenied(getSession(), sql, exceptionsMessageRegExp, deniedPrivileges);
    }

    protected void assertAccessDenied(
            Session session,
            @Language("SQL") String sql,
            @Language("RegExp") String exceptionsMessageRegExp,
            TestingPrivilege... deniedPrivileges)
    {
        assertException(session, sql, ".*Access Denied: " + exceptionsMessageRegExp, deniedPrivileges);
    }

    protected void assertFunctionNotFound(
            Session session,
            @Language("SQL") String sql,
            String functionName,
            TestingPrivilege... deniedPrivileges)
    {
        assertException(session, sql, ".*[Ff]unction '" + functionName + "' not registered", deniedPrivileges);
    }

    private void assertException(Session session, @Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingPrivilege[] deniedPrivileges)
    {
        assertThatThrownBy(() -> executeExclusively(session, sql, deniedPrivileges))
                .as("Query: " + sql)
                .hasMessageMatching(exceptionsMessageRegExp);
    }

    protected void assertTableColumnNames(String tableName, String... columnNames)
    {
        MaterializedResult result = computeActual("DESCRIBE " + tableName);
        List<String> actual = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        assertThat(actual).as("Columns of table %s", tableName)
                .isEqualTo(List.of(columnNames));
    }

    protected void assertExplain(@Language("SQL") String query, @Language("RegExp") String... expectedExplainRegExps)
    {
        assertExplain(getSession(), query, expectedExplainRegExps);
    }

    protected void assertExplain(Session session, @Language("SQL") String query, @Language("RegExp") String... expectedExplainRegExps)
    {
        assertExplainAnalyze(false, session, query, expectedExplainRegExps);
    }

    protected void assertExplainAnalyze(@Language("SQL") String query, @Language("RegExp") String... expectedExplainRegExps)
    {
        assertExplainAnalyze(getSession(), query, expectedExplainRegExps);
    }

    protected void assertExplainAnalyze(Session session, @Language("SQL") String query, @Language("RegExp") String... expectedExplainRegExps)
    {
        assertExplainAnalyze(true, session, query, expectedExplainRegExps);
    }

    private void assertExplainAnalyze(
            boolean analyze,
            Session session,
            @Language("SQL") String query,
            @Language("RegExp") String... expectedExplainRegExps)
    {
        String value = (String) computeActual(session, query).getOnlyValue();

        if (analyze) {
            // TODO: check that rendered plan is as expected, once stats are collected in a consistent way
            // assertTrue(value.contains("Cost: "), format("Expected output to contain \"Cost: \", but it is %s", value));
            assertThat(value).containsPattern("CPU:.*, Input:.*, Output");
        }

        for (String expectedExplainRegExp : expectedExplainRegExps) {
            assertThat(value).containsPattern(expectedExplainRegExp);
        }
    }

    protected void assertQueryStats(
            Session session,
            @Language("SQL") String query,
            Consumer<QueryStats> queryStatsAssertion,
            Consumer<MaterializedResult> resultAssertion)
    {
        QueryRunner queryRunner = getDistributedQueryRunner();
        MaterializedResultWithPlan result = queryRunner.executeWithPlan(session, query);
        QueryStats queryStats = queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats();
        queryStatsAssertion.accept(queryStats);
        resultAssertion.accept(result.result());
    }

    protected void assertNoDataRead(@Language("SQL") String sql)
    {
        assertQueryStats(
                getSession(),
                sql,
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isEqualTo(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));
    }

    protected MaterializedResult computeExpected(@Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        return h2QueryRunner.execute(getSession(), sql, resultTypes);
    }

    protected void executeExclusively(Runnable executionBlock)
    {
        queryRunner.getExclusiveLock().lock();
        try {
            executionBlock.run();
        }
        finally {
            queryRunner.getExclusiveLock().unlock();
        }
    }

    protected String formatSqlText(@Language("SQL") String sql)
    {
        return formatSql(SQL_PARSER.createStatement(sql));
    }

    protected String formatPlan(Session session, Plan plan)
    {
        Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();
        FunctionManager functionManager = getDistributedQueryRunner().getPlannerContext().getFunctionManager();
        return inTransaction(session, transactionSession -> textLogicalPlan(plan.getRoot(), metadata, functionManager, StatsAndCosts.empty(), transactionSession, 0, false));
    }

    protected String getExplainPlan(@Language("SQL") String query, ExplainType.Type planType)
    {
        return getExplainPlan(getSession(), query, planType);
    }

    protected String getExplainPlan(Session session, @Language("SQL") String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = queryRunner.getQueryExplainer();
        return newTransaction()
                .singleStatement()
                .execute(session, transactionSession -> {
                    return explainer.getPlan(transactionSession, SQL_PARSER.createStatement(query), planType, emptyList(), WarningCollector.NOOP, createPlanOptimizersStatsCollector());
                });
    }

    protected String getGraphvizExplainPlan(@Language("SQL") String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = queryRunner.getQueryExplainer();
        return newTransaction()
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getGraphvizPlan(session, SQL_PARSER.createStatement(query), planType, emptyList(), WarningCollector.NOOP, createPlanOptimizersStatsCollector());
                });
    }

    protected final QueryRunner getQueryRunner()
    {
        checkState(queryRunner != null, "queryRunner not set");
        return queryRunner;
    }

    protected final DistributedQueryRunner getDistributedQueryRunner()
    {
        checkState(queryRunner != null, "queryRunner not set");
        checkState(queryRunner instanceof DistributedQueryRunner, "queryRunner is not a DistributedQueryRunner: %s [%s]", queryRunner, queryRunner.getClass());
        return (DistributedQueryRunner) queryRunner;
    }

    private Optional<DistributedQueryRunner> tryGetDistributedQueryRunner()
    {
        if (queryRunner != null && queryRunner instanceof DistributedQueryRunner runner) {
            return Optional.of(runner);
        }
        return Optional.empty();
    }

    protected Session noJoinReordering()
    {
        return noJoinReordering(JoinDistributionType.PARTITIONED);
    }

    protected Session noJoinReordering(JoinDistributionType distributionType)
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, distributionType.name())
                .build();
    }

    protected OperatorStats searchScanFilterAndProjectOperatorStats(QueryId queryId, QualifiedObjectName catalogSchemaTableName)
    {
        DistributedQueryRunner runner = getDistributedQueryRunner();
        Plan plan = runner.getQueryPlan(queryId);
        PlanNodeId nodeId = PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> {
                    if (!(node instanceof ProjectNode projectNode)) {
                        return false;
                    }
                    if (!(projectNode.getSource() instanceof FilterNode filterNode)) {
                        return false;
                    }
                    if (!(filterNode.getSource() instanceof TableScanNode tableScanNode)) {
                        return false;
                    }
                    CatalogSchemaTableName tableName = getTableName(tableScanNode.getTable());
                    return tableName.equals(catalogSchemaTableName.asCatalogSchemaTableName());
                })
                .findOnlyElement()
                .getId();

        return extractOperatorStatsForNodeId(queryId, nodeId, "ScanFilterAndProjectOperator");
    }

    protected OperatorStats extractOperatorStatsForNodeId(QueryId queryId, PlanNodeId nodeId, String operatorType)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeId.equals(summary.getPlanNodeId()) && operatorType.equals(summary.getOperatorType()))
                .collect(MoreCollectors.onlyElement());
    }

    protected DynamicFiltersStats getDynamicFilteringStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getDynamicFiltersStats();
    }

    protected QualifiedObjectName getQualifiedTableName(String tableName)
    {
        Session session = getQueryRunner().getDefaultSession();
        return new QualifiedObjectName(
                session.getCatalog().orElseThrow(),
                session.getSchema().orElseThrow(),
                tableName);
    }

    private CatalogSchemaTableName getTableName(TableHandle tableHandle)
    {
        return inTransaction(getSession(), transactionSession -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            getQueryRunner().getPlannerContext().getMetadata().getCatalogHandle(transactionSession, tableHandle.catalogHandle().getCatalogName().toString());
            return getQueryRunner().getPlannerContext().getMetadata().getTableName(transactionSession, tableHandle);
        });
    }

    private <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer)
    {
        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getPlannerContext().getMetadata(), getQueryRunner().getAccessControl())
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    @CanIgnoreReturnValue
    protected final <T extends AutoCloseable> T closeAfterClass(T resource)
    {
        checkState(
                afterClassCloser != null,
                "closeAfterClass invoked before test is initialized or after it is torn down. " +
                        "In particular, make sure you do not allocate any resources in a test class constructor, " +
                        "as this can easily lead to OutOfMemoryErrors and other types of test flakiness.");
        return afterClassCloser.register(resource);
    }
}
