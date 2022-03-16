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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.warnings.WarningCollector;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.MemoryPool;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.operator.OperatorStats;
import io.trino.server.BasicQueryInfo;
import io.trino.server.DynamicFilterService.DynamicFiltersStats;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
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
import io.trino.testing.TestingAccessControlManager.TestingPrivilege;
import io.trino.transaction.TransactionBuilder;
import io.trino.util.AutoCloseableCloser;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueryFramework
{
    private QueryRunner queryRunner;
    private H2QueryRunner h2QueryRunner;
    private SqlParser sqlParser;
    private final AutoCloseableCloser afterClassCloser = AutoCloseableCloser.create();
    private io.trino.sql.query.QueryAssertions queryAssertions;

    @BeforeClass
    public void init()
            throws Exception
    {
        queryRunner = afterClassCloser.register(createQueryRunner());
        h2QueryRunner = afterClassCloser.register(new H2QueryRunner());
        sqlParser = new SqlParser();
        queryAssertions = new io.trino.sql.query.QueryAssertions(queryRunner);
    }

    protected abstract QueryRunner createQueryRunner()
            throws Exception;

    @AfterClass(alwaysRun = true)
    public final void close()
            throws Exception
    {
        try (afterClassCloser) {
            checkQueryMemoryReleased();
        }
        finally {
            queryRunner = null;
            h2QueryRunner = null;
            sqlParser = null;
            queryAssertions = null;
        }
    }

    private void checkQueryMemoryReleased()
    {
        if (queryRunner == null) {
            return;
        }
        if (!(queryRunner instanceof DistributedQueryRunner)) {
            return;
        }
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) queryRunner;
        assertEventually(
                new Duration(30, SECONDS),
                new Duration(1, SECONDS),
                () -> {
                    List<TestingTrinoServer> servers = distributedQueryRunner.getServers();
                    for (int serverId = 0; serverId < servers.size(); ++serverId) {
                        TestingTrinoServer server = servers.get(serverId);
                        assertMemoryPoolReleased(distributedQueryRunner.getCoordinator(), server, serverId);
                    }

                    assertThat(distributedQueryRunner.getCoordinator().getClusterMemoryManager().getClusterTotalMemoryReservation())
                            .describedAs("cluster memory reservation")
                            .isZero();
                });
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
                .filter(query -> queryReservations.keySet().contains(query.getQueryId()))
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
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl());
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

    protected AssertProvider<QueryAssert> query(@Language("SQL") String sql)
    {
        return query(getSession(), sql);
    }

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
        checkArgument(queryRunner instanceof DistributedQueryRunner, "pattern assertion is only supported for DistributedQueryRunner");
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

    private void assertException(Session session, @Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingPrivilege[] deniedPrivileges)
    {
        assertThatThrownBy(() -> executeExclusively(session, sql, deniedPrivileges))
                .as("Query: " + sql)
                .hasMessageMatching(exceptionsMessageRegExp);
    }

    protected void assertTableColumnNames(String tableName, String... columnNames)
    {
        MaterializedResult result = computeActual("DESCRIBE " + tableName);
        List<String> expected = ImmutableList.copyOf(columnNames);
        List<String> actual = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        assertEquals(actual, expected);
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
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        ResultWithQueryId<MaterializedResult> resultWithQueryId = queryRunner.executeWithQueryId(session, query);
        QueryStats queryStats = queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(resultWithQueryId.getQueryId())
                .getQueryStats();
        queryStatsAssertion.accept(queryStats);
        resultAssertion.accept(resultWithQueryId.getResult());
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

    protected String formatSqlText(String sql)
    {
        return formatSql(sqlParser.createStatement(sql, createParsingOptions(getSession())));
    }

    //TODO: should WarningCollector be added?
    protected String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = queryRunner.getQueryExplainer();
        return newTransaction()
                .singleStatement()
                .execute(getSession(), session -> {
                    return explainer.getPlan(session, sqlParser.createStatement(query, createParsingOptions(session)), planType, emptyList(), WarningCollector.NOOP);
                });
    }

    protected String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = queryRunner.getQueryExplainer();
        return newTransaction()
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getGraphvizPlan(session, sqlParser.createStatement(query, createParsingOptions(session)), planType, emptyList(), WarningCollector.NOOP);
                });
    }

    protected static void skipTestUnless(boolean requirement)
    {
        if (!requirement) {
            throw new SkipException("requirement not met");
        }
    }

    protected final QueryRunner getQueryRunner()
    {
        checkState(queryRunner != null, "queryRunner not set");
        return queryRunner;
    }

    protected final DistributedQueryRunner getDistributedQueryRunner()
    {
        checkState(queryRunner != null, "queryRunner not set");
        checkState(queryRunner instanceof DistributedQueryRunner, "queryRunner is not a DistributedQueryRunner");
        return (DistributedQueryRunner) queryRunner;
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
                    if (!(node instanceof ProjectNode)) {
                        return false;
                    }
                    ProjectNode projectNode = (ProjectNode) node;
                    if (!(projectNode.getSource() instanceof FilterNode)) {
                        return false;
                    }
                    FilterNode filterNode = (FilterNode) projectNode.getSource();
                    if (!(filterNode.getSource() instanceof TableScanNode)) {
                        return false;
                    }
                    TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
                    TableMetadata tableMetadata = getTableMetadata(tableScanNode.getTable());
                    return tableMetadata.getQualifiedName().equals(catalogSchemaTableName);
                })
                .findOnlyElement()
                .getId();

        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeId.equals(summary.getPlanNodeId()) && summary.getOperatorType().equals("ScanFilterAndProjectOperator"))
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

    private TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        return inTransaction(getSession(), transactionSession -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            getQueryRunner().getMetadata().getCatalogHandle(transactionSession, tableHandle.getCatalogName().getCatalogName());
            return getQueryRunner().getMetadata().getTableMetadata(transactionSession, tableHandle);
        });
    }

    private <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer)
    {
        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    @CanIgnoreReturnValue
    protected final <T extends AutoCloseable> T closeAfterClass(T resource)
    {
        return afterClassCloser.register(resource);
    }
}
