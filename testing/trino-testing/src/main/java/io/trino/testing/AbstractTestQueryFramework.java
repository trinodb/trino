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
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculatorUsingExchanges;
import io.trino.cost.CostCalculatorWithEstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.TaskCountEstimator;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanOptimizers;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.TestingAccessControlManager.TestingPrivilege;
import io.trino.util.AutoCloseableCloser;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

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
    public void close()
            throws Exception
    {
        afterClassCloser.close();
        queryRunner = null;
        h2QueryRunner = null;
        sqlParser = null;
        queryAssertions = null;
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
        return computeActual(sql).getOnlyValue();
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
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(getSession(), session -> {
                    return explainer.getPlan(session, sqlParser.createStatement(query, createParsingOptions(session)), planType, emptyList(), WarningCollector.NOOP);
                });
    }

    protected String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
                    return explainer.getGraphvizPlan(session, sqlParser.createStatement(query, createParsingOptions(session)), planType, emptyList(), WarningCollector.NOOP);
                });
    }

    private QueryExplainer getQueryExplainer()
    {
        Metadata metadata = queryRunner.getMetadata();
        FeaturesConfig featuresConfig = new FeaturesConfig().setOptimizeHashGeneration(true);
        boolean forceSingleNode = queryRunner.getNodeCount() == 1;
        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(queryRunner::getNodeCount);
        CostCalculator costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        TypeOperators typeOperators = new TypeOperators();
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(sqlParser, metadata);
        List<PlanOptimizer> optimizers = new PlanOptimizers(
                metadata,
                typeOperators,
                typeAnalyzer,
                new TaskManagerConfig(),
                forceSingleNode,
                queryRunner.getSplitManager(),
                queryRunner.getPageSourceManager(),
                queryRunner.getStatsCalculator(),
                new ScalarStatsCalculator(metadata, typeAnalyzer),
                costCalculator,
                new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator),
                new CostComparator(featuresConfig),
                taskCountEstimator,
                queryRunner.getNodePartitioningManager()).get();
        return new QueryExplainer(
                optimizers,
                new PlanFragmenter(metadata, queryRunner.getNodePartitioningManager(), new QueryManagerConfig()),
                metadata,
                typeOperators,
                queryRunner.getGroupProvider(),
                queryRunner.getAccessControl(),
                sqlParser,
                queryRunner.getStatsCalculator(),
                costCalculator,
                ImmutableMap.of());
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
                .setSystemProperty(JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, distributionType.name())
                .build();
    }

    protected OperatorStats searchScanFilterAndProjectOperatorStats(QueryId queryId, String tableName)
    {
        Plan plan = getDistributedQueryRunner().getQueryPlan(queryId);
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
                    return tableName.equals(tableScanNode.getTable().getConnectorHandle().toString());
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

    @CanIgnoreReturnValue
    protected final <T extends AutoCloseable> T closeAfterClass(T resource)
    {
        return afterClassCloser.register(resource);
    }
}
