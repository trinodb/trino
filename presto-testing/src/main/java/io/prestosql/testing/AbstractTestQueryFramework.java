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
package io.prestosql.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostCalculatorUsingExchanges;
import io.prestosql.cost.CostCalculatorWithEstimatedExchanges;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.TaskCountEstimator;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.OperatorStats;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanFragmenter;
import io.prestosql.sql.planner.PlanOptimizers;
import io.prestosql.sql.planner.RuleStatsRecorder;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.query.QueryAssertions.QueryAssert;
import io.prestosql.sql.tree.ExplainType;
import io.prestosql.testing.TestingAccessControlManager.TestingPrivilege;
import io.prestosql.util.AutoCloseableCloser;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.SqlFormatter.formatSql;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueryFramework
{
    private QueryRunner queryRunner;
    private H2QueryRunner h2QueryRunner;
    private SqlParser sqlParser;
    private final AutoCloseableCloser afterClassCloser = AutoCloseableCloser.create();
    private io.prestosql.sql.query.QueryAssertions queryAssertions;

    @BeforeClass
    public void init()
            throws Exception
    {
        queryRunner = afterClassCloser.register(createQueryRunner());
        h2QueryRunner = afterClassCloser.register(new H2QueryRunner());
        sqlParser = new SqlParser();
        queryAssertions = new io.prestosql.sql.query.QueryAssertions(queryRunner);
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
        return queryAssertions.query(sql);
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
        executeExclusively(() -> {
            try {
                queryRunner.getAccessControl().deny(deniedPrivileges);
                queryRunner.execute(session, sql);
                fail("Expected " + AccessDeniedException.class.getSimpleName());
            }
            catch (RuntimeException e) {
                assertExceptionMessage(sql, e, ".*Access Denied: " + exceptionsMessageRegExp);
            }
            finally {
                queryRunner.getAccessControl().reset();
            }
        });
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

    private static void assertExceptionMessage(String sql, Exception exception, @Language("RegExp") String regex)
    {
        if (!nullToEmpty(exception.getMessage()).matches(regex)) {
            fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), regex, sql), exception);
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
        return formatSql(sqlParser.createStatement(sql, createParsingOptions(queryRunner.getDefaultSession())));
    }

    //TODO: should WarningCollector be added?
    protected String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), session -> {
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
        List<PlanOptimizer> optimizers = new PlanOptimizers(
                metadata,
                new TypeAnalyzer(sqlParser, metadata),
                new TaskManagerConfig(),
                forceSingleNode,
                new MBeanExporter(new TestingMBeanServer()),
                queryRunner.getSplitManager(),
                queryRunner.getPageSourceManager(),
                queryRunner.getStatsCalculator(),
                costCalculator,
                new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator),
                new CostComparator(featuresConfig),
                taskCountEstimator,
                new RuleStatsRecorder()).get();
        return new QueryExplainer(
                optimizers,
                new PlanFragmenter(metadata, queryRunner.getNodePartitioningManager(), new QueryManagerConfig()),
                metadata,
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
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
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
                    return tableName.equals(tableScanNode.getTable().getConnectorHandle().toString());
                })
                .findOnlyElement()
                .getId();

        return runner.getCoordinator()
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
