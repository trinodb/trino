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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class BasePlanTest
{
    private final Map<String, String> sessionProperties;
    private LocalQueryRunner queryRunner;

    public BasePlanTest()
    {
        this(ImmutableMap.of());
    }

    public BasePlanTest(Map<String, String> sessionProperties)
    {
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    // Subclasses should implement this method to inject their own query runners
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        sessionProperties.forEach(sessionBuilder::setSystemProperty);

        LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        return queryRunner;
    }

    @BeforeClass
    public final void initPlanTest()
    {
        this.queryRunner = createLocalQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroyPlanTest()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    protected CatalogName getCurrentConnectorId()
    {
        return queryRunner.inTransaction(transactionSession -> queryRunner.getMetadata().getCatalogHandle(transactionSession, transactionSession.getCatalog().get())).get();
    }

    protected LocalQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    protected void assertPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, OPTIMIZED_AND_VALIDATED, pattern);
    }

    protected void assertPlan(@Language("SQL") String sql, Session session, PlanMatchPattern pattern)
    {
        assertPlanWithSession(sql, session, true, pattern);
    }

    protected void assertPlan(@Language("SQL") String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(@Language("SQL") String sql, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        assertPlan(sql, OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlan(@Language("SQL") String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, Predicate<PlanOptimizer> optimizerPredicate)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true).stream()
                .filter(optimizerPredicate)
                .collect(toList());

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(@Language("SQL") String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers, stage, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getFunctionManager(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertDistributedPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        assertDistributedPlan(sql, getQueryRunner().getDefaultSession(), pattern);
    }

    protected void assertDistributedPlan(@Language("SQL") String sql, Session session, PlanMatchPattern pattern)
    {
        assertPlanWithSession(sql, session, false, pattern);
    }

    protected void assertMinimallyOptimizedPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(getQueryRunner().getMetadata()),
                new IterativeOptimizer(
                        queryRunner.getPlannerContext(),
                        new RuleStatsRecorder(),
                        queryRunner.getStatsCalculator(),
                        queryRunner.getCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(columnPruningRules(getQueryRunner().getMetadata()))
                                .build()));

        assertPlan(sql, OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, OPTIMIZED_AND_VALIDATED, forceSingleNode, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getFunctionManager(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern, Consumer<Plan> planValidator)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, OPTIMIZED_AND_VALIDATED, forceSingleNode, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getFunctionManager(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            planValidator.accept(actualPlan);
            return null;
        });
    }

    protected Plan plan(@Language("SQL") String sql)
    {
        return plan(sql, OPTIMIZED_AND_VALIDATED);
    }

    protected Plan plan(@Language("SQL") String sql, LogicalPlanner.Stage stage)
    {
        return plan(sql, stage, true);
    }

    protected Plan plan(@Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode)
    {
        try {
            return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, stage, forceSingleNode, WarningCollector.NOOP));
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    protected SubPlan subplan(@Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode)
    {
        return subplan(sql, stage, forceSingleNode, getQueryRunner().getDefaultSession());
    }

    protected SubPlan subplan(@Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, Session session)
    {
        try {
            return queryRunner.inTransaction(session, transactionSession -> {
                Plan plan = queryRunner.createPlan(transactionSession, sql, stage, forceSingleNode, WarningCollector.NOOP);
                return queryRunner.createSubPlans(transactionSession, plan, forceSingleNode);
            });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }
}
