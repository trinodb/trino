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
import io.trino.metadata.Metadata;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class BasePlanTest
{
    private final Map<String, String> sessionProperties;
    private PlanTester planTester;

    public BasePlanTest()
    {
        this(ImmutableMap.of());
    }

    public BasePlanTest(Map<String, String> sessionProperties)
    {
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    // Subclasses should implement this method to inject their own query runners
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        sessionProperties.forEach(sessionBuilder::setSystemProperty);

        PlanTester planTester = PlanTester.create(sessionBuilder.build());

        planTester.createCatalog(planTester.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        return planTester;
    }

    @BeforeAll
    public final void initPlanTest()
    {
        this.planTester = createPlanTester();
    }

    @AfterAll
    public final void destroyPlanTest()
    {
        closeAllRuntimeException(planTester);
        planTester = null;
    }

    protected CatalogHandle getCurrentCatalogHandle()
    {
        return planTester.inTransaction(transactionSession -> planTester.getPlannerContext().getMetadata().getCatalogHandle(transactionSession, transactionSession.getCatalog().get())).get();
    }

    protected CatalogHandle getCatalogHandle(String catalogName)
    {
        return planTester.inTransaction(transactionSession -> planTester.getPlannerContext().getMetadata().getCatalogHandle(transactionSession, catalogName)).get();
    }

    protected PlanTester getPlanTester()
    {
        return planTester;
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
        List<PlanOptimizer> optimizers = planTester.getPlanOptimizers(true);

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(@Language("SQL") String sql, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        assertPlan(sql, OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlan(@Language("SQL") String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, Predicate<PlanOptimizer> optimizerPredicate)
    {
        List<PlanOptimizer> optimizers = planTester.getPlanOptimizers(true).stream()
                .filter(optimizerPredicate)
                .collect(toList());

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(@Language("SQL") String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        try {
            planTester.inTransaction(transactionSession -> {
                Plan actualPlan = planTester.createPlan(transactionSession, sql, optimizers, stage, NOOP, createPlanOptimizersStatsCollector());
                PlanAssert.assertPlan(transactionSession, planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), planTester.getStatsCalculator(), actualPlan, pattern);
                return null;
            });
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }

    protected void assertDistributedPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        assertDistributedPlan(sql, getPlanTester().getDefaultSession(), pattern);
    }

    protected void assertDistributedPlan(@Language("SQL") String sql, Session session, PlanMatchPattern pattern)
    {
        assertPlanWithSession(sql, session, false, pattern);
    }

    protected void assertMinimallyOptimizedPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        Metadata metadata = getPlanTester().getPlannerContext().getMetadata();
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(metadata),
                new IterativeOptimizer(
                        planTester.getPlannerContext(),
                        new RuleStatsRecorder(),
                        planTester.getStatsCalculator(),
                        planTester.getCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(columnPruningRules(metadata))
                                .build()));

        assertPlan(sql, OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern)
    {
        try {
            planTester.inTransaction(session, transactionSession -> {
                Plan actualPlan = planTester.createPlan(
                        transactionSession,
                        sql,
                        planTester.getPlanOptimizers(forceSingleNode),
                        OPTIMIZED_AND_VALIDATED,
                        NOOP,
                        createPlanOptimizersStatsCollector());
                PlanAssert.assertPlan(transactionSession, planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), planTester.getStatsCalculator(), actualPlan, pattern);
                return null;
            });
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern, Consumer<Plan> planValidator)
    {
        try {
            planTester.inTransaction(session, transactionSession -> {
                Plan actualPlan = planTester.createPlan(transactionSession, sql, planTester.getPlanOptimizers(forceSingleNode), OPTIMIZED_AND_VALIDATED, NOOP, createPlanOptimizersStatsCollector());
                PlanAssert.assertPlan(transactionSession, planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), planTester.getStatsCalculator(), actualPlan, pattern);
                planValidator.accept(actualPlan);
                return null;
            });
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
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
            return planTester.inTransaction(planTester.getDefaultSession(), transactionSession ->
                    planTester.createPlan(transactionSession, sql, planTester.getPlanOptimizers(forceSingleNode), stage, NOOP, createPlanOptimizersStatsCollector()));
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    protected SubPlan subplan(@Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode)
    {
        return subplan(sql, stage, forceSingleNode, getPlanTester().getDefaultSession());
    }

    protected SubPlan subplan(@Language("SQL") String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, Session session)
    {
        try {
            return planTester.inTransaction(session, transactionSession -> {
                Plan plan = planTester.createPlan(transactionSession, sql, planTester.getPlanOptimizers(forceSingleNode), stage, NOOP, createPlanOptimizersStatsCollector());
                return planTester.createSubPlans(transactionSession, plan, forceSingleNode);
            });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }
}
