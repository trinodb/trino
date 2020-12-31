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
package io.prestosql.sql.planner.iterative;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.planner.RuleStatsRecorder;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestIterativeOptimizer
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty("iterative_optimizer_timeout", "1ms");

        queryRunner = LocalQueryRunner.create(sessionBuilder.build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test(timeOut = 1000)
    public void optimizerTimeoutsOnNonConvergingPlan()
    {
        PlanOptimizer optimizer = new IterativeOptimizer(
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.of(new AddIdentityOverTableScan(), new RemoveRedundantIdentityProjections()));

        try {
            queryRunner.inTransaction(transactionSession -> {
                queryRunner.createPlan(transactionSession, "SELECT nationkey FROM nation", ImmutableList.of(optimizer), WarningCollector.NOOP);
                fail("The optimizer should not converge");
                return null;
            });
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), OPTIMIZER_TIMEOUT.toErrorCode());
        }
    }

    private static class AddIdentityOverTableScan
            implements Rule<TableScanNode>
    {
        @Override
        public Pattern<TableScanNode> getPattern()
        {
            return tableScan();
        }

        @Override
        public Result apply(TableScanNode tableScan, Captures captures, Context context)
        {
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), tableScan, Assignments.identity(tableScan.getOutputSymbols())));
        }
    }
}
