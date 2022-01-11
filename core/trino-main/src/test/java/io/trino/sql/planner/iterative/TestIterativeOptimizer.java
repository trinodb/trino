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
package io.trino.sql.planner.iterative;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

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

    @Test(timeOut = 10_000)
    public void optimizerTimeoutsOnNonConvergingPlan()
    {
        PlanOptimizer optimizer = new IterativeOptimizer(
                queryRunner.getMetadata(),
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.of(new AddIdentityOverTableScan(), new RemoveRedundantIdentityProjections()));

        assertTrinoExceptionThrownBy(() -> queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, "SELECT nationkey FROM nation", ImmutableList.of(optimizer), WarningCollector.NOOP)))
                .hasErrorCode(OPTIMIZER_TIMEOUT)
                .hasMessage("The optimizer exhausted the time limit of 1 ms");
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
