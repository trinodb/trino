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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.ir.Constant;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestCanonicalize
        extends BasePlanTest
{
    @Test
    public void testJoin()
    {
        // canonicalization + constant folding
        assertPlan(
                "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT EXTRACT(DAY FROM DATE '2017-01-01')\n" +
                        ") t\n" +
                        "CROSS JOIN (VALUES 2)",
                anyTree(
                        values(ImmutableList.of("expr", "field"), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L), new Constant(INTEGER, 2L))))));
    }

    @Test
    public void testDuplicatesInWindowOrderBy()
    {
        ExpectedValueProvider<DataOrganizationSpecification> specification = specification(
                ImmutableList.of(),
                ImmutableList.of("A"),
                ImmutableMap.of("A", SortOrder.ASC_NULLS_LAST));

        assertPlan(
                "WITH x as (SELECT a, a as b FROM (VALUES 1) t(a))" +
                        "SELECT *, row_number() OVER(ORDER BY a ASC, b DESC)" +
                        "FROM x",
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification)
                                        .addFunction(windowFunction("row_number", ImmutableList.of(), DEFAULT_FRAME)),
                                values("A"))),
                ImmutableList.of(
                        new UnaliasSymbolReferences(),
                        new IterativeOptimizer(
                                getPlanTester().getPlannerContext(),
                                new RuleStatsRecorder(),
                                getPlanTester().getStatsCalculator(),
                                getPlanTester().getCostCalculator(),
                                ImmutableSet.of(new RemoveRedundantIdentityProjections()))));
    }
}
