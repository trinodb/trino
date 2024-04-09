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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.GatherAndMergeWindows;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestReorderWindows
        extends BasePlanTest
{
    private static final String DISCOUNT_ALIAS = "DISCOUNT";
    private static final String ORDERKEY_ALIAS = "ORDERKEY";
    private static final String QUANTITY_ALIAS = "QUANTITY";
    private static final String PARTKEY_ALIAS = "PARTKEY";
    private static final String RECEIPTDATE_ALIAS = "RECEIPTDATE";
    private static final String SHIPDATE_ALIAS = "SHIPDATE";
    private static final String SUPPKEY_ALIAS = "SUPPKEY";
    private static final String TAX_ALIAS = "TAX";

    private static final PlanMatchPattern LINEITEM_TABLESCAN_DOQPRSST;
    private static final PlanMatchPattern LINEITEM_TABLESCAN_DOQRST;

    private static final ExpectedValueProvider<DataOrganizationSpecification> windowA;
    private static final ExpectedValueProvider<DataOrganizationSpecification> windowAp;
    private static final ExpectedValueProvider<DataOrganizationSpecification> windowApp;
    private static final ExpectedValueProvider<DataOrganizationSpecification> windowB;
    private static final ExpectedValueProvider<DataOrganizationSpecification> windowC;
    private static final ExpectedValueProvider<DataOrganizationSpecification> windowD;
    private static final ExpectedValueProvider<DataOrganizationSpecification> windowE;

    static {
        ImmutableMap.Builder<String, String> columns = ImmutableMap.builder();
        columns.put(DISCOUNT_ALIAS, "discount");
        columns.put(ORDERKEY_ALIAS, "orderkey");
        columns.put(QUANTITY_ALIAS, "quantity");
        columns.put(PARTKEY_ALIAS, "partkey");
        columns.put(RECEIPTDATE_ALIAS, "receiptdate");
        columns.put(SHIPDATE_ALIAS, "shipdate");
        columns.put(SUPPKEY_ALIAS, "suppkey");
        columns.put(TAX_ALIAS, "tax");
        LINEITEM_TABLESCAN_DOQPRSST = tableScan("lineitem", columns.buildOrThrow());

        columns = ImmutableMap.builder();
        columns.put(DISCOUNT_ALIAS, "discount");
        columns.put(ORDERKEY_ALIAS, "orderkey");
        columns.put(QUANTITY_ALIAS, "quantity");
        columns.put(RECEIPTDATE_ALIAS, "receiptdate");
        columns.put(SUPPKEY_ALIAS, "suppkey");
        columns.put(TAX_ALIAS, "tax");
        LINEITEM_TABLESCAN_DOQRST = tableScan("lineitem", columns.buildOrThrow());

        windowA = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowAp = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(SHIPDATE_ALIAS),
                ImmutableMap.of(SHIPDATE_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowApp = specification(
                ImmutableList.of(SUPPKEY_ALIAS, TAX_ALIAS),
                ImmutableList.of(RECEIPTDATE_ALIAS),
                ImmutableMap.of(RECEIPTDATE_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowB = specification(
                ImmutableList.of(PARTKEY_ALIAS),
                ImmutableList.of(RECEIPTDATE_ALIAS),
                ImmutableMap.of(RECEIPTDATE_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowC = specification(
                ImmutableList.of(RECEIPTDATE_ALIAS),
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableMap.of(SUPPKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowD = specification(
                ImmutableList.of(TAX_ALIAS),
                ImmutableList.of(RECEIPTDATE_ALIAS),
                ImmutableMap.of(RECEIPTDATE_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowE = specification(
                ImmutableList.of(QUANTITY_ALIAS),
                ImmutableList.of(RECEIPTDATE_ALIAS),
                ImmutableMap.of(RECEIPTDATE_ALIAS, SortOrder.ASC_NULLS_LAST));
    }

    @Test
    public void testNonMergeableABAReordersToAABAllOptimizers()
    {
        @Language("SQL") String sql = "select " +
                "sum(quantity) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) sum_quantity_A, " +
                "avg(discount) over(PARTITION BY partkey ORDER BY receiptdate ASC NULLS LAST) avg_discount_B, " +
                "min(tax) over(PARTITION BY suppkey ORDER BY shipdate ASC NULLS LAST) min_tax_A " +
                "from lineitem";

        PlanMatchPattern pattern =
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowAp)
                                        .addFunction(windowFunction("min", ImmutableList.of(TAX_ALIAS), DEFAULT_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowA)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                                project(
                                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                                        .specification(windowB)
                                                                        .addFunction(windowFunction("avg", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                                                anyTree(LINEITEM_TABLESCAN_DOQPRSST)))))));

        assertPlan(sql, pattern);
    }

    @Test
    public void testNonMergeableABAReordersToAAB()
    {
        @Language("SQL") String sql = "select " +
                "sum(quantity) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) sum_quantity_A, " +
                "avg(discount) over(PARTITION BY partkey ORDER BY receiptdate ASC NULLS LAST) avg_discount_B, " +
                "min(tax) over(PARTITION BY suppkey ORDER BY shipdate ASC NULLS LAST) min_tax_A " +
                "from lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowAp)
                                        .addFunction(windowFunction("min", ImmutableList.of(TAX_ALIAS), DEFAULT_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowA)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                                project(
                                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                                        .specification(windowB)
                                                                        .addFunction(windowFunction("avg", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                                                LINEITEM_TABLESCAN_DOQPRSST)))))));
    }

    @Test
    public void testPrefixOfPartitionComesFirstRegardlessOfTheirOrderInSQL()
    {
        assertUnitPlan(
                "select " +
                        "avg(discount) over(PARTITION BY suppkey, tax ORDER BY receiptdate ASC NULLS LAST) avg_discount_A, " +
                        "sum(quantity) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) sum_quantity_A " +
                        "from lineitem",
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowApp)
                                        .addFunction(windowFunction("avg", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowA)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                                LINEITEM_TABLESCAN_DOQRST)))));

        assertUnitPlan(
                "select " +
                        "sum(quantity) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) sum_quantity_A, " +
                        "avg(discount) over(PARTITION BY suppkey, tax ORDER BY receiptdate ASC NULLS LAST) avg_discount_A " +
                        "from lineitem",
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowApp)
                                        .addFunction(windowFunction("avg", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowA)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                                LINEITEM_TABLESCAN_DOQRST)))));
    }

    @Test
    public void testReorderAcrossProjectNodes()
    {
        @Language("SQL") String sql = "select " +
                "avg(discount) over(PARTITION BY suppkey, tax ORDER BY receiptdate ASC NULLS LAST) avg_discount_A, " +
                "lag(quantity, 1) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) lag_quantity_A " + // produces ProjectNode because of constant 1
                "from lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowApp)
                                        .addFunction(windowFunction("avg", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowA)
                                                        .addFunction(windowFunction("lag", ImmutableList.of(QUANTITY_ALIAS, "ONE"), DEFAULT_FRAME)),
                                                project(ImmutableMap.of("ONE", expression(new Cast(new Reference(INTEGER, "expr"), BIGINT))),
                                                        project(ImmutableMap.of("expr", expression(new Constant(INTEGER, 1L))),
                                                                LINEITEM_TABLESCAN_DOQRST)))))));
    }

    @Test
    public void testNotReorderAcrossNonPartitionFilter()
    {
        @Language("SQL") String sql = "" +
                "SELECT " +
                "  avg_discount_APP, " +
                "  AVG(quantity) OVER(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) avg_quantity_A " +
                "FROM ( " +
                "   SELECT " +
                "     *, " +
                "     AVG(discount) OVER(PARTITION BY suppkey, tax ORDER BY receiptdate ASC NULLS LAST) avg_discount_APP " +
                "   FROM lineitem) " +
                "WHERE receiptdate IS NOT NULL";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowA)
                                        .addFunction(windowFunction("avg", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                project(
                                        filter(
                                                new Not(new IsNull(new Reference(VARCHAR, RECEIPTDATE_ALIAS))),
                                                project(
                                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                                        .specification(windowApp)
                                                                        .addFunction(windowFunction("avg", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                                                LINEITEM_TABLESCAN_DOQRST)))))));
    }

    @Test
    public void testReorderAcrossPartitionFilter()
    {
        @Language("SQL") String sql = "" +
                "SELECT " +
                "  avg_discount_APP, " +
                "  AVG(quantity) OVER(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) avg_quantity_A " +
                "FROM ( " +
                "   SELECT " +
                "     *, " +
                "     AVG(discount) OVER(PARTITION BY suppkey, tax ORDER BY receiptdate ASC NULLS LAST) avg_discount_APP " +
                "   FROM lineitem) " +
                "WHERE suppkey > 0";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowApp)
                                        .addFunction(windowFunction("avg", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowA)
                                                        .addFunction(windowFunction("avg", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                                filter(
                                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "SUPPKEY"), new Constant(BIGINT, 0L)),
                                                        LINEITEM_TABLESCAN_DOQRST))))));
    }

    @Test
    public void testReorderBDAC()
    {
        // This test is to catch the mistake with naive implementation of swapping adjacent windows without recursions, e.g.:
        // sorting of B,D,A,C descending should result in D,C,B,A but when swap is applied to adjacent windows only without recursions, then:
        // B,D is swapped, resulting in D,B,A,C, then B and A are in correct order, lastly A and C is swapped resulting in
        // D,B,C,A instead of D,C,B,A

        // The order of windows by partition key is:
        // 1st - windowE
        // 2nd - windowC
        // 3rd - windowA
        // 4th - windowD

        @Language("SQL") String sql = "select " +
                "avg(discount) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) avg_discount_A, " +
                "sum(tax) over(PARTITION BY quantity ORDER BY receiptdate ASC NULLS LAST) sum_tax_E, " +
                "avg(quantity) over(PARTITION BY tax ORDER BY receiptdate ASC NULLS LAST) avg_quantity_D, " +
                "sum(discount) over(PARTITION BY receiptdate ORDER BY suppkey ASC NULLS LAST) sum_discount_C " +
                "from lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowD)
                                        .addFunction(windowFunction("avg", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowA)
                                                        .addFunction(windowFunction("avg", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                                window(windowMatcherBuilder -> windowMatcherBuilder
                                                                .specification(windowC)
                                                                .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                                        .specification(windowE)
                                                                        .addFunction(windowFunction("sum", ImmutableList.of(TAX_ALIAS), DEFAULT_FRAME)),
                                                                LINEITEM_TABLESCAN_DOQRST)))))));
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PredicatePushDown(
                        getPlanTester().getPlannerContext(),
                        false,
                        false),
                new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getEstimatedExchangesCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new RemoveRedundantIdentityProjections())
                                .add(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                                .add(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(1))
                                .add(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(2))
                                .addAll(columnPruningRules(getPlanTester().getPlannerContext().getMetadata()))
                                .build()));
        assertPlan(sql, pattern, optimizers);
    }
}
