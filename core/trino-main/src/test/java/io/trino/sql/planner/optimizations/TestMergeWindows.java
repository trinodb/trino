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
import io.trino.sql.planner.plan.FrameBoundType;
import io.trino.sql.planner.plan.WindowFrameType;
import io.trino.sql.planner.plan.WindowNode;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestMergeWindows
        extends BasePlanTest
{
    private static final String SUPPKEY_ALIAS = "SUPPKEY";
    private static final String ORDERKEY_ALIAS = "ORDERKEY";
    private static final String SHIPDATE_ALIAS = "SHIPDATE";
    private static final String QUANTITY_ALIAS = "QUANTITY";
    private static final String DISCOUNT_ALIAS = "DISCOUNT";
    private static final String EXTENDEDPRICE_ALIAS = "EXTENDEDPRICE";

    private static final PlanMatchPattern LINEITEM_TABLESCAN_DOQSS = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity",
                    DISCOUNT_ALIAS, "discount",
                    SUPPKEY_ALIAS, "suppkey",
                    ORDERKEY_ALIAS, "orderkey",
                    SHIPDATE_ALIAS, "shipdate"));

    private static final PlanMatchPattern LINEITEM_TABLESCAN_DOQS = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity",
                    DISCOUNT_ALIAS, "discount",
                    SUPPKEY_ALIAS, "suppkey",
                    ORDERKEY_ALIAS, "orderkey"));

    private static final PlanMatchPattern LINEITEM_TABLESCAN_DEOQS = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity",
                    SUPPKEY_ALIAS, "suppkey",
                    ORDERKEY_ALIAS, "orderkey",
                    DISCOUNT_ALIAS, "discount",
                    EXTENDEDPRICE_ALIAS, "extendedprice"));

    private static final WindowNode.Frame COMMON_FRAME = new WindowNode.Frame(
            WindowFrameType.ROWS,
            FrameBoundType.UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            FrameBoundType.CURRENT_ROW,
            Optional.empty(),
            Optional.empty());

    private final ExpectedValueProvider<DataOrganizationSpecification> specificationA;
    private final ExpectedValueProvider<DataOrganizationSpecification> specificationB;

    public TestMergeWindows()
    {
        super(ImmutableMap.of());

        specificationA = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        specificationB = specification(
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableList.of(SHIPDATE_ALIAS),
                ImmutableMap.of(SHIPDATE_ALIAS, SortOrder.ASC_NULLS_LAST));
    }

    /**
     * There are two types of tests in here, and they answer two different
     * questions about MergeWindows (MW):
     * <p>
     * 1) Is MW working as it's supposed to be? The tests running the minimal
     * set of optimizers can tell us this.
     * 2) Has some other optimizer changed the plan in such a way that MW no
     * longer merges windows with identical specifications because the plan
     * that MW sees cannot be optimized by MW? The test running the full set
     * of optimizers answers this, though it isn't actually meaningful unless
     * we know the answer to question 1 is "yes".
     * <p>
     * The tests that use only the minimal set of optimizers are closer to true
     * "unit" tests in that they verify the behavior of MW with as few
     * external dependencies as possible. Those dependencies to include the
     * parser and analyzer, so the phrase "unit" tests should be taken with a
     * grain of salt. Using the parser and anayzler instead of creating plan
     * nodes by hand does have a couple of advantages over a true unit test:
     * 1) The tests are more self-maintaining.
     * 2) They're a lot easier to read.
     * 3) It's a lot less typing.
     * <p>
     * The test that runs with all of the optimzers acts as an integration test
     * and ensures that MW is effective when run with the complete set of
     * optimizers.
     */
    @Test
    public void testMergeableWindowsAllOptimizers()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        PlanMatchPattern pattern =
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME))
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                anyTree(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationB)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                                LINEITEM_TABLESCAN_DOQSS))));  // should be anyTree(LINEITEM_TABLESCAN_DOQSS) but anyTree does not handle zero nodes case correctly

        assertPlan(sql, pattern);
    }

    @Test
    public void testIdenticalWindowSpecificationsABA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME))
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationB)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                                LINEITEM_TABLESCAN_DOQSS)))));
    }

    @Test
    public void testIdenticalWindowSpecificationsABcpA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "NTH_VALUE(quantity, 1) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME))
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationB)
                                                        .addFunction(windowFunction("nth_value", ImmutableList.of(QUANTITY_ALIAS, "ONE"), COMMON_FRAME)),
                                                project(
                                                        ImmutableMap.of("ONE", expression(new Cast(new Reference(INTEGER, "expr"), BIGINT))),
                                                        project(
                                                                ImmutableMap.of("expr", expression(new Constant(INTEGER, 1L))),
                                                                LINEITEM_TABLESCAN_DOQSS)))))));
    }

    @Test
    public void testIdenticalWindowSpecificationsABfilterA()
    {
        // This test makes sure that we don't merge window nodes across filter nodes, which obviously would affect
        // the result of the window function by removing some input values.
        @Language("SQL") String sql = "" +
                "SELECT" +
                "  sum_discount_A, " +
                "  SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "  SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B " +
                "FROM (" +
                "  SELECT" +
                "    *, " +
                "    SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "  FROM lineitem)" +
                "WHERE shipdate IS NOT NULL";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationB)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                                filter(not(getPlanTester().getPlannerContext().getMetadata(), new IsNull(new Reference(VARCHAR, SHIPDATE_ALIAS))),
                                                        project(
                                                                window(windowMatcherBuilder -> windowMatcherBuilder
                                                                                .specification(specificationA)
                                                                                .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                                                        LINEITEM_TABLESCAN_DOQSS))))))));
    }

    @Test
    public void testIdenticalWindowSpecificationsAAcpA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "NTH_VALUE(quantity, 1) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_quantity_A, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME))
                                        .addFunction(windowFunction("nth_value", ImmutableList.of(QUANTITY_ALIAS, "ONE"), COMMON_FRAME))
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                project(ImmutableMap.of("ONE", expression(new Cast(new Reference(INTEGER, "expr"), BIGINT))),
                                        project(ImmutableMap.of("expr", expression(new Constant(INTEGER, 1L))),
                                                LINEITEM_TABLESCAN_DOQS)))));
    }

    @Test
    public void testIdenticalWindowSpecificationsAAfilterA()
    {
        @Language("SQL") String sql = "" +
                "SELECT" +
                "  sum_discount_A, " +
                "  SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "  AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) avg_quantity_A " +
                "FROM (" +
                "  SELECT" +
                "    *, " +
                "    SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "  FROM lineitem)" +
                "WHERE shipdate IS NOT NULL";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME))
                                        .addFunction(windowFunction("avg", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                project(
                                        filter(not(getPlanTester().getPlannerContext().getMetadata(), new IsNull(new Reference(VARCHAR, SHIPDATE_ALIAS))),
                                                project(
                                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                                        .specification(specificationA)
                                                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                                                LINEITEM_TABLESCAN_DOQSS)))))));
    }

    @Test
    public void testIdenticalWindowSpecificationsDefaultFrame()
    {
        ExpectedValueProvider<DataOrganizationSpecification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        ExpectedValueProvider<DataOrganizationSpecification> specificationD = specification(
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableList.of(SHIPDATE_ALIAS),
                ImmutableMap.of(SHIPDATE_ALIAS, SortOrder.ASC_NULLS_LAST));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION By suppkey ORDER BY orderkey), " +
                "SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate), " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey) " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationC)
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME))
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationD)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                                LINEITEM_TABLESCAN_DOQSS)))));
    }

    @Test
    public void testMergeDifferentFrames()
    {
        WindowNode.Frame frameC = new WindowNode.Frame(
                WindowFrameType.ROWS,
                FrameBoundType.UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                FrameBoundType.CURRENT_ROW,
                Optional.empty(),
                Optional.empty());

        ExpectedValueProvider<DataOrganizationSpecification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        WindowNode.Frame frameD = new WindowNode.Frame(
                WindowFrameType.ROWS,
                FrameBoundType.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                FrameBoundType.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty());

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) avg_quantity_D, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_C " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationC)
                                        .addFunction(windowFunction("avg", ImmutableList.of(QUANTITY_ALIAS), frameD))
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), frameC))
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), frameC)),
                                LINEITEM_TABLESCAN_DOQS)));
    }

    @Test
    public void testMergeDifferentFramesWithDefault()
    {
        WindowNode.Frame frameD = new WindowNode.Frame(
                WindowFrameType.ROWS,
                FrameBoundType.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                FrameBoundType.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty());

        ExpectedValueProvider<DataOrganizationSpecification> specificationD = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey) sum_quantity_C, " +
                "AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) avg_quantity_D, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey) sum_discount_C " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationD)
                                        .addFunction(windowFunction("avg", ImmutableList.of(QUANTITY_ALIAS), frameD))
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), DEFAULT_FRAME))
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), DEFAULT_FRAME)),
                                LINEITEM_TABLESCAN_DOQS)));
    }

    @Test
    public void testNotMergeAcrossJoinBranches()
    {
        String rOrderkeyAlias = "R_ORDERKEY";
        String rShipdateAlias = "R_SHIPDATE";
        String rQuantityAlias = "R_QUANTITY";

        @Language("SQL") String sql = "WITH foo AS (" +
                "SELECT " +
                "suppkey, orderkey, partkey, " +
                "SUM(discount) OVER (PARTITION BY orderkey ORDER BY shipdate, quantity DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a " +
                "FROM lineitem WHERE (partkey = 272 OR partkey = 273) AND suppkey > 50 " +
                "), " +
                "bar AS ( " +
                "SELECT " +
                "suppkey, orderkey, partkey, " +
                "AVG(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate, quantity DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) b " +
                "FROM lineitem WHERE (partkey = 272 OR partkey = 273) AND suppkey > 50 " +
                ")" +
                "SELECT * FROM foo, bar WHERE foo.a = bar.b";

        ExpectedValueProvider<DataOrganizationSpecification> leftSpecification = specification(
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableList.of(SHIPDATE_ALIAS, QUANTITY_ALIAS),
                ImmutableMap.of(SHIPDATE_ALIAS, SortOrder.ASC_NULLS_LAST, QUANTITY_ALIAS, SortOrder.DESC_NULLS_LAST));

        ExpectedValueProvider<DataOrganizationSpecification> rightSpecification = specification(
                ImmutableList.of(rOrderkeyAlias),
                ImmutableList.of(rShipdateAlias, rQuantityAlias),
                ImmutableMap.of(rShipdateAlias, SortOrder.ASC_NULLS_LAST, rQuantityAlias, SortOrder.DESC_NULLS_LAST));

        // Too many items in the map to call ImmutableMap.of() :-(
        ImmutableMap.Builder<String, String> leftTableScanBuilder = ImmutableMap.builder();
        leftTableScanBuilder.put(DISCOUNT_ALIAS, "discount");
        leftTableScanBuilder.put(ORDERKEY_ALIAS, "orderkey");
        leftTableScanBuilder.put("PARTKEY", "partkey");
        leftTableScanBuilder.put(SUPPKEY_ALIAS, "suppkey");
        leftTableScanBuilder.put(QUANTITY_ALIAS, "quantity");
        leftTableScanBuilder.put(SHIPDATE_ALIAS, "shipdate");

        PlanMatchPattern leftTableScan = tableScan("lineitem", leftTableScanBuilder.buildOrThrow());

        PlanMatchPattern rightTableScan = tableScan(
                "lineitem",
                ImmutableMap.of(
                        rOrderkeyAlias, "orderkey",
                        "R_PARTKEY", "partkey",
                        "R_SUPPKEY", "suppkey",
                        rQuantityAlias, "quantity",
                        rShipdateAlias, "shipdate"));

        assertUnitPlan(sql,
                anyTree(
                        filter(new Comparison(EQUAL, new Reference(BIGINT, "SUM"), new Reference(BIGINT, "AVG")),
                                join(INNER, builder -> builder
                                        .left(
                                                any(
                                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                                        .specification(leftSpecification)
                                                                        .addFunction("SUM", windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                                                any(leftTableScan))))
                                        .right(
                                                any(
                                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                                        .specification(rightSpecification)
                                                                        .addFunction("AVG", windowFunction("avg", ImmutableList.of(rQuantityAlias), COMMON_FRAME)),
                                                                any(rightTableScan))))))));
    }

    @Test
    public void testNotMergeDifferentPartition()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) over (PARTITION BY quantity ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C " +
                "FROM lineitem";

        ExpectedValueProvider<DataOrganizationSpecification> specificationC = specification(
                ImmutableList.of(QUANTITY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationC)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                                LINEITEM_TABLESCAN_DOQS)))));
    }

    @Test
    public void testNotMergeDifferentOrderBy()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY quantity ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C " +
                "FROM lineitem";

        ExpectedValueProvider<DataOrganizationSpecification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(QUANTITY_ALIAS),
                ImmutableMap.of(QUANTITY_ALIAS, SortOrder.ASC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationC)
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationA)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                                LINEITEM_TABLESCAN_DOQS)))));
    }

    @Test
    public void testNotMergeDifferentOrdering()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) over (PARTITION BY suppkey ORDER BY orderkey DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "SUM(discount) over (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        ExpectedValueProvider<DataOrganizationSpecification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.DESC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationC)
                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationA)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(EXTENDEDPRICE_ALIAS), COMMON_FRAME))
                                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                                LINEITEM_TABLESCAN_DEOQS)))));
    }

    @Test
    public void testNotMergeDifferentNullOrdering()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        ExpectedValueProvider<DataOrganizationSpecification> specificationC = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_FIRST));

        assertUnitPlan(sql,
                anyTree(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction("sum", ImmutableList.of(EXTENDEDPRICE_ALIAS), COMMON_FRAME))
                                        .addFunction(windowFunction("sum", ImmutableList.of(DISCOUNT_ALIAS), COMMON_FRAME)),
                                project(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specificationC)
                                                        .addFunction(windowFunction("sum", ImmutableList.of(QUANTITY_ALIAS), COMMON_FRAME)),
                                                LINEITEM_TABLESCAN_DEOQS)))));
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getEstimatedExchangesCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .addAll(columnPruningRules(getPlanTester().getPlannerContext().getMetadata()))
                                .build()));
        assertPlan(sql, pattern, optimizers);
    }
}
