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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.SensitiveExpression;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSensitiveSymbols
{
    private static final Symbol X = new Symbol(BIGINT, "x");
    private static final Symbol Y = new Symbol(BIGINT, "y");
    private static final Symbol Z = new Symbol(BIGINT, "z");
    private static final Symbol X_RENAMED = new Symbol(BIGINT, "x_renamed");
    private static final Symbol Y_MASKED = new Symbol(BIGINT, "y_masked");
    private static final ColumnHandle X_COLUMN = new TestingColumnHandle("x");
    private static final ColumnHandle Y_COLUMN = new TestingColumnHandle("y");

    @Test
    public void testReferencedIn()
    {
        Expression sensitive = new SensitiveExpression(Logical.and(equal(X, 1), greaterThan(Y, 2)));

        assertThat(SensitiveSymbols.referencedIn(Logical.and(sensitive, equal(Z, 3)))).containsExactlyInAnyOrder(X, Y);
        assertThat(SensitiveSymbols.referencedIn(equal(Z, 3))).isEmpty();
    }

    @Test
    public void testInPlanFollowsProjectedCopies()
    {
        PlanBuilder planBuilder = planBuilder();
        TableScanNode scan = scan(planBuilder);
        // the sensitive predicate references the projected name, which must map back to the scan's own symbol
        PlanNode filter = planBuilder.filter(
                new SensitiveExpression(equal(X_RENAMED, 1)),
                planBuilder.project(Assignments.of(X_RENAMED, X.toSymbolReference(), Y, Y.toSymbolReference()), scan));

        assertThat(SensitiveSymbols.inPlan(filter)).containsExactlyInAnyOrder(X_RENAMED, X);
    }

    @Test
    public void testInPlanIncludesMaskOutputsAndComputedSymbols()
    {
        PlanBuilder planBuilder = planBuilder();
        // y is replaced by a sensitive mask; z is computed from the masked value further up
        PlanNode masked = planBuilder.project(
                Assignments.of(X, X.toSymbolReference(), Y_MASKED, new SensitiveExpression(Y.toSymbolReference())),
                scan(planBuilder));
        PlanNode root = planBuilder.project(Assignments.of(Z, Y_MASKED.toSymbolReference()), masked);

        assertThat(SensitiveSymbols.inPlan(root)).containsExactlyInAnyOrder(Y_MASKED, Y, Z);
    }

    @Test
    public void testComputedSymbolDoesNotMarkItsInputs()
    {
        PlanBuilder planBuilder = planBuilder();
        PlanNode masked = planBuilder.project(
                Assignments.of(X, X.toSymbolReference(), Y_MASKED, new SensitiveExpression(Y.toSymbolReference())),
                scan(planBuilder));
        PlanNode root = planBuilder.project(Assignments.of(Z, new Coalesce(X.toSymbolReference(), Y_MASKED.toSymbolReference())), masked);

        assertThat(SensitiveSymbols.inPlan(root)).containsExactlyInAnyOrder(Y, Y_MASKED, Z);
    }

    @Test
    public void testSetOperationCopiesSymbols()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol other = new Symbol(BIGINT, "other");
        PlanNode filtered = planBuilder.filter(new SensitiveExpression(equal(X, 1)), scan(planBuilder));
        PlanNode union = planBuilder.union(ImmutableListMultimap.of(Z, X, Z, other), ImmutableList.of(filtered, planBuilder.values(other)));
        // the output carries the sensitive input's values, but the other input is not constrained
        assertThat(SensitiveSymbols.inPlan(union)).containsExactlyInAnyOrder(X, Z);

        union = planBuilder.union(ImmutableListMultimap.of(Z, X, Z, other), ImmutableList.of(scan(planBuilder), planBuilder.values(other)));
        // a constraint on the output constrains every input
        assertThat(SensitiveSymbols.inPlan(planBuilder.filter(new SensitiveExpression(equal(Z, 1)), union))).containsExactlyInAnyOrder(Z, X, other);
    }

    @Test
    public void testExchangeCopiesSymbols()
    {
        PlanBuilder planBuilder = planBuilder();
        PlanNode exchange = planBuilder.exchange(builder -> builder
                .singleDistributionPartitioningScheme(Z)
                .addInputsSet(X)
                .addSource(scan(planBuilder)));

        assertThat(SensitiveSymbols.inPlan(planBuilder.filter(new SensitiveExpression(equal(Z, 1)), exchange))).containsExactlyInAnyOrder(Z, X);
    }

    @Test
    public void testGroupIdCopiesGroupingColumns()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol groupId = new Symbol(BIGINT, "group_id");
        PlanNode groupIdNode = planBuilder.groupId(ImmutableList.of(ImmutableList.of(Z)), ImmutableMap.of(Z, X), ImmutableList.of(Y), groupId, scan(planBuilder));

        assertThat(SensitiveSymbols.inPlan(planBuilder.filter(new SensitiveExpression(equal(Z, 1)), groupIdNode))).containsExactlyInAnyOrder(Z, X);
    }

    @Test
    public void testUnnestComputesUnnestedValues()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol array = new Symbol(new ArrayType(BIGINT), "array");
        PlanNode masked = planBuilder.project(Assignments.of(array, new SensitiveExpression(new Constant(new ArrayType(BIGINT), null))), planBuilder.values(X));
        PlanNode unnest = planBuilder.unnest(ImmutableList.of(), ImmutableList.of(new UnnestNode.Mapping(array, ImmutableList.of(Z))), masked);

        assertThat(SensitiveSymbols.inPlan(unnest)).containsExactlyInAnyOrder(array, Z);
    }

    @Test
    public void testValuesMarkSensitiveItems()
    {
        PlanBuilder planBuilder = planBuilder();
        PlanNode values = planBuilder.values(ImmutableList.of(X, Y), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L), new SensitiveExpression(new Constant(BIGINT, 2L)))));

        assertThat(SensitiveSymbols.inPlan(values)).containsExactly(Y);
    }

    @Test
    public void testOtherNodesComputeTheirNewOutputsFromTheirSources()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol marker = new Symbol(BOOLEAN, "marker");
        PlanNode filter = planBuilder.filter(new SensitiveExpression(equal(X, 1)), scan(planBuilder));

        assertThat(SensitiveSymbols.inPlan(planBuilder.markDistinct(marker, ImmutableList.of(Y), filter))).containsExactlyInAnyOrder(X, marker);
    }

    @Test
    public void testInPlanStopsAtGroupReferences()
    {
        PlanBuilder planBuilder = planBuilder();
        // unresolved memo nodes are printed while debugging the optimizer and must not fail the walk
        PlanNode filter = planBuilder.filter(
                new SensitiveExpression(equal(X, 1)),
                new GroupReference(new PlanNodeId("group"), 1, ImmutableList.of(X, Y)));

        assertThat(SensitiveSymbols.inPlan(filter)).containsExactly(X);
    }

    private static PlanBuilder planBuilder()
    {
        return new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
    }

    private static TableScanNode scan(PlanBuilder planBuilder)
    {
        return planBuilder.tableScan(ImmutableList.of(X, Y), ImmutableMap.of(X, X_COLUMN, Y, Y_COLUMN));
    }

    private static Expression equal(Symbol symbol, long value)
    {
        return comparison(EQUAL, symbol.toSymbolReference(), new Constant(BIGINT, value));
    }

    private static Expression greaterThan(Symbol symbol, long value)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), new Constant(BIGINT, value));
    }
}
