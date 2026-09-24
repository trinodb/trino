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
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.SecureExpression;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.type.CharVarcharCoercion;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSecureColumns
{
    private static final CharVarcharCoercion CHAR_VARCHAR_COERCION = getCharVarcharCoercion(TEST_SESSION);
    private static final Symbol X = new Symbol(BIGINT, "x");
    private static final Symbol Y = new Symbol(BIGINT, "y");
    private static final Symbol Z = new Symbol(BIGINT, "z");
    private static final Symbol X_RENAMED = new Symbol(BIGINT, "x_renamed");
    private static final Symbol Y_MASKED = new Symbol(BIGINT, "y_masked");
    private static final ColumnHandle X_COLUMN = new TestingColumnHandle("x");
    private static final ColumnHandle Y_COLUMN = new TestingColumnHandle("y");

    @Test
    public void testSymbolsOfExpression()
    {
        Expression secure = new SecureExpression(Logical.and(equal(X, 1), greaterThan(Y, 2)));

        assertThat(SecureColumns.symbols(Logical.and(secure, equal(Z, 3)))).containsExactlyInAnyOrder(X, Y);
        assertThat(SecureColumns.symbols(equal(Z, 3))).isEmpty();
    }

    @Test
    public void testSymbolsOfPlanFollowProjections()
    {
        PlanBuilder planBuilder = planBuilder();
        TableScanNode scan = scan(planBuilder);
        // the secure predicate references the projected name, which must map back to the scan's own symbol
        PlanNode filter = planBuilder.filter(
                new SecureExpression(equal(X_RENAMED, 1)),
                planBuilder.project(Assignments.of(X_RENAMED, X.toSymbolReference(), Y, Y.toSymbolReference()), scan));

        assertThat(SecureColumns.symbols(filter)).containsExactlyInAnyOrder(X_RENAMED, X);
    }

    @Test
    public void testSymbolsOfPlanIncludeMaskOutputsAndDerivedSymbols()
    {
        PlanBuilder planBuilder = planBuilder();
        // y is replaced by a secure mask; z is computed from the masked value further up
        PlanNode masked = planBuilder.project(
                Assignments.of(X, X.toSymbolReference(), Y_MASKED, new SecureExpression(Y.toSymbolReference())),
                scan(planBuilder));
        PlanNode root = planBuilder.project(Assignments.of(Z, Y_MASKED.toSymbolReference()), masked);

        assertThat(SecureColumns.symbols(root)).containsExactlyInAnyOrder(Y_MASKED, Y, Z);
    }

    @Test
    public void testDerivedProjectionDoesNotMarkIndependentInputSecure()
    {
        PlanBuilder planBuilder = planBuilder();
        PlanNode masked = planBuilder.project(
                Assignments.of(X, X.toSymbolReference(), Y_MASKED, new SecureExpression(Y.toSymbolReference())),
                scan(planBuilder));
        PlanNode root = planBuilder.project(Assignments.of(Z, new Coalesce(X.toSymbolReference(), Y_MASKED.toSymbolReference())), masked);

        assertThat(SecureColumns.symbols(root)).containsExactlyInAnyOrder(Y, Y_MASKED, Z);
    }

    @Test
    public void testSymbolsOfPlanStopAtGroupReferences()
    {
        PlanBuilder planBuilder = planBuilder();
        // unresolved memo nodes are printed while debugging the optimizer and must not fail the walk
        PlanNode filter = planBuilder.filter(
                new SecureExpression(equal(X, 1)),
                new GroupReference(new PlanNodeId("group"), 1, ImmutableList.of(X, Y)));

        assertThat(SecureColumns.symbols(filter)).containsExactly(X);
    }

    @Test
    public void testToPredicate()
    {
        DomainTranslator domainTranslator = new DomainTranslator(PLANNER_CONTEXT.getMetadata());
        TupleDomain<Symbol> domain = TupleDomain.withColumnDomains(ImmutableMap.of(
                X, singleValue(BIGINT, 1L),
                Y, singleValue(BIGINT, 2L)));

        assertThat(extractConjuncts(SecureColumns.toPredicate(domainTranslator, CHAR_VARCHAR_COERCION, domain, ImmutableSet.of())))
                .containsExactlyInAnyOrder(equal(X, 1), equal(Y, 2));
        assertThat(extractConjuncts(SecureColumns.toPredicate(domainTranslator, CHAR_VARCHAR_COERCION, domain, ImmutableSet.of(Z))))
                .containsExactlyInAnyOrder(equal(X, 1), equal(Y, 2));
        // the domain of a secure column stays enforceable but is not rebuilt in clear text
        assertThat(extractConjuncts(SecureColumns.toPredicate(domainTranslator, CHAR_VARCHAR_COERCION, domain, ImmutableSet.of(X))))
                .containsExactlyInAnyOrder(equal(Y, 2), new SecureExpression(equal(X, 1)));
        assertThat(SecureColumns.toPredicate(domainTranslator, CHAR_VARCHAR_COERCION, TupleDomain.all(), ImmutableSet.of(X))).isEqualTo(TRUE);
    }

    @Test
    public void testWithoutValueRanges()
    {
        PlanNodeId scan = new PlanNodeId("scan");
        StatsAndCosts statsAndCosts = new StatsAndCosts(
                ImmutableMap.of(scan, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(X, SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(2)
                                .setDistinctValuesCount(2)
                                .setNullsFraction(0)
                                .setAverageRowSize(8)
                                .build())
                        .build()),
                ImmutableMap.of(scan, new PlanCostEstimate(1, 3, 2, 4)));

        StatsAndCosts redacted = SecureColumns.withoutValueRanges(statsAndCosts);

        PlanNodeStatsEstimate estimate = redacted.getStats().get(scan);
        assertThat(estimate.getOutputRowCount()).isEqualTo(10);
        SymbolStatsEstimate statistics = estimate.getSymbolStatistics(X);
        assertThat(statistics.getLowValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(statistics.getHighValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(statistics.getDistinctValuesCount()).isEqualTo(2);
        assertThat(statistics.getNullsFraction()).isEqualTo(0);
        assertThat(statistics.getAverageRowSize()).isEqualTo(8);
        assertThat(redacted.getCosts()).isEqualTo(statsAndCosts.getCosts());
    }

    @Test
    public void testRedact()
    {
        TableScanNode scan = scan(planBuilder());
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(ImmutableMap.of(
                X_COLUMN, singleValue(BIGINT, 1L),
                Y_COLUMN, singleValue(BIGINT, 2L)));

        assertThat(SecureColumns.redact(domain, scan, ImmutableSet.of(X)))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(Y_COLUMN, singleValue(BIGINT, 2L))));
        assertThat(SecureColumns.redact(domain, scan, ImmutableSet.of(X, Y))).isEqualTo(TupleDomain.all());
        assertThat(SecureColumns.redact(domain, scan, ImmutableSet.of(Z))).isSameAs(domain);
        assertThat(SecureColumns.redact(TupleDomain.none(), scan, ImmutableSet.of(X))).isEqualTo(TupleDomain.none());
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
