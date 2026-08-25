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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.DeriveTableScanConstraintThroughProject.DeriveTableScanConstraintThroughProjectWithFilter;
import io.trino.sql.planner.iterative.rule.DeriveTableScanConstraintThroughProject.DeriveTableScanConstraintThroughProjectWithoutFilter;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestDeriveTableScanConstraintThroughProject
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());

    private static final Type STATUS_TYPE = createVarcharType(1);
    private static final Type WIDE_STATUS_TYPE = createVarcharType(3);
    private static final Reference X = new Reference(WIDE_STATUS_TYPE, "x");
    private static final Expression X_O_OR_F = new Logical(OR, ImmutableList.of(
            comparison(EQUAL, X, new Constant(WIDE_STATUS_TYPE, utf8Slice("O"))),
            comparison(EQUAL, X, new Constant(WIDE_STATUS_TYPE, utf8Slice("F")))));
    private static final Domain O_OR_F_DOMAIN = Domain.multipleValues(STATUS_TYPE, ImmutableList.of(utf8Slice("O"), utf8Slice("F")));

    private DeriveTableScanConstraintThroughProjectWithoutFilter rule;
    private DeriveTableScanConstraintThroughProjectWithFilter withFilterRule;
    private TableHandle ordersTableHandle;
    private ColumnHandle orderStatusColumn;

    @BeforeAll
    public void setUpBeforeClass()
    {
        rule = new DeriveTableScanConstraintThroughProjectWithoutFilter(tester().getPlannerContext());
        withFilterRule = new DeriveTableScanConstraintThroughProjectWithFilter(tester().getPlannerContext());
        ordersTableHandle = new TableHandle(
                tester().getCurrentCatalogHandle(),
                new TpchTableHandle("sf1", "orders", 1.0),
                TpchTransactionHandle.INSTANCE);
        orderStatusColumn = new TpchColumnHandle("orderstatus", STATUS_TYPE);
    }

    @Test
    public void testDerivesConstraintForNonInlinableConjunct()
    {
        // the OR conjunct references the projected expression twice, so it cannot be inlined
        tester().assertThat(rule)
                .on(p -> p.filter(
                        X_O_OR_F,
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("x", WIDE_STATUS_TYPE), new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))
                                        .build(),
                                p.tableScan(
                                        ordersTableHandle,
                                        ImmutableList.of(p.symbol("orderstatus", STATUS_TYPE)),
                                        ImmutableMap.of(p.symbol("orderstatus", STATUS_TYPE), orderStatusColumn)))))
                .matches(
                        filter(
                                X_O_OR_F,
                                project(
                                        ImmutableMap.of("x", expression(new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))),
                                        constrainedTableScanWithTableLayout(
                                                "orders",
                                                ImmutableMap.of("orderstatus", O_OR_F_DOMAIN),
                                                ImmutableMap.of("orderstatus", "orderstatus")))));
    }

    @Test
    public void testDerivesConstraintThroughResidualFilter()
    {
        // a residual filter (e.g. a view's own WHERE clause) sits between the projection and the scan
        tester().assertThat(withFilterRule)
                .on(p -> p.filter(
                        X_O_OR_F,
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("x", WIDE_STATUS_TYPE), new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))
                                        .build(),
                                p.filter(
                                        notNull(),
                                        p.tableScan(
                                                ordersTableHandle,
                                                ImmutableList.of(p.symbol("orderstatus", STATUS_TYPE)),
                                                ImmutableMap.of(p.symbol("orderstatus", STATUS_TYPE), orderStatusColumn))))))
                .matches(
                        filter(
                                X_O_OR_F,
                                project(
                                        ImmutableMap.of("x", expression(new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))),
                                        filter(
                                                notNull(),
                                                constrainedTableScanWithTableLayout(
                                                        "orders",
                                                        ImmutableMap.of("orderstatus", O_OR_F_DOMAIN),
                                                        ImmutableMap.of("orderstatus", "orderstatus"))))));
    }

    @Test
    public void testReplacesWithValuesWhenDomainIsNone()
    {
        // contradictory conjuncts derive an unsatisfiable domain
        tester().assertThat(rule)
                .on(p -> p.filter(
                        new Logical(AND, ImmutableList.of(
                                new Logical(OR, ImmutableList.of(
                                        comparison(LESS_THAN, X, new Constant(WIDE_STATUS_TYPE, utf8Slice("B"))),
                                        comparison(LESS_THAN, X, new Constant(WIDE_STATUS_TYPE, utf8Slice("A"))))),
                                new Logical(OR, ImmutableList.of(
                                        comparison(GREATER_THAN, X, new Constant(WIDE_STATUS_TYPE, utf8Slice("Y"))),
                                        comparison(GREATER_THAN, X, new Constant(WIDE_STATUS_TYPE, utf8Slice("Z"))))))),
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("x", WIDE_STATUS_TYPE), new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))
                                        .build(),
                                p.tableScan(
                                        ordersTableHandle,
                                        ImmutableList.of(p.symbol("orderstatus", STATUS_TYPE)),
                                        ImmutableMap.of(p.symbol("orderstatus", STATUS_TYPE), orderStatusColumn)))))
                .matches(values("x"));
    }

    @Test
    public void testDoesNotFireForInlinableConjunct()
    {
        // a conjunct referencing the projected expression once is left to PredicatePushDown
        tester().assertThat(rule)
                .on(p -> p.filter(
                        comparison(EQUAL, X, new Constant(WIDE_STATUS_TYPE, utf8Slice("O"))),
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("x", WIDE_STATUS_TYPE), new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))
                                        .build(),
                                p.tableScan(
                                        ordersTableHandle,
                                        ImmutableList.of(p.symbol("orderstatus", STATUS_TYPE)),
                                        ImmutableMap.of(p.symbol("orderstatus", STATUS_TYPE), orderStatusColumn)))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonDeterministicAssignment()
    {
        Reference xDouble = new Reference(DOUBLE, "x");
        tester().assertThat(rule)
                .on(p -> p.filter(
                        new Logical(OR, ImmutableList.of(
                                comparison(EQUAL, xDouble, new Constant(DOUBLE, 1.0)),
                                comparison(EQUAL, xDouble, new Constant(DOUBLE, 2.0)))),
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("x", DOUBLE), new Call(RANDOM, ImmutableList.of()))
                                        .build(),
                                p.tableScan(
                                        ordersTableHandle,
                                        ImmutableList.of(p.symbol("orderstatus", STATUS_TYPE)),
                                        ImmutableMap.of(p.symbol("orderstatus", STATUS_TYPE), orderStatusColumn)))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDynamicFilter()
    {
        tester().assertThat(rule)
                .on(p -> p.filter(
                        createDynamicFilterExpression(tester().getMetadata(), getCharVarcharCoercion(TEST_SESSION), new DynamicFilterId("df"), WIDE_STATUS_TYPE, X),
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("x", WIDE_STATUS_TYPE), new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))
                                        .build(),
                                p.tableScan(
                                        ordersTableHandle,
                                        ImmutableList.of(p.symbol("orderstatus", STATUS_TYPE)),
                                        ImmutableMap.of(p.symbol("orderstatus", STATUS_TYPE), orderStatusColumn)))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenPushdownIntoConnectorsDisabled()
    {
        tester().assertThat(rule)
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .on(p -> p.filter(
                        X_O_OR_F,
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("x", WIDE_STATUS_TYPE), new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))
                                        .build(),
                                p.tableScan(
                                        ordersTableHandle,
                                        ImmutableList.of(p.symbol("orderstatus", STATUS_TYPE)),
                                        ImmutableMap.of(p.symbol("orderstatus", STATUS_TYPE), orderStatusColumn)))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenNoNarrowing()
    {
        // the scan already enforces the derivable domain: a second application must not loop
        tester().assertThat(rule)
                .on(p -> p.filter(
                        X_O_OR_F,
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("x", WIDE_STATUS_TYPE), new Cast(new Reference(STATUS_TYPE, "orderstatus"), WIDE_STATUS_TYPE))
                                        .build(),
                                p.tableScan(
                                        ordersTableHandle,
                                        ImmutableList.of(p.symbol("orderstatus", STATUS_TYPE)),
                                        ImmutableMap.of(p.symbol("orderstatus", STATUS_TYPE), orderStatusColumn),
                                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                                orderStatusColumn, O_OR_F_DOMAIN))))))
                .doesNotFire();
    }

    private static Expression notNull()
    {
        return not(FUNCTIONS.getMetadata(), getCharVarcharCoercion(TEST_SESSION), new IsNull(new Reference(STATUS_TYPE, "orderstatus")));
    }
}
