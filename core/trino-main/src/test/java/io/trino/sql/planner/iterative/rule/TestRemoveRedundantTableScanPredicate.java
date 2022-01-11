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
import io.trino.connector.CatalogName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTableLayoutHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MODULUS;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
import static org.testng.Assert.assertTrue;

public class TestRemoveRedundantTableScanPredicate
        extends BaseRuleTest
{
    private RemoveRedundantTableScanPredicate removeRedundantTableScanPredicate;
    private TableHandle nationTableHandle;
    private TableHandle ordersTableHandle;

    @BeforeClass
    public void setUpBeforeClass()
    {
        removeRedundantTableScanPredicate = new RemoveRedundantTableScanPredicate(tester().getPlannerContext(), tester().getTypeAnalyzer());
        CatalogName catalogName = tester().getCurrentConnectorId();
        TpchTableHandle nation = new TpchTableHandle("sf1", "nation", 1.0);
        nationTableHandle = new TableHandle(
                catalogName,
                nation,
                TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(nation, TupleDomain.all())));

        TpchTableHandle orders = new TpchTableHandle("sf1", "orders", 1.0);
        ordersTableHandle = new TableHandle(
                catalogName,
                orders,
                TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(orders, TupleDomain.all())));
    }

    @Test
    public void doesNotFireIfNoTableScan()
    {
        tester().assertThat(removeRedundantTableScanPredicate)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void consumesDeterministicPredicateIfNewDomainIsSame()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(removeRedundantTableScanPredicate)
                .on(p -> p.filter(expression("nationkey = BIGINT '44'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.fromFixedValues(ImmutableMap.of(
                                        columnHandle, NullableValue.of(BIGINT, (long) 44))))))
                .matches(constrainedTableScanWithTableLayout(
                        "nation",
                        ImmutableMap.of("nationkey", singleValue(BIGINT, (long) 44)),
                        ImmutableMap.of("nationkey", "nationkey")));
    }

    @Test
    public void consumesDeterministicPredicateIfNewDomainIsWider()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(removeRedundantTableScanPredicate)
                .on(p -> p.filter(expression("nationkey = BIGINT '44' OR nationkey = BIGINT '45'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.fromFixedValues(ImmutableMap.of(
                                        columnHandle, NullableValue.of(BIGINT, (long) 44))))))
                .matches(constrainedTableScanWithTableLayout(
                        "nation",
                        ImmutableMap.of("nationkey", singleValue(BIGINT, (long) 44)),
                        ImmutableMap.of("nationkey", "nationkey")));
    }

    @Test
    public void consumesDeterministicPredicateIfNewDomainIsNarrower()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(removeRedundantTableScanPredicate)
                .on(p -> p.filter(expression("nationkey = BIGINT '44' OR nationkey = BIGINT '45' OR nationkey = BIGINT '47'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.multipleValues(BIGINT, ImmutableList.of(44L, 45L, 46L)))))))
                .matches(
                        filter(
                                expression("nationkey IN (BIGINT '44', BIGINT '45')"),
                                constrainedTableScanWithTableLayout(
                                        "nation",
                                        ImmutableMap.of("nationkey", Domain.multipleValues(BIGINT, ImmutableList.of(44L, 45L, 46L))),
                                        ImmutableMap.of("nationkey", "nationkey"))));
    }

    @Test
    public void doesNotConsumeRemainingPredicateIfNewDomainIsWider()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(removeRedundantTableScanPredicate)
                .on(p -> p.filter(
                        new LogicalExpression(
                                AND,
                                ImmutableList.of(
                                        new ComparisonExpression(
                                                EQUAL,
                                                FunctionCallBuilder.resolve(tester().getSession(), tester().getMetadata())
                                                        .setName(QualifiedName.of("rand"))
                                                        .build(),
                                                new GenericLiteral("BIGINT", "42")),
                                        new ComparisonExpression(
                                                EQUAL,
                                                new ArithmeticBinaryExpression(
                                                        MODULUS,
                                                        new SymbolReference("nationkey"),
                                                        new GenericLiteral("BIGINT", "17")),
                                                new GenericLiteral("BIGINT", "44")),
                                        LogicalExpression.or(
                                                new ComparisonExpression(
                                                        EQUAL,
                                                        new SymbolReference("nationkey"),
                                                        new GenericLiteral("BIGINT", "44")),
                                                new ComparisonExpression(
                                                        EQUAL,
                                                        new SymbolReference("nationkey"),
                                                        new GenericLiteral("BIGINT", "45"))))),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.fromFixedValues(ImmutableMap.of(
                                        columnHandle, NullableValue.of(BIGINT, (long) 44))))))
                .matches(
                        filter(
                                LogicalExpression.and(
                                        new ComparisonExpression(
                                                EQUAL,
                                                FunctionCallBuilder.resolve(tester().getSession(), tester().getMetadata())
                                                        .setName(QualifiedName.of("rand"))
                                                        .build(),
                                                new GenericLiteral("BIGINT", "42")),
                                        new ComparisonExpression(
                                                EQUAL,
                                                new ArithmeticBinaryExpression(
                                                        MODULUS,
                                                        new SymbolReference("nationkey"),
                                                        new GenericLiteral("BIGINT", "17")),
                                                new GenericLiteral("BIGINT", "44"))),
                                constrainedTableScanWithTableLayout(
                                        "nation",
                                        ImmutableMap.of("nationkey", singleValue(BIGINT, (long) 44)),
                                        ImmutableMap.of("nationkey", "nationkey"))));
    }

    @Test
    public void doesNotFireOnNonDeterministicPredicate()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(removeRedundantTableScanPredicate)
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                FunctionCallBuilder.resolve(tester().getSession(), tester().getMetadata())
                                        .setName(QualifiedName.of("rand"))
                                        .build(),
                                new GenericLiteral("BIGINT", "42")),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.all())))
                .doesNotFire();
    }

    @Test
    public void doesNotFireIfRuleNotChangePlan()
    {
        tester().assertThat(removeRedundantTableScanPredicate)
                .on(p -> p.filter(expression("nationkey % 17 = BIGINT '44' AND nationkey % 15 = BIGINT '43'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                TupleDomain.all())))
                .doesNotFire();
    }

    @Test
    public void doesNotAddTableLayoutToFilterTableScan()
    {
        tester().assertThat(removeRedundantTableScanPredicate)
                .on(p -> p.filter(expression("orderstatus = 'F'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))))))
                .doesNotFire();
    }

    @Test
    public void testLogicalEquals()
    {
        Expression expression1 = new LogicalExpression(
                AND,
                ImmutableList.of(
                        new LogicalExpression(
                                OR,
                                ImmutableList.of(
                                        new ComparisonExpression(
                                                GREATER_THAN,
                                                new StringLiteral("uid"),
                                                new LongLiteral("0")),
                                        new ComparisonExpression(
                                                GREATER_THAN,
                                                new StringLiteral("uid"),
                                                new LongLiteral("3")))),
                        new LogicalExpression(
                                OR,
                                ImmutableList.of(
                                        new ComparisonExpression(
                                                GREATER_THAN,
                                                new StringLiteral("uid"),
                                                new LongLiteral("1")),
                                        new ComparisonExpression(
                                                LESS_THAN,
                                                new StringLiteral("uid"),
                                                new LongLiteral("3"))))));
        Expression expression2 = new LogicalExpression(
                AND,
                ImmutableList.of(
                        new LogicalExpression(
                                OR,
                                ImmutableList.of(
                                        new ComparisonExpression(
                                                GREATER_THAN,
                                                new StringLiteral("uid"),
                                                new LongLiteral("3")),
                                        new ComparisonExpression(
                                                GREATER_THAN,
                                                new StringLiteral("uid"),
                                                new LongLiteral("0")))),
                        new LogicalExpression(
                                OR,
                                ImmutableList.of(
                                        new ComparisonExpression(
                                                LESS_THAN,
                                                new StringLiteral("uid"),
                                                new LongLiteral("3")),
                                        new ComparisonExpression(
                                                GREATER_THAN,
                                                new StringLiteral("uid"),
                                                new LongLiteral("1"))))));
        RemoveRedundantTableScanPredicate removeRedundantTableScanPredicate =
                RemoveRedundantTableScanPredicate.createRemoveRedundantTableScanPredicateForTest();
        assertTrue(
                removeRedundantTableScanPredicate.logicalEquals(expression1, expression2));
    }
}
