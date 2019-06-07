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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.plugin.tpch.TpchTableLayoutHandle;
import io.prestosql.plugin.tpch.TpchTransactionHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPushPredicateIntoTableScan
        extends BaseRuleTest
{
    private PushPredicateIntoTableScan pushPredicateIntoTableScan;
    private TableHandle nationTableHandle;
    private TableHandle ordersTableHandle;
    private CatalogName catalogName;

    @BeforeClass
    public void setUpBeforeClass()
    {
        pushPredicateIntoTableScan = new PushPredicateIntoTableScan(tester().getMetadata(), new TypeAnalyzer(new SqlParser(), tester().getMetadata()));

        catalogName = tester().getCurrentConnectorId();

        TpchTableHandle nation = new TpchTableHandle("nation", 1.0);
        nationTableHandle = new TableHandle(
                catalogName,
                nation,
                TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(nation, TupleDomain.all())));

        TpchTableHandle orders = new TpchTableHandle("orders", 1.0);
        ordersTableHandle = new TableHandle(
                catalogName,
                orders,
                TpchTransactionHandle.INSTANCE,
                Optional.of(new TpchTableLayoutHandle(orders, TupleDomain.all())));
    }

    @Test
    public void doesNotFireIfNoTableScan()
    {
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void eliminateTableScanWhenNoLayoutExist()
    {
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("orderstatus = 'G'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))))))
                .matches(values("A"));
    }

    @Test
    public void replaceWithExistsWhenNoLayoutExist()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("nationkey = BIGINT '44'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.none())))
                .matches(values("A"));
    }

    @Test
    public void doesNotFireIfRuleNotChangePlan()
    {
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("nationkey % 17 =  BIGINT '44' AND nationkey % 15 =  BIGINT '43'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                TupleDomain.all())))
                .doesNotFire();
    }

    @Test
    public void ruleAddedTableLayoutToFilterTableScan()
    {
        Map<String, Domain> filterConstraint = ImmutableMap.<String, Domain>builder()
                .put("orderstatus", singleValue(createVarcharType(1), utf8Slice("F")))
                .build();
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("orderstatus = CAST ('F' AS VARCHAR(1))"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))))))
                .matches(
                        constrainedTableScanWithTableLayout("orders", filterConstraint, ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void ruleAddedNewTableLayoutIfTableScanHasEmptyConstraint()
    {
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("orderstatus = 'F'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))))))
                .matches(
                        constrainedTableScanWithTableLayout(
                                "orders",
                                ImmutableMap.of("orderstatus", singleValue(createVarcharType(1), utf8Slice("F"))),
                                ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void ruleWithPushdownableToTableLayoutPredicate()
    {
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("orderstatus = 'O'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", orderStatusType)),
                                ImmutableMap.of(p.symbol("orderstatus", orderStatusType), new TpchColumnHandle("orderstatus", orderStatusType)))))
                .matches(constrainedTableScanWithTableLayout(
                        "orders",
                        ImmutableMap.of("orderstatus", singleValue(orderStatusType, utf8Slice("O"))),
                        ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void nonDeterministicPredicate()
    {
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("orderstatus = 'O' AND rand() = 0"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", orderStatusType)),
                                ImmutableMap.of(p.symbol("orderstatus", orderStatusType), new TpchColumnHandle("orderstatus", orderStatusType)))))
                .matches(
                        filter("rand() = 0",
                                constrainedTableScanWithTableLayout(
                                        "orders",
                                        ImmutableMap.of("orderstatus", singleValue(orderStatusType, utf8Slice("O"))),
                                        ImmutableMap.of("orderstatus", "orderstatus"))));
    }
}
