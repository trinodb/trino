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
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPushPredicateIntoTableScan
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));

    private static final String MOCK_CATALOG = "mock_catalog";
    private static final ConnectorTableHandle CONNECTOR_PARTITIONED_TABLE_HANDLE =
            new MockConnectorTableHandle(new SchemaTableName("schema", "partitioned"));
    private static final ConnectorTableHandle CONNECTOR_PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED =
            new MockConnectorTableHandle(new SchemaTableName("schema", "partitioned_to_unpartitioned"));
    private static final ConnectorTableHandle CONNECTOR_UNPARTITIONED_TABLE_HANDLE =
            new MockConnectorTableHandle(new SchemaTableName("schema", "unpartitioned"));
    private static final ConnectorPartitioningHandle PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};
    private static final ColumnHandle MOCK_COLUMN_HANDLE = new MockConnectorColumnHandle("col", VARCHAR);

    private PushPredicateIntoTableScan pushPredicateIntoTableScan;
    private CatalogHandle mockCatalogHandle;
    private TableHandle nationTableHandle;
    private TableHandle ordersTableHandle;
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @BeforeAll
    public void setUpBeforeClass()
    {
        pushPredicateIntoTableScan = new PushPredicateIntoTableScan(tester().getPlannerContext(), false);

        CatalogHandle catalogHandle = tester().getCurrentCatalogHandle();
        tester().getPlanTester().createCatalog(MOCK_CATALOG, createMockFactory(), ImmutableMap.of());
        mockCatalogHandle = tester().getPlanTester().getCatalogHandle(MOCK_CATALOG);

        TpchTableHandle nation = new TpchTableHandle("sf1", "nation", 1.0);
        nationTableHandle = new TableHandle(
                catalogHandle,
                nation,
                TpchTransactionHandle.INSTANCE);

        TpchTableHandle orders = new TpchTableHandle("sf1", "orders", 1.0);
        ordersTableHandle = new TableHandle(
                catalogHandle,
                orders,
                TpchTransactionHandle.INSTANCE);
    }

    @Test
    public void testDoesNotFireIfNoTableScan()
    {
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testEliminateTableScanWhenNoLayoutExist()
    {
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Comparison(EQUAL, new Reference(createVarcharType(1), "orderstatus"), new Constant(createVarcharType(1), utf8Slice("G"))),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))))))
                .matches(values("A"));
    }

    @Test
    public void testReplaceWithExistsWhenNoLayoutExist()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 44L)),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.fromFixedValues(ImmutableMap.of(
                                        columnHandle, NullableValue.of(BIGINT, (long) 45))))))
                .matches(values("A"));
    }

    @Test
    public void testConsumesDeterministicPredicateIfNewDomainIsSame()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 44L)),
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
    public void testConsumesDeterministicPredicateIfNewDomainIsWider()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 44L)), new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 45L)))),
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
    public void testConsumesDeterministicPredicateIfNewDomainIsNarrower()
    {
        Type orderStatusType = createVarcharType(1);
        ColumnHandle columnHandle = new TpchColumnHandle("orderstatus", orderStatusType);
        Map<String, Domain> filterConstraint = ImmutableMap.of("orderstatus", singleValue(orderStatusType, utf8Slice("O")));
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(createVarcharType(1), "orderstatus"), new Constant(createVarcharType(1), utf8Slice("O"))), new Comparison(EQUAL, new Reference(createVarcharType(1), "orderstatus"), new Constant(createVarcharType(1), utf8Slice("F"))))),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", orderStatusType)),
                                ImmutableMap.of(p.symbol("orderstatus", orderStatusType), new TpchColumnHandle("orderstatus", orderStatusType)),
                                TupleDomain.withColumnDomains(ImmutableMap.of(
                                        columnHandle, Domain.multipleValues(orderStatusType, ImmutableList.of(Slices.utf8Slice("O"), Slices.utf8Slice("P"))))))))
                .matches(
                        constrainedTableScanWithTableLayout("orders", filterConstraint, ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void testDoesNotConsumeRemainingPredicateIfNewDomainIsWider()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Logical(
                                AND,
                                ImmutableList.of(
                                        new Comparison(
                                                EQUAL,
                                                functionResolution
                                                        .functionCallBuilder("rand")
                                                        .build(),
                                                new Constant(DOUBLE, 42.0)),
                                        // non-translatable to connector expression
                                        new Coalesce(
                                                new Constant(BOOLEAN, null),
                                                new Comparison(
                                                        EQUAL,
                                                        new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 17L))),
                                                        new Constant(BIGINT, 44L))),
                                        Logical.or(
                                                new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 44L)),
                                                new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 45L))))),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.fromFixedValues(ImmutableMap.of(
                                        columnHandle, NullableValue.of(BIGINT, (long) 44))))))
                .matches(
                        filter(
                                Logical.and(
                                        new Comparison(
                                                EQUAL,
                                                functionResolution
                                                        .functionCallBuilder("rand")
                                                        .build(),
                                                new Constant(DOUBLE, 42.0)),
                                        new Comparison(
                                                EQUAL,
                                                new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 17L))),
                                                new Constant(BIGINT, 44L))),
                                constrainedTableScanWithTableLayout(
                                        "nation",
                                        ImmutableMap.of("nationkey", singleValue(BIGINT, (long) 44)),
                                        ImmutableMap.of("nationkey", "nationkey"))));
    }

    @Test
    public void testDoesNotFireOnNonDeterministicPredicate()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Comparison(
                                EQUAL,
                                functionResolution
                                        .functionCallBuilder("rand")
                                        .build(),
                                new Constant(DOUBLE, 42.0)),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                TupleDomain.all())))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireIfRuleNotChangePlan()
    {
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 17L))), new Constant(BIGINT, 44L)), new Comparison(EQUAL, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 15L))), new Constant(BIGINT, 43L)))),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                TupleDomain.all())))
                .doesNotFire();
    }

    @Test
    public void testRuleAddedTableLayoutToFilterTableScan()
    {
        Map<String, Domain> filterConstraint = ImmutableMap.of("orderstatus", singleValue(createVarcharType(1), utf8Slice("F")));
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Comparison(EQUAL, new Reference(createVarcharType(1), "orderstatus"), new Constant(createVarcharType(1), utf8Slice("F"))),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))))))
                .matches(
                        constrainedTableScanWithTableLayout("orders", filterConstraint, ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void testNonDeterministicPredicate()
    {
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        Logical.and(
                                new Comparison(EQUAL, new Reference(createVarcharType(1), "orderstatus"), new Constant(createVarcharType(1), utf8Slice("O"))),
                                new Comparison(
                                        EQUAL,
                                        functionResolution
                                                .functionCallBuilder("rand")
                                                .build(),
                                        new Constant(DOUBLE, 0.0))),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", orderStatusType)),
                                ImmutableMap.of(p.symbol("orderstatus", orderStatusType), new TpchColumnHandle("orderstatus", orderStatusType)))))
                .matches(
                        filter(
                                new Comparison(
                                        EQUAL,
                                        functionResolution
                                                .functionCallBuilder("rand")
                                                .build(),
                                        new Constant(DOUBLE, 0.0)),
                                constrainedTableScanWithTableLayout(
                                        "orders",
                                        ImmutableMap.of("orderstatus", singleValue(orderStatusType, utf8Slice("O"))),
                                        ImmutableMap.of("orderstatus", "orderstatus"))));
    }

    @Test
    public void testPartitioningChanged()
    {
        Session session = Session.builder(tester().getSession())
                .setCatalog(MOCK_CATALOG)
                .build();
        assertThatThrownBy(() -> tester().assertThat(pushPredicateIntoTableScan)
                .withSession(session)
                .on(p -> p.filter(
                        new Comparison(EQUAL, new Reference(VARCHAR, "col"), new Constant(VARCHAR, utf8Slice("G"))),
                        p.tableScan(
                                mockTableHandle(CONNECTOR_PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED),
                                ImmutableList.of(p.symbol("col", VARCHAR)),
                                ImmutableMap.of(p.symbol("col", VARCHAR), MOCK_COLUMN_HANDLE),
                                Optional.of(true))))
                .matches(anyTree()))
                .hasMessage("Partitioning must not change after predicate is pushed down");

        tester().assertThat(pushPredicateIntoTableScan)
                .withSession(session)
                .on(p -> p.filter(
                        new Comparison(EQUAL, new Reference(VARCHAR, "col"), new Constant(VARCHAR, utf8Slice("G"))),
                        p.tableScan(
                                mockTableHandle(CONNECTOR_PARTITIONED_TABLE_HANDLE),
                                ImmutableList.of(p.symbol("col", VARCHAR)),
                                ImmutableMap.of(p.symbol("col", VARCHAR), MOCK_COLUMN_HANDLE),
                                Optional.of(true))))
                .matches(tableScan("partitioned"));
    }

    @Test
    public void testEliminateTableScanWhenPredicateIsNull()
    {
        ColumnHandle nationKeyColumn = new TpchColumnHandle("nationkey", BIGINT);

        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Constant(BOOLEAN, null),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), nationKeyColumn))))
                .matches(values(ImmutableList.of("A"), ImmutableList.of()));

        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, null)),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), nationKeyColumn))))
                .matches(values(ImmutableList.of("A"), ImmutableList.of()));

        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 44L)), new Constant(BOOLEAN, null))),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), nationKeyColumn))))
                .matches(values(ImmutableList.of("A"), ImmutableList.of()));
    }

    public static MockConnectorFactory createMockFactory()
    {
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
        builder
                .withApplyFilter((session, tableHandle, constraint) -> {
                    if (tableHandle.equals(CONNECTOR_PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED)) {
                        return Optional.of(new ConstraintApplicationResult<>(CONNECTOR_UNPARTITIONED_TABLE_HANDLE, TupleDomain.all(), constraint.getExpression(), false));
                    }
                    if (tableHandle.equals(CONNECTOR_PARTITIONED_TABLE_HANDLE)) {
                        return Optional.of(new ConstraintApplicationResult<>(CONNECTOR_PARTITIONED_TABLE_HANDLE, TupleDomain.all(), constraint.getExpression(), false));
                    }
                    return Optional.empty();
                })
                .withGetTableProperties((session, tableHandle) -> {
                    if (tableHandle.equals(CONNECTOR_PARTITIONED_TABLE_HANDLE) || tableHandle.equals(CONNECTOR_PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.of(new ConnectorTablePartitioning(PARTITIONING_HANDLE, ImmutableList.of(MOCK_COLUMN_HANDLE))),
                                Optional.empty(),
                                ImmutableList.of());
                    }
                    return new ConnectorTableProperties();
                });
        return builder.build();
    }

    private TableHandle mockTableHandle(ConnectorTableHandle connectorTableHandle)
    {
        return new TableHandle(
                mockCatalogHandle,
                connectorTableHandle,
                TestingTransactionHandle.create());
    }
}
