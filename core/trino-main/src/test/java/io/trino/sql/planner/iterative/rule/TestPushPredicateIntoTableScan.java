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
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MODULUS;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPushPredicateIntoTableScan
        extends BaseRuleTest
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final ConnectorTableHandle CONNECTOR_PARTITIONED_TABLE_HANDLE =
            new MockConnectorTableHandle(new SchemaTableName("schema", "partitioned"));
    private static final ConnectorTableHandle CONNECTOR_PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED =
            new MockConnectorTableHandle(new SchemaTableName("schema", "partitioned_to_unpartitioned"));
    private static final ConnectorTableHandle CONNECTOR_UNPARTITIONED_TABLE_HANDLE =
            new MockConnectorTableHandle(new SchemaTableName("schema", "unpartitioned"));
    private static final TableHandle PARTITIONED_TABLE_HANDLE = tableHandle(CONNECTOR_PARTITIONED_TABLE_HANDLE);
    private static final TableHandle PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED = tableHandle(CONNECTOR_PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED);
    private static final ConnectorPartitioningHandle PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};
    private static final ColumnHandle MOCK_COLUMN_HANDLE = new MockConnectorColumnHandle("col", VARCHAR);

    private PushPredicateIntoTableScan pushPredicateIntoTableScan;
    private TableHandle nationTableHandle;
    private TableHandle ordersTableHandle;
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @BeforeClass
    public void setUpBeforeClass()
    {
        pushPredicateIntoTableScan = new PushPredicateIntoTableScan(tester().getPlannerContext(), createTestingTypeAnalyzer(tester().getPlannerContext()));

        CatalogName catalogName = tester().getCurrentConnectorId();
        tester().getQueryRunner().createCatalog(MOCK_CATALOG, createMockFactory(), ImmutableMap.of());

        TpchTableHandle nation = new TpchTableHandle("sf1", "nation", 1.0);
        nationTableHandle = new TableHandle(
                catalogName,
                nation,
                TpchTransactionHandle.INSTANCE);

        TpchTableHandle orders = new TpchTableHandle("sf1", "orders", 1.0);
        ordersTableHandle = new TableHandle(
                catalogName,
                orders,
                TpchTransactionHandle.INSTANCE);
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
                                TupleDomain.fromFixedValues(ImmutableMap.of(
                                        columnHandle, NullableValue.of(BIGINT, (long) 45))))))
                .matches(values("A"));
    }

    @Test
    public void consumesDeterministicPredicateIfNewDomainIsSame()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pushPredicateIntoTableScan)
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
        tester().assertThat(pushPredicateIntoTableScan)
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
        Type orderStatusType = createVarcharType(1);
        ColumnHandle columnHandle = new TpchColumnHandle("orderstatus", orderStatusType);
        Map<String, Domain> filterConstraint = ImmutableMap.<String, Domain>builder()
                .put("orderstatus", singleValue(orderStatusType, utf8Slice("O")))
                .buildOrThrow();
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("orderstatus = 'O' OR orderstatus = 'F'"),
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
    public void doesNotConsumeRemainingPredicateIfNewDomainIsWider()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new LogicalExpression(
                                AND,
                                ImmutableList.of(
                                        new ComparisonExpression(
                                                EQUAL,
                                                functionResolution
                                                        .functionCallBuilder(QualifiedName.of("rand"))
                                                        .build(),
                                                new GenericLiteral("BIGINT", "42")),
                                        // non-translatable to connector expression
                                        new CoalesceExpression(
                                                new Cast(new NullLiteral(), toSqlType(BOOLEAN)),
                                                new ComparisonExpression(
                                                        EQUAL,
                                                        new ArithmeticBinaryExpression(
                                                                MODULUS,
                                                                new SymbolReference("nationkey"),
                                                                new GenericLiteral("BIGINT", "17")),
                                                        new GenericLiteral("BIGINT", "44"))),
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
                                                functionResolution
                                                        .functionCallBuilder(QualifiedName.of("rand"))
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
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        new ComparisonExpression(
                                EQUAL,
                                functionResolution
                                        .functionCallBuilder(QualifiedName.of("rand"))
                                        .build(),
                                new LongLiteral("42")),
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
                .buildOrThrow();
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(expression("orderstatus = 'F'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))))))
                .matches(
                        constrainedTableScanWithTableLayout("orders", filterConstraint, ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void nonDeterministicPredicate()
    {
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(pushPredicateIntoTableScan)
                .on(p -> p.filter(
                        LogicalExpression.and(
                                new ComparisonExpression(
                                        EQUAL,
                                        new SymbolReference("orderstatus"),
                                        new StringLiteral("O")),
                                new ComparisonExpression(
                                        EQUAL,
                                        functionResolution
                                                .functionCallBuilder(QualifiedName.of("rand"))
                                                .build(),
                                        new LongLiteral("0"))),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", orderStatusType)),
                                ImmutableMap.of(p.symbol("orderstatus", orderStatusType), new TpchColumnHandle("orderstatus", orderStatusType)))))
                .matches(
                        filter(
                                new ComparisonExpression(
                                        EQUAL,
                                        functionResolution
                                                .functionCallBuilder(QualifiedName.of("rand"))
                                                .build(),
                                        new LongLiteral("0")),
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
                .on(p -> p.filter(expression("col = 'G'"),
                        p.tableScan(
                                PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED,
                                ImmutableList.of(p.symbol("col", VARCHAR)),
                                ImmutableMap.of(p.symbol("col", VARCHAR), MOCK_COLUMN_HANDLE),
                                Optional.of(true))))
                .matches(anyTree()))
                .hasMessage("Partitioning must not change after predicate is pushed down");

        tester().assertThat(pushPredicateIntoTableScan)
                .withSession(session)
                .on(p -> p.filter(expression("col = 'G'"),
                        p.tableScan(
                                PARTITIONED_TABLE_HANDLE,
                                ImmutableList.of(p.symbol("col", VARCHAR)),
                                ImmutableMap.of(p.symbol("col", VARCHAR), MOCK_COLUMN_HANDLE),
                                Optional.of(true))))
                .matches(tableScan("partitioned"));
    }

    public static MockConnectorFactory createMockFactory()
    {
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
        builder
                .withApplyFilter((session, tableHandle, constraint) -> {
                    if (tableHandle.equals(CONNECTOR_PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED)) {
                        return Optional.of(new ConstraintApplicationResult<>(CONNECTOR_UNPARTITIONED_TABLE_HANDLE, TupleDomain.all(), false));
                    }
                    if (tableHandle.equals(CONNECTOR_PARTITIONED_TABLE_HANDLE)) {
                        return Optional.of(new ConstraintApplicationResult<>(CONNECTOR_PARTITIONED_TABLE_HANDLE, TupleDomain.all(), false));
                    }
                    return Optional.empty();
                })
                .withGetTableProperties((session, tableHandle) -> {
                    if (tableHandle.equals(CONNECTOR_PARTITIONED_TABLE_HANDLE) || tableHandle.equals(CONNECTOR_PARTITIONED_TABLE_HANDLE_TO_UNPARTITIONED)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.of(new ConnectorTablePartitioning(PARTITIONING_HANDLE, ImmutableList.of(MOCK_COLUMN_HANDLE))),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of());
                    }
                    return new ConnectorTableProperties();
                });
        return builder.build();
    }

    private static TableHandle tableHandle(ConnectorTableHandle connectorTableHandle)
    {
        return new TableHandle(
                new CatalogName(MOCK_CATALOG),
                connectorTableHandle,
                TestingTransactionHandle.create());
    }
}
