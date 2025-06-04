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
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPushJoinIntoTableScan
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));

    private static final String SCHEMA = "test_schema";
    private static final String TABLE_A = "test_table_a";
    private static final String TABLE_B = "test_table_b";
    private static final SchemaTableName TABLE_A_SCHEMA_TABLE_NAME = new SchemaTableName(SCHEMA, TABLE_A);
    private static final SchemaTableName TABLE_B_SCHEMA_TABLE_NAME = new SchemaTableName(SCHEMA, TABLE_B);

    private static final Session MOCK_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(SCHEMA)
            .build();

    private static final String COLUMN_A1 = "columna1";
    public static final Variable COLUMN_A1_VARIABLE = new Variable(COLUMN_A1, BIGINT);
    private static final ColumnHandle COLUMN_A1_HANDLE = new MockConnectorColumnHandle(COLUMN_A1, BIGINT);
    private static final String COLUMN_A2 = "columna2";
    private static final ColumnHandle COLUMN_A2_HANDLE = new MockConnectorColumnHandle(COLUMN_A2, BIGINT);
    private static final String COLUMN_B1 = "columnb1";
    public static final Variable COLUMN_B1_VARIABLE = new Variable(COLUMN_B1, BIGINT);
    private static final ColumnHandle COLUMN_B1_HANDLE = new MockConnectorColumnHandle(COLUMN_B1, BIGINT);

    private static final Map<String, ColumnHandle> TABLE_A_ASSIGNMENTS = ImmutableMap.of(
            COLUMN_A1, COLUMN_A1_HANDLE,
            COLUMN_A2, COLUMN_A2_HANDLE);

    private static final Map<String, ColumnHandle> TABLE_B_ASSIGNMENTS = ImmutableMap.of(
            COLUMN_B1, COLUMN_B1_HANDLE);

    private static final List<ColumnMetadata> TABLE_A_COLUMN_METADATA = TABLE_A_ASSIGNMENTS.entrySet().stream()
            .map(entry -> new ColumnMetadata(entry.getKey(), ((MockConnectorColumnHandle) entry.getValue()).getType()))
            .collect(toImmutableList());

    private static final List<ColumnMetadata> TABLE_B_COLUMN_METADATA = TABLE_B_ASSIGNMENTS.entrySet().stream()
            .map(entry -> new ColumnMetadata(entry.getKey(), ((MockConnectorColumnHandle) entry.getValue()).getType()))
            .collect(toImmutableList());

    public static final SchemaTableName JOIN_PUSHDOWN_SCHEMA_TABLE_NAME = new SchemaTableName(SCHEMA, "TABLE_A_JOINED_WITH_B");

    public static final ColumnHandle JOIN_COLUMN_A1_HANDLE = new MockConnectorColumnHandle("join_" + COLUMN_A1, BIGINT);
    public static final ColumnHandle JOIN_COLUMN_A2_HANDLE = new MockConnectorColumnHandle("join_" + COLUMN_A2, BIGINT);
    public static final ColumnHandle JOIN_COLUMN_B1_HANDLE = new MockConnectorColumnHandle("join_" + COLUMN_B1, BIGINT);

    public static final MockConnectorTableHandle JOIN_CONNECTOR_TABLE_HANDLE = new MockConnectorTableHandle(
            JOIN_PUSHDOWN_SCHEMA_TABLE_NAME, TupleDomain.none(), Optional.of(ImmutableList.of(JOIN_COLUMN_A1_HANDLE, JOIN_COLUMN_A2_HANDLE, JOIN_COLUMN_B1_HANDLE)));

    public static final Map<ColumnHandle, ColumnHandle> JOIN_TABLE_A_COLUMN_MAPPING = ImmutableMap.of(
            COLUMN_A1_HANDLE, JOIN_COLUMN_A1_HANDLE,
            COLUMN_A2_HANDLE, JOIN_COLUMN_A2_HANDLE);
    public static final Map<ColumnHandle, ColumnHandle> JOIN_TABLE_B_COLUMN_MAPPING = ImmutableMap.of(
            COLUMN_B1_HANDLE, JOIN_COLUMN_B1_HANDLE);

    public static final List<ColumnMetadata> JOIN_TABLE_COLUMN_METADATA = JOIN_TABLE_A_COLUMN_MAPPING.entrySet().stream()
            .map(entry -> new ColumnMetadata(((MockConnectorColumnHandle) entry.getValue()).getName(), ((MockConnectorColumnHandle) entry.getValue()).getType()))
            .collect(toImmutableList());

    @ParameterizedTest
    @MethodSource("testPushJoinIntoTableScanParams")
    public void testPushJoinIntoTableScan(io.trino.sql.planner.plan.JoinType joinType, Optional<Comparison.Operator> filterComparisonOperator)
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory((session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
            assertThat(((MockConnectorTableHandle) left).getTableName()).isEqualTo(TABLE_A_SCHEMA_TABLE_NAME);
            assertThat(((MockConnectorTableHandle) right).getTableName()).isEqualTo(TABLE_B_SCHEMA_TABLE_NAME);
            assertThat(applyJoinType).isEqualTo(toSpiJoinType(joinType));
            JoinCondition.Operator expectedOperator = filterComparisonOperator.map(this::getConditionOperator).orElse(JoinCondition.Operator.EQUAL);
            assertThat(joinConditions).containsExactly(new JoinCondition(expectedOperator, COLUMN_A1_VARIABLE, COLUMN_B1_VARIABLE));

            return Optional.of(new JoinApplicationResult<>(
                    JOIN_CONNECTOR_TABLE_HANDLE,
                    JOIN_TABLE_A_COLUMN_MAPPING,
                    JOIN_TABLE_B_COLUMN_MAPPING,
                    false));
        });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1);
                        TableScanNode left = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode right = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE));

                        if (filterComparisonOperator.isEmpty()) {
                            return p.join(
                                    joinType,
                                    left,
                                    right,
                                    new JoinNode.EquiJoinClause(columnA1Symbol, columnB1Symbol));
                        }
                        return p.join(
                                joinType,
                                left,
                                right,
                                new Comparison(filterComparisonOperator.get(), columnA1Symbol.toSymbolReference(), columnB1Symbol.toSymbolReference()));
                    })
                    .matches(
                            project(
                                    tableScan(JOIN_PUSHDOWN_SCHEMA_TABLE_NAME.getTableName())));
        }
    }

    public static Stream<Arguments> testPushJoinIntoTableScanParams()
    {
        return Stream.of(
                Arguments.of(INNER, Optional.empty()),
                Arguments.of(INNER, Optional.of(Comparison.Operator.EQUAL)),
                Arguments.of(INNER, Optional.of(Comparison.Operator.LESS_THAN)),
                Arguments.of(INNER, Optional.of(Comparison.Operator.LESS_THAN_OR_EQUAL)),
                Arguments.of(INNER, Optional.of(Comparison.Operator.GREATER_THAN)),
                Arguments.of(INNER, Optional.of(Comparison.Operator.GREATER_THAN_OR_EQUAL)),
                Arguments.of(INNER, Optional.of(Comparison.Operator.NOT_EQUAL)),
                Arguments.of(INNER, Optional.of(Comparison.Operator.IDENTICAL)),

                Arguments.of(io.trino.sql.planner.plan.JoinType.LEFT, Optional.empty()),
                Arguments.of(io.trino.sql.planner.plan.JoinType.LEFT, Optional.of(Comparison.Operator.EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.LEFT, Optional.of(Comparison.Operator.LESS_THAN)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.LEFT, Optional.of(Comparison.Operator.LESS_THAN_OR_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.LEFT, Optional.of(Comparison.Operator.GREATER_THAN)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.LEFT, Optional.of(Comparison.Operator.GREATER_THAN_OR_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.LEFT, Optional.of(Comparison.Operator.NOT_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.LEFT, Optional.of(Comparison.Operator.IDENTICAL)),

                Arguments.of(io.trino.sql.planner.plan.JoinType.RIGHT, Optional.empty()),
                Arguments.of(io.trino.sql.planner.plan.JoinType.RIGHT, Optional.of(Comparison.Operator.EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.RIGHT, Optional.of(Comparison.Operator.LESS_THAN)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.RIGHT, Optional.of(Comparison.Operator.LESS_THAN_OR_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.RIGHT, Optional.of(Comparison.Operator.GREATER_THAN)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.RIGHT, Optional.of(Comparison.Operator.GREATER_THAN_OR_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.RIGHT, Optional.of(Comparison.Operator.NOT_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.RIGHT, Optional.of(Comparison.Operator.IDENTICAL)),

                Arguments.of(io.trino.sql.planner.plan.JoinType.FULL, Optional.empty()),
                Arguments.of(io.trino.sql.planner.plan.JoinType.FULL, Optional.of(Comparison.Operator.EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.FULL, Optional.of(Comparison.Operator.LESS_THAN)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.FULL, Optional.of(Comparison.Operator.LESS_THAN_OR_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.FULL, Optional.of(Comparison.Operator.GREATER_THAN)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.FULL, Optional.of(Comparison.Operator.GREATER_THAN_OR_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.FULL, Optional.of(Comparison.Operator.NOT_EQUAL)),
                Arguments.of(io.trino.sql.planner.plan.JoinType.FULL, Optional.of(Comparison.Operator.IDENTICAL)));
    }

    /**
     * Test a scenario where join condition cannot be represented with simple comparisons.
     */
    @Test
    public void testPushJoinIntoTableScanWithComplexFilter()
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    assertThat(joinConditions).as("joinConditions")
                            .isEqualTo(List.of(
                                    new JoinCondition(
                                            JoinCondition.Operator.GREATER_THAN,
                                            new Call(
                                                    BIGINT,
                                                    MULTIPLY_FUNCTION_NAME,
                                                    List.of(
                                                            new io.trino.spi.expression.Constant(44L, BIGINT),
                                                            new Variable("columna1", BIGINT))),
                                            new Variable("columnb1", BIGINT))));
                    return Optional.of(new JoinApplicationResult<>(
                            JOIN_CONNECTOR_TABLE_HANDLE,
                            JOIN_TABLE_A_COLUMN_MAPPING,
                            JOIN_TABLE_B_COLUMN_MAPPING,
                            false));
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1);
                        TableScanNode left = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode right = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE));

                        return p.join(
                                INNER,
                                left,
                                right,
                                new Comparison(
                                        Comparison.Operator.GREATER_THAN,
                                        new io.trino.sql.ir.Call(MULTIPLY_BIGINT, ImmutableList.of(new Constant(BIGINT, 44L), columnA1Symbol.toSymbolReference())),
                                        columnB1Symbol.toSymbolReference()));
                    })
                    .matches(
                            project(
                                    tableScan(JOIN_PUSHDOWN_SCHEMA_TABLE_NAME.getTableName())));
        }
    }

    @Test
    public void testPushJoinIntoTableScanDoesNotFireForDifferentCatalogs()
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    throw new IllegalStateException("applyJoin should not be called!");
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.getPlanTester().createCatalog("another_catalog", "mock", ImmutableMap.of());
            TableHandle tableBHandleAnotherCatalog = createTableHandle(new MockConnectorTableHandle(new SchemaTableName(SCHEMA, TABLE_B)), createTestCatalogHandle("another_catalog"));

            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1);
                        TableScanNode left = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode right = p.tableScan(
                                tableBHandleAnotherCatalog,
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE));

                        return p.join(
                                INNER,
                                left,
                                right,
                                new JoinNode.EquiJoinClause(columnA1Symbol, columnB1Symbol));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testPushJoinIntoTableScanDoesNotFireWhenDisabled()
    {
        Session joinPushDownDisabledSession = Session.builder(MOCK_SESSION)
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();

        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    throw new IllegalStateException("applyJoin should not be called!");
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(joinPushDownDisabledSession)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1);
                        TableScanNode left = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode right = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE));

                        return p.join(
                                INNER,
                                left,
                                right,
                                new JoinNode.EquiJoinClause(columnA1Symbol, columnB1Symbol));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testPushJoinIntoTableScanDoesNotFireWhenAllPushdownsDisabled()
    {
        Session allPushdownsDisabledSession = Session.builder(MOCK_SESSION)
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();

        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    throw new IllegalStateException("applyJoin should not be called!");
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(allPushdownsDisabledSession)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1);
                        TableScanNode left = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode right = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE));

                        return p.join(
                                INNER,
                                left,
                                right,
                                new JoinNode.EquiJoinClause(columnA1Symbol, columnB1Symbol));
                    })
                    .doesNotFire();
        }
    }

    @ParameterizedTest
    @MethodSource("testPushJoinIntoTableScanPreservesEnforcedConstraintParams")
    public void testPushJoinIntoTableScanPreservesEnforcedConstraint(io.trino.sql.planner.plan.JoinType joinType, TupleDomain<ColumnHandle> leftConstraint, TupleDomain<ColumnHandle> rightConstraint, TupleDomain<Predicate<ColumnHandle>> expectedConstraint)
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory((session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> Optional.of(new JoinApplicationResult<>(
                JOIN_CONNECTOR_TABLE_HANDLE,
                JOIN_TABLE_A_COLUMN_MAPPING,
                JOIN_TABLE_B_COLUMN_MAPPING,
                false)));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1);

                        TableScanNode left = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE),
                                leftConstraint);
                        TableScanNode right = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE),
                                rightConstraint);

                        return p.join(
                                joinType,
                                left,
                                right,
                                new JoinNode.EquiJoinClause(columnA1Symbol, columnB1Symbol));
                    })
                    .matches(
                            project(
                                    tableScan(
                                            tableHandle -> JOIN_PUSHDOWN_SCHEMA_TABLE_NAME.equals(((MockConnectorTableHandle) tableHandle).getTableName()),
                                            expectedConstraint,
                                            ImmutableMap.of())));
        }
    }

    public static Stream<Arguments> testPushJoinIntoTableScanPreservesEnforcedConstraintParams()
    {
        Domain columnA1Domain = Domain.multipleValues(BIGINT, List.of(3L));
        Domain columnA2Domain = Domain.multipleValues(BIGINT, List.of(10L, 20L));
        Domain columnB1Domain = Domain.multipleValues(BIGINT, List.of(30L, 40L));
        return Stream.of(
                Arguments.of(
                        INNER,
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_A1_HANDLE, columnA1Domain,
                                COLUMN_A2_HANDLE, columnA2Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_B1_HANDLE, columnB1Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                equalTo(JOIN_COLUMN_A1_HANDLE), columnA1Domain,
                                equalTo(JOIN_COLUMN_A2_HANDLE), columnA2Domain,
                                equalTo(JOIN_COLUMN_B1_HANDLE), columnB1Domain))),
                Arguments.of(
                        RIGHT,
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_A1_HANDLE, columnA1Domain,
                                COLUMN_A2_HANDLE, columnA2Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_B1_HANDLE, columnB1Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                equalTo(JOIN_COLUMN_A1_HANDLE), columnA1Domain.union(onlyNull(BIGINT)),
                                equalTo(JOIN_COLUMN_A2_HANDLE), columnA2Domain.union(onlyNull(BIGINT)),
                                equalTo(JOIN_COLUMN_B1_HANDLE), columnB1Domain))),
                Arguments.of(
                        LEFT,
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_A1_HANDLE, columnA1Domain,
                                COLUMN_A2_HANDLE, columnA2Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_B1_HANDLE, columnB1Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                equalTo(JOIN_COLUMN_A1_HANDLE), columnA1Domain,
                                equalTo(JOIN_COLUMN_A2_HANDLE), columnA2Domain,
                                equalTo(JOIN_COLUMN_B1_HANDLE), columnB1Domain.union(onlyNull(BIGINT))))),
                Arguments.of(
                        FULL,
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_A1_HANDLE, columnA1Domain,
                                COLUMN_A2_HANDLE, columnA2Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_B1_HANDLE, columnB1Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                equalTo(JOIN_COLUMN_A1_HANDLE), columnA1Domain.union(onlyNull(BIGINT)),
                                equalTo(JOIN_COLUMN_A2_HANDLE), columnA2Domain.union(onlyNull(BIGINT)),
                                equalTo(JOIN_COLUMN_B1_HANDLE), columnB1Domain.union(onlyNull(BIGINT))))));
    }

    @Test
    public void testPushJoinIntoTableDoesNotFireForCrossJoin()
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    throw new IllegalStateException("applyJoin should not be called!");
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1);

                        TableScanNode left = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode right = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE));

                        // cross-join - no criteria
                        return p.join(
                                INNER,
                                left,
                                right);
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testPushJoinIntoTableRequiresFullColumnHandleMappingInResult()
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory((session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> Optional.of(new JoinApplicationResult<>(
                JOIN_CONNECTOR_TABLE_HANDLE,
                ImmutableMap.of(COLUMN_A1_HANDLE, JOIN_COLUMN_A1_HANDLE, COLUMN_A2_HANDLE, JOIN_COLUMN_A2_HANDLE),
                // mapping for COLUMN_B1_HANDLE is missing
                ImmutableMap.of(),
                false)));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            assertThatThrownBy(() -> {
                ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext()))
                        .withSession(MOCK_SESSION)
                        .on(p -> {
                            Symbol columnA1Symbol = p.symbol(COLUMN_A1);
                            Symbol columnA2Symbol = p.symbol(COLUMN_A2);
                            Symbol columnB1Symbol = p.symbol(COLUMN_B1);

                            TupleDomain<ColumnHandle> leftConstraint =
                                    TupleDomain.fromFixedValues(ImmutableMap.of(COLUMN_A2_HANDLE, NullableValue.of(BIGINT, 44L)));
                            TupleDomain<ColumnHandle> rightConstraint =
                                    TupleDomain.fromFixedValues(ImmutableMap.of(COLUMN_B1_HANDLE, NullableValue.of(BIGINT, 45L)));

                            TableScanNode left = p.tableScan(
                                    ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                    ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                    ImmutableMap.of(
                                            columnA1Symbol, COLUMN_A1_HANDLE,
                                            columnA2Symbol, COLUMN_A2_HANDLE),
                                    leftConstraint);
                            TableScanNode right = p.tableScan(
                                    ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                    ImmutableList.of(columnB1Symbol),
                                    ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE),
                                    rightConstraint);

                            return p.join(
                                    INNER,
                                    left,
                                    right,
                                    new JoinNode.EquiJoinClause(columnA1Symbol, columnB1Symbol));
                        })
                        .matches(anyTree());
            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Column handle mappings do not match old column handles");
        }
    }

    private static TableHandle createTableHandle(ConnectorTableHandle tableHandle)
    {
        return createTableHandle(tableHandle, TEST_CATALOG_HANDLE);
    }

    private static TableHandle createTableHandle(ConnectorTableHandle tableHandle, CatalogHandle catalogHandle)
    {
        return new TableHandle(
                catalogHandle,
                tableHandle,
                new ConnectorTransactionHandle() {});
    }

    private MockConnectorFactory createMockConnectorFactory(MockConnectorFactory.ApplyJoin applyJoin)
    {
        return MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of(SCHEMA))
                .withListTables((connectorSession, schema) -> SCHEMA.equals(schema) ? ImmutableList.of(TABLE_A_SCHEMA_TABLE_NAME.getTableName(), TABLE_B_SCHEMA_TABLE_NAME.getTableName()) : ImmutableList.of())
                .withApplyJoin(applyJoin)
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(TABLE_A_SCHEMA_TABLE_NAME)) {
                        return TABLE_A_COLUMN_METADATA;
                    }
                    if (schemaTableName.equals(TABLE_B_SCHEMA_TABLE_NAME)) {
                        return TABLE_B_COLUMN_METADATA;
                    }
                    if (schemaTableName.equals(JOIN_PUSHDOWN_SCHEMA_TABLE_NAME)) {
                        return JOIN_TABLE_COLUMN_METADATA;
                    }
                    throw new RuntimeException("Unknown table: " + schemaTableName);
                })
                .build();
    }

    private JoinType toSpiJoinType(io.trino.sql.planner.plan.JoinType joinType)
    {
        return switch (joinType) {
            case INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT_OUTER;
            case RIGHT -> JoinType.RIGHT_OUTER;
            case FULL -> JoinType.FULL_OUTER;
        };
    }

    private JoinCondition.Operator getConditionOperator(Comparison.Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return JoinCondition.Operator.EQUAL;
            case NOT_EQUAL:
                return JoinCondition.Operator.NOT_EQUAL;
            case LESS_THAN:
                return JoinCondition.Operator.LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return JoinCondition.Operator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return JoinCondition.Operator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return JoinCondition.Operator.GREATER_THAN_OR_EQUAL;
            case IDENTICAL:
                return JoinCondition.Operator.IDENTICAL;
        }
        throw new IllegalArgumentException("Unknown operator: " + operator);
    }
}
