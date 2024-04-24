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
import io.trino.connector.MockConnectorFactory.ApplyJoin;
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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Comparison.Operator;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.PushJoinIntoTableScan.ProjectSide;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.ProjectNode;
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
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPushJoinIntoTableScan
{
    private static final VarcharType VARCHAR = createVarcharType(5);
    private static final String SCHEMA = "test_schema";
    private static final String TABLE_A = "test_table_a";
    private static final String TABLE_B = "test_table_b";
    private static final SchemaTableName TABLE_A_SCHEMA_TABLE_NAME = new SchemaTableName(SCHEMA, TABLE_A);
    private static final SchemaTableName TABLE_B_SCHEMA_TABLE_NAME = new SchemaTableName(SCHEMA, TABLE_B);

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction UPPER_VARCHAR = FUNCTIONS.resolveFunction("upper", fromTypes(VARCHAR));

    private static final Session MOCK_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(SCHEMA)
            .build();

    private static final String CALL_1 = "upper";
    private static final String CALL_2 = "upper_0";
    private static final String COLUMN_A1 = "columna1";
    public static final Variable COLUMN_A1_VARIABLE = new Variable(COLUMN_A1, VARCHAR);
    private static final ColumnHandle COLUMN_A1_HANDLE = new MockConnectorColumnHandle(COLUMN_A1, VARCHAR);
    private static final String COLUMN_A2 = "columna2";
    private static final ColumnHandle COLUMN_A2_HANDLE = new MockConnectorColumnHandle(COLUMN_A2, VARCHAR);
    private static final String COLUMN_B1 = "columnb1";
    public static final Variable COLUMN_B1_VARIABLE = new Variable(COLUMN_B1, VARCHAR);
    private static final ColumnHandle COLUMN_B1_HANDLE = new MockConnectorColumnHandle(COLUMN_B1, VARCHAR);
    private static final String COLUMN_B2 = "columnb2";
    private static final ColumnHandle COLUMN_B2_HANDLE = new MockConnectorColumnHandle(COLUMN_B2, VARCHAR);

    private static final Map<String, ColumnHandle> TABLE_A_ASSIGNMENTS = ImmutableMap.of(
            COLUMN_A1, COLUMN_A1_HANDLE,
            COLUMN_A2, COLUMN_A2_HANDLE);

    private static final Map<String, ColumnHandle> TABLE_B_ASSIGNMENTS = ImmutableMap.of(
            COLUMN_B1, COLUMN_B1_HANDLE,
            COLUMN_B2, COLUMN_B2_HANDLE);

    private static final List<ColumnMetadata> TABLE_A_COLUMN_METADATA = TABLE_A_ASSIGNMENTS.entrySet().stream()
            .map(entry -> new ColumnMetadata(entry.getKey(), ((MockConnectorColumnHandle) entry.getValue()).getType()))
            .collect(toImmutableList());

    private static final List<ColumnMetadata> TABLE_B_COLUMN_METADATA = TABLE_B_ASSIGNMENTS.entrySet().stream()
            .map(entry -> new ColumnMetadata(entry.getKey(), ((MockConnectorColumnHandle) entry.getValue()).getType()))
            .collect(toImmutableList());

    public static final SchemaTableName JOIN_PUSHDOWN_SCHEMA_TABLE_NAME = new SchemaTableName(SCHEMA, "TABLE_A_JOINED_WITH_B");

    public static final ColumnHandle JOIN_COLUMN_A1_HANDLE = new MockConnectorColumnHandle("join_" + COLUMN_A1, VARCHAR);
    public static final ColumnHandle JOIN_COLUMN_A2_HANDLE = new MockConnectorColumnHandle("join_" + COLUMN_A2, VARCHAR);
    public static final ColumnHandle JOIN_COLUMN_B1_HANDLE = new MockConnectorColumnHandle("join_" + COLUMN_B1, VARCHAR);
    public static final ColumnHandle JOIN_COLUMN_B2_HANDLE = new MockConnectorColumnHandle("join_" + COLUMN_B2, VARCHAR);

    public static final MockConnectorTableHandle JOIN_CONNECTOR_TABLE_HANDLE = new MockConnectorTableHandle(
            JOIN_PUSHDOWN_SCHEMA_TABLE_NAME,
            TupleDomain.none(),
            Optional.of(ImmutableList.of(JOIN_COLUMN_A1_HANDLE, JOIN_COLUMN_A2_HANDLE, JOIN_COLUMN_B1_HANDLE, JOIN_COLUMN_B2_HANDLE)));

    public static final Map<ColumnHandle, ColumnHandle> JOIN_TABLE_A_COLUMN_MAPPING = ImmutableMap.of(
            COLUMN_A1_HANDLE, JOIN_COLUMN_A1_HANDLE,
            COLUMN_A2_HANDLE, JOIN_COLUMN_A2_HANDLE);
    public static final Map<ColumnHandle, ColumnHandle> JOIN_TABLE_B_COLUMN_MAPPING = ImmutableMap.of(
            COLUMN_B1_HANDLE, JOIN_COLUMN_B1_HANDLE,
            COLUMN_B2_HANDLE, JOIN_COLUMN_B2_HANDLE);

    public static final List<ColumnMetadata> JOIN_TABLE_COLUMN_METADATA = JOIN_TABLE_A_COLUMN_MAPPING.values().stream()
            .map(columnHandle -> new ColumnMetadata(((MockConnectorColumnHandle) columnHandle).getName(), ((MockConnectorColumnHandle) columnHandle).getType()))
            .collect(toImmutableList());

    @ParameterizedTest
    @MethodSource("testPushJoinIntoTableScanParams")
    public void testPushJoinIntoTableScanThroughProject(JoinType joinType, Optional<Operator> filterComparisonOperator, ProjectSide projectSide)
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory((session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
            assertThat(((MockConnectorTableHandle) left).getTableName()).isEqualTo(TABLE_A_SCHEMA_TABLE_NAME);
            assertThat(((MockConnectorTableHandle) right).getTableName()).isEqualTo(TABLE_B_SCHEMA_TABLE_NAME);
            assertThat(applyJoinType).isEqualTo(toSpiJoinType(joinType));
            if (filterComparisonOperator.isEmpty()) {
                assertThat(joinConditions).isEqualTo(new Call(
                        BooleanType.BOOLEAN,
                        new FunctionName("$equal"),
                        switch (projectSide) {
                            case LEFT -> ImmutableList.of(
                                    new Call(VARCHAR, new FunctionName(CALL_1), ImmutableList.of(COLUMN_A1_VARIABLE)),
                                    COLUMN_B1_VARIABLE);
                            case RIGHT -> ImmutableList.of(
                                    COLUMN_A1_VARIABLE,
                                    new Call(VARCHAR, new FunctionName(CALL_1), ImmutableList.of(COLUMN_B1_VARIABLE)));
                            case BOTH -> ImmutableList.of(
                                    new Call(VARCHAR, new FunctionName(CALL_1), ImmutableList.of(COLUMN_A1_VARIABLE)),
                                    new Call(VARCHAR, new FunctionName(CALL_1), ImmutableList.of(COLUMN_B1_VARIABLE)));
                            case NONE -> ImmutableList.of(
                                    COLUMN_A1_VARIABLE,
                                    COLUMN_B1_VARIABLE);
                        }));
            }
            else {
                assertThat(joinConditions).isEqualTo(new Call(
                        BooleanType.BOOLEAN,
                        new FunctionName("$and"),
                        ImmutableList.of(
                                new Call(
                                        BooleanType.BOOLEAN,
                                        new FunctionName("$equal"),
                                        switch (projectSide) {
                                            case LEFT -> ImmutableList.of(
                                                    new Call(
                                                            VARCHAR,
                                                            new FunctionName("upper"),
                                                            ImmutableList.of(COLUMN_A1_VARIABLE)),
                                                    COLUMN_B1_VARIABLE);
                                            case RIGHT -> ImmutableList.of(
                                                    COLUMN_A1_VARIABLE,
                                                    new Call(
                                                            VARCHAR,
                                                            new FunctionName("upper"),
                                                            ImmutableList.of(COLUMN_B1_VARIABLE)));
                                            case BOTH -> ImmutableList.of(
                                                    new Call(
                                                            VARCHAR,
                                                            new FunctionName("upper"),
                                                            ImmutableList.of(COLUMN_A1_VARIABLE)),
                                                    new Call(
                                                            VARCHAR,
                                                            new FunctionName("upper"),
                                                            ImmutableList.of(COLUMN_B1_VARIABLE)));
                                            case NONE -> ImmutableList.of(
                                                    COLUMN_A1_VARIABLE,
                                                    COLUMN_B1_VARIABLE);
                                        }),
                                new Call(
                                        BooleanType.BOOLEAN,
                                        filterComparisonOperator.map(this::toSpiFunctionName).orElse(new FunctionName("$equal")),
                                        switch (projectSide) {
                                            case LEFT -> ImmutableList.of(new Variable(CALL_1, VARCHAR), COLUMN_B1_VARIABLE);
                                            case RIGHT -> ImmutableList.of(COLUMN_A1_VARIABLE, new Variable(CALL_1, VARCHAR));
                                            case BOTH -> ImmutableList.of(new Variable(CALL_1, VARCHAR), new Variable(CALL_2, VARCHAR));
                                            case NONE -> ImmutableList.of(COLUMN_A1_VARIABLE, COLUMN_B1_VARIABLE);
                                        }))));
            }
            return Optional.of(new JoinApplicationResult<>(
                    JOIN_CONNECTOR_TABLE_HANDLE,
                    JOIN_TABLE_A_COLUMN_MAPPING,
                    JOIN_TABLE_B_COLUMN_MAPPING,
                    false));
        });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext(), projectSide))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1, VARCHAR);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2, VARCHAR);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1, VARCHAR);
                        Symbol columnB2Symbol = p.symbol(COLUMN_B2, VARCHAR);
                        Symbol callSymbolA = p.symbol(CALL_1, VARCHAR);
                        Symbol callSymbolB = p.symbol(projectSide.equals(ProjectSide.BOTH) ? CALL_2 : CALL_1, VARCHAR);
                        TableScanNode leftScan = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode rightScan = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                ImmutableMap.of(
                                        columnB1Symbol, COLUMN_B1_HANDLE,
                                        columnB2Symbol, COLUMN_B2_HANDLE));
                        ProjectNode leftProject = p.project(
                                Assignments.builder()
                                        .putIdentity(columnA1Symbol)
                                        .putIdentity(columnA2Symbol)
                                        .put(callSymbolA, upperCall(ImmutableList.of(columnA1Symbol.toSymbolReference())))
                                        .build(),
                                leftScan);
                        ProjectNode rightProject = p.project(
                                Assignments.builder()
                                        .putIdentity(columnB1Symbol)
                                        .putIdentity(columnB2Symbol)
                                        .put(callSymbolB, upperCall(ImmutableList.of(columnB1Symbol.toSymbolReference())))
                                        .build(),
                                rightScan);

                        if (filterComparisonOperator.isEmpty()) {
                            return switch (projectSide) {
                                case LEFT -> p.join(
                                        joinType,
                                        leftProject,
                                        rightScan,
                                        ImmutableList.of(new EquiJoinClause(callSymbolA, columnB1Symbol)),
                                        ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                        ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                        Optional.empty());
                                case RIGHT -> p.join(
                                        joinType,
                                        leftScan,
                                        rightProject,
                                        ImmutableList.of(new EquiJoinClause(columnA1Symbol, callSymbolB)),
                                        ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                        ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                        Optional.empty());
                                case BOTH -> p.join(
                                        joinType,
                                        leftProject,
                                        rightProject,
                                        ImmutableList.of(new EquiJoinClause(callSymbolA, callSymbolB)),
                                        ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                        ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                        Optional.empty());
                                case NONE -> p.join(
                                        joinType,
                                        leftScan,
                                        rightScan,
                                        ImmutableList.of(new EquiJoinClause(columnA1Symbol, columnB1Symbol)),
                                        ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                        ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                        Optional.empty());
                            };
                        }
                        return switch (projectSide) {
                            case LEFT -> p.join(
                                    joinType,
                                    leftProject,
                                    rightScan,
                                    ImmutableList.of(new EquiJoinClause(callSymbolA, columnB1Symbol)),
                                    ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                    ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                    Optional.of(new Comparison(filterComparisonOperator.get(), callSymbolA.toSymbolReference(), columnB1Symbol.toSymbolReference())));
                            case RIGHT -> p.join(
                                    joinType,
                                    leftScan,
                                    rightProject,
                                    ImmutableList.of(new EquiJoinClause(columnA1Symbol, callSymbolB)),
                                    ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                    ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                    Optional.of(new Comparison(filterComparisonOperator.get(), columnA1Symbol.toSymbolReference(), callSymbolB.toSymbolReference())));
                            case BOTH -> p.join(
                                    joinType,
                                    leftProject,
                                    rightProject,
                                    ImmutableList.of(new EquiJoinClause(callSymbolA, callSymbolB)),
                                    ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                    ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                    Optional.of(new Comparison(filterComparisonOperator.get(), callSymbolA.toSymbolReference(), callSymbolB.toSymbolReference())));
                            case NONE -> p.join(
                                    joinType,
                                    leftScan,
                                    rightScan,
                                    ImmutableList.of(new EquiJoinClause(columnA1Symbol, columnB1Symbol)),
                                    ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                    ImmutableList.of(columnB1Symbol, columnB2Symbol),
                                    Optional.of(new Comparison(filterComparisonOperator.get(), columnA1Symbol.toSymbolReference(), columnB1Symbol.toSymbolReference())));
                        };
                    })
                    .matches(
                            project(
                                    tableScan(JOIN_PUSHDOWN_SCHEMA_TABLE_NAME.getTableName())));
        }
    }

    private static io.trino.sql.ir.Call upperCall(ImmutableList<Expression> arguments)
    {
        return new io.trino.sql.ir.Call(UPPER_VARCHAR, arguments);
    }

    public static Stream<Arguments> testPushJoinIntoTableScanParams()
    {
        return Stream.of(
                Arguments.of(INNER, Optional.empty(), ProjectSide.LEFT),
                Arguments.of(INNER, Optional.of(EQUAL), ProjectSide.LEFT),
                Arguments.of(INNER, Optional.of(Operator.LESS_THAN), ProjectSide.LEFT),
                Arguments.of(INNER, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.LEFT),
                Arguments.of(INNER, Optional.of(Operator.GREATER_THAN), ProjectSide.LEFT),
                Arguments.of(INNER, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.LEFT),
                Arguments.of(INNER, Optional.of(Operator.NOT_EQUAL), ProjectSide.LEFT),
                Arguments.of(INNER, Optional.of(Operator.IDENTICAL), ProjectSide.LEFT),

                Arguments.of(LEFT, Optional.empty(), ProjectSide.LEFT),
                Arguments.of(LEFT, Optional.of(EQUAL), ProjectSide.LEFT),
                Arguments.of(LEFT, Optional.of(Operator.LESS_THAN), ProjectSide.LEFT),
                Arguments.of(LEFT, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.LEFT),
                Arguments.of(LEFT, Optional.of(Operator.GREATER_THAN), ProjectSide.LEFT),
                Arguments.of(LEFT, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.LEFT),
                Arguments.of(LEFT, Optional.of(Operator.NOT_EQUAL), ProjectSide.LEFT),
                Arguments.of(LEFT, Optional.of(Operator.IDENTICAL), ProjectSide.LEFT),

                Arguments.of(RIGHT, Optional.empty(), ProjectSide.LEFT),
                Arguments.of(RIGHT, Optional.of(EQUAL), ProjectSide.LEFT),
                Arguments.of(RIGHT, Optional.of(Operator.LESS_THAN), ProjectSide.LEFT),
                Arguments.of(RIGHT, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.LEFT),
                Arguments.of(RIGHT, Optional.of(Operator.GREATER_THAN), ProjectSide.LEFT),
                Arguments.of(RIGHT, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.LEFT),
                Arguments.of(RIGHT, Optional.of(Operator.NOT_EQUAL), ProjectSide.LEFT),
                Arguments.of(RIGHT, Optional.of(Operator.IDENTICAL), ProjectSide.LEFT),

                Arguments.of(FULL, Optional.empty(), ProjectSide.LEFT),
                Arguments.of(FULL, Optional.of(EQUAL), ProjectSide.LEFT),
                Arguments.of(FULL, Optional.of(Operator.LESS_THAN), ProjectSide.LEFT),
                Arguments.of(FULL, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.LEFT),
                Arguments.of(FULL, Optional.of(Operator.GREATER_THAN), ProjectSide.LEFT),
                Arguments.of(FULL, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.LEFT),
                Arguments.of(FULL, Optional.of(Operator.NOT_EQUAL), ProjectSide.LEFT),
                Arguments.of(FULL, Optional.of(Operator.IDENTICAL), ProjectSide.LEFT),

                Arguments.of(INNER, Optional.empty(), ProjectSide.RIGHT),
                Arguments.of(INNER, Optional.of(EQUAL), ProjectSide.RIGHT),
                Arguments.of(INNER, Optional.of(Operator.LESS_THAN), ProjectSide.RIGHT),
                Arguments.of(INNER, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.RIGHT),
                Arguments.of(INNER, Optional.of(Operator.GREATER_THAN), ProjectSide.RIGHT),
                Arguments.of(INNER, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.RIGHT),
                Arguments.of(INNER, Optional.of(Operator.NOT_EQUAL), ProjectSide.RIGHT),
                Arguments.of(INNER, Optional.of(Operator.IDENTICAL), ProjectSide.RIGHT),

                Arguments.of(LEFT, Optional.empty(), ProjectSide.RIGHT),
                Arguments.of(LEFT, Optional.of(EQUAL), ProjectSide.RIGHT),
                Arguments.of(LEFT, Optional.of(Operator.LESS_THAN), ProjectSide.RIGHT),
                Arguments.of(LEFT, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.RIGHT),
                Arguments.of(LEFT, Optional.of(Operator.GREATER_THAN), ProjectSide.RIGHT),
                Arguments.of(LEFT, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.RIGHT),
                Arguments.of(LEFT, Optional.of(Operator.NOT_EQUAL), ProjectSide.RIGHT),
                Arguments.of(LEFT, Optional.of(Operator.IDENTICAL), ProjectSide.RIGHT),

                Arguments.of(RIGHT, Optional.empty(), ProjectSide.RIGHT),
                Arguments.of(RIGHT, Optional.of(EQUAL), ProjectSide.RIGHT),
                Arguments.of(RIGHT, Optional.of(Operator.LESS_THAN), ProjectSide.RIGHT),
                Arguments.of(RIGHT, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.RIGHT),
                Arguments.of(RIGHT, Optional.of(Operator.GREATER_THAN), ProjectSide.RIGHT),
                Arguments.of(RIGHT, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.RIGHT),
                Arguments.of(RIGHT, Optional.of(Operator.NOT_EQUAL), ProjectSide.RIGHT),
                Arguments.of(RIGHT, Optional.of(Operator.IDENTICAL), ProjectSide.RIGHT),

                Arguments.of(FULL, Optional.empty(), ProjectSide.RIGHT),
                Arguments.of(FULL, Optional.of(EQUAL), ProjectSide.RIGHT),
                Arguments.of(FULL, Optional.of(Operator.LESS_THAN), ProjectSide.RIGHT),
                Arguments.of(FULL, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.RIGHT),
                Arguments.of(FULL, Optional.of(Operator.GREATER_THAN), ProjectSide.RIGHT),
                Arguments.of(FULL, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.RIGHT),
                Arguments.of(FULL, Optional.of(Operator.NOT_EQUAL), ProjectSide.RIGHT),
                Arguments.of(FULL, Optional.of(Operator.IDENTICAL), ProjectSide.RIGHT),

                Arguments.of(INNER, Optional.empty(), ProjectSide.BOTH),
                Arguments.of(INNER, Optional.of(EQUAL), ProjectSide.BOTH),
                Arguments.of(INNER, Optional.of(Operator.LESS_THAN), ProjectSide.BOTH),
                Arguments.of(INNER, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.BOTH),
                Arguments.of(INNER, Optional.of(Operator.GREATER_THAN), ProjectSide.BOTH),
                Arguments.of(INNER, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.BOTH),
                Arguments.of(INNER, Optional.of(Operator.NOT_EQUAL), ProjectSide.BOTH),
                Arguments.of(INNER, Optional.of(Operator.IDENTICAL), ProjectSide.BOTH),

                Arguments.of(LEFT, Optional.empty(), ProjectSide.BOTH),
                Arguments.of(LEFT, Optional.of(EQUAL), ProjectSide.BOTH),
                Arguments.of(LEFT, Optional.of(Operator.LESS_THAN), ProjectSide.BOTH),
                Arguments.of(LEFT, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.BOTH),
                Arguments.of(LEFT, Optional.of(Operator.GREATER_THAN), ProjectSide.BOTH),
                Arguments.of(LEFT, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.BOTH),
                Arguments.of(LEFT, Optional.of(Operator.NOT_EQUAL), ProjectSide.BOTH),
                Arguments.of(LEFT, Optional.of(Operator.IDENTICAL), ProjectSide.BOTH),

                Arguments.of(RIGHT, Optional.empty(), ProjectSide.BOTH),
                Arguments.of(RIGHT, Optional.of(EQUAL), ProjectSide.BOTH),
                Arguments.of(RIGHT, Optional.of(Operator.LESS_THAN), ProjectSide.BOTH),
                Arguments.of(RIGHT, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.BOTH),
                Arguments.of(RIGHT, Optional.of(Operator.GREATER_THAN), ProjectSide.BOTH),
                Arguments.of(RIGHT, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.BOTH),
                Arguments.of(RIGHT, Optional.of(Operator.NOT_EQUAL), ProjectSide.BOTH),
                Arguments.of(RIGHT, Optional.of(Operator.IDENTICAL), ProjectSide.BOTH),

                Arguments.of(FULL, Optional.empty(), ProjectSide.BOTH),
                Arguments.of(FULL, Optional.of(EQUAL), ProjectSide.BOTH),
                Arguments.of(FULL, Optional.of(Operator.LESS_THAN), ProjectSide.BOTH),
                Arguments.of(FULL, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.BOTH),
                Arguments.of(FULL, Optional.of(Operator.GREATER_THAN), ProjectSide.BOTH),
                Arguments.of(FULL, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.BOTH),
                Arguments.of(FULL, Optional.of(Operator.NOT_EQUAL), ProjectSide.BOTH),
                Arguments.of(FULL, Optional.of(Operator.IDENTICAL), ProjectSide.BOTH),

                Arguments.of(INNER, Optional.empty(), ProjectSide.NONE),
                Arguments.of(INNER, Optional.of(EQUAL), ProjectSide.NONE),
                Arguments.of(INNER, Optional.of(Operator.LESS_THAN), ProjectSide.NONE),
                Arguments.of(INNER, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.NONE),
                Arguments.of(INNER, Optional.of(Operator.GREATER_THAN), ProjectSide.NONE),
                Arguments.of(INNER, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.NONE),
                Arguments.of(INNER, Optional.of(Operator.NOT_EQUAL), ProjectSide.NONE),
                Arguments.of(INNER, Optional.of(Operator.IDENTICAL), ProjectSide.NONE),

                Arguments.of(LEFT, Optional.empty(), ProjectSide.NONE),
                Arguments.of(LEFT, Optional.of(EQUAL), ProjectSide.NONE),
                Arguments.of(LEFT, Optional.of(Operator.LESS_THAN), ProjectSide.NONE),
                Arguments.of(LEFT, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.NONE),
                Arguments.of(LEFT, Optional.of(Operator.GREATER_THAN), ProjectSide.NONE),
                Arguments.of(LEFT, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.NONE),
                Arguments.of(LEFT, Optional.of(Operator.NOT_EQUAL), ProjectSide.NONE),
                Arguments.of(LEFT, Optional.of(Operator.IDENTICAL), ProjectSide.NONE),

                Arguments.of(RIGHT, Optional.empty(), ProjectSide.NONE),
                Arguments.of(RIGHT, Optional.of(EQUAL), ProjectSide.NONE),
                Arguments.of(RIGHT, Optional.of(Operator.LESS_THAN), ProjectSide.NONE),
                Arguments.of(RIGHT, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.NONE),
                Arguments.of(RIGHT, Optional.of(Operator.GREATER_THAN), ProjectSide.NONE),
                Arguments.of(RIGHT, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.NONE),
                Arguments.of(RIGHT, Optional.of(Operator.NOT_EQUAL), ProjectSide.NONE),
                Arguments.of(RIGHT, Optional.of(Operator.IDENTICAL), ProjectSide.NONE),

                Arguments.of(FULL, Optional.empty(), ProjectSide.NONE),
                Arguments.of(FULL, Optional.of(EQUAL), ProjectSide.NONE),
                Arguments.of(FULL, Optional.of(Operator.LESS_THAN), ProjectSide.NONE),
                Arguments.of(FULL, Optional.of(Operator.LESS_THAN_OR_EQUAL), ProjectSide.NONE),
                Arguments.of(FULL, Optional.of(Operator.GREATER_THAN), ProjectSide.NONE),
                Arguments.of(FULL, Optional.of(Operator.GREATER_THAN_OR_EQUAL), ProjectSide.NONE),
                Arguments.of(FULL, Optional.of(Operator.NOT_EQUAL), ProjectSide.NONE),
                Arguments.of(FULL, Optional.of(Operator.IDENTICAL), ProjectSide.NONE));
    }

    @Test
    public void testDoesNotFireForDifferentCatalogs()
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    throw new IllegalStateException("applyJoin should not be called!");
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.getPlanTester().createCatalog("another_catalog", "mock", ImmutableMap.of());
            TableHandle tableBHandleAnotherCatalog = createTableHandle(
                    new MockConnectorTableHandle(new SchemaTableName(SCHEMA, TABLE_B)),
                    createTestCatalogHandle("another_catalog"));

            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext(), ProjectSide.LEFT))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1, VARCHAR);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2, VARCHAR);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1, VARCHAR);
                        Symbol callSymbolA = p.symbol(CALL_1, VARCHAR);

                        TableScanNode leftScan = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode rightScan = p.tableScan(
                                tableBHandleAnotherCatalog,
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE));

                        ProjectNode leftProject = p.project(
                                Assignments.builder()
                                        .putIdentity(columnB1Symbol)
                                        .put(callSymbolA, upperCall(ImmutableList.of(columnA1Symbol.toSymbolReference())))
                                        .build(),
                                leftScan);

                        return p.join(
                                INNER,
                                leftProject,
                                rightScan,
                                new EquiJoinClause(callSymbolA, columnB1Symbol));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        Session joinPushDownDisabledSession = Session.builder(MOCK_SESSION)
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();

        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    throw new IllegalStateException("applyJoin should not be called!");
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext(), ProjectSide.LEFT))
                    .withSession(joinPushDownDisabledSession)
                    .on(p -> genericJoinLeftProject(p, ruleTester))
                    .doesNotFire();
        }
    }

    private static JoinNode genericJoinLeftProject(PlanBuilder planBuilder, RuleTester ruleTester)
    {
        return genericJoinLeftProject(planBuilder, ruleTester, INNER, TupleDomain.all(), TupleDomain.all());
    }

    private static JoinNode genericJoinLeftProject(
            PlanBuilder planBuilder,
            RuleTester ruleTester,
            JoinType joinType,
            TupleDomain<ColumnHandle> leftConstraint,
            TupleDomain<ColumnHandle> rightConstraint)
    {
        Symbol columnA1Symbol = planBuilder.symbol(COLUMN_A1, VARCHAR);
        Symbol columnA2Symbol = planBuilder.symbol(COLUMN_A2, VARCHAR);
        Symbol columnB1Symbol = planBuilder.symbol(COLUMN_B1, VARCHAR);
        Symbol callSymbolA = planBuilder.symbol(CALL_1, VARCHAR);

        TableScanNode leftScan = planBuilder.tableScan(
                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                ImmutableMap.of(
                        columnA1Symbol, COLUMN_A1_HANDLE,
                        columnA2Symbol, COLUMN_A2_HANDLE),
                leftConstraint);
        TableScanNode rightScan = planBuilder.tableScan(
                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                ImmutableList.of(columnB1Symbol),
                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE),
                rightConstraint);
        ProjectNode leftProject = planBuilder.project(
                Assignments.builder()
                        .putIdentity(columnB1Symbol)
                        .put(callSymbolA, upperCall(ImmutableList.of(columnA1Symbol.toSymbolReference())))
                        .build(),
                leftScan);

        return planBuilder.join(
                joinType,
                leftProject,
                rightScan,
                new EquiJoinClause(callSymbolA, columnB1Symbol));
    }

    @Test
    public void testDoesNotFireWhenAllPushdownsDisabled()
    {
        Session allPushdownsDisabledSession = Session.builder(MOCK_SESSION)
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();

        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    throw new IllegalStateException("applyJoin should not be called!");
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext(), ProjectSide.LEFT))
                    .withSession(allPushdownsDisabledSession)
                    .on(p -> genericJoinLeftProject(p, ruleTester))
                    .doesNotFire();
        }
    }

    @ParameterizedTest
    @MethodSource("testPreservesEnforcedConstraintParams")
    public void testPreservesEnforcedConstraint(
            JoinType joinType,
            TupleDomain<ColumnHandle> leftConstraint,
            TupleDomain<ColumnHandle> rightConstraint,
            TupleDomain<Predicate<ColumnHandle>> expectedConstraint)
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory((session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> Optional.of(
                new JoinApplicationResult<>(
                        JOIN_CONNECTOR_TABLE_HANDLE,
                        JOIN_TABLE_A_COLUMN_MAPPING,
                        JOIN_TABLE_B_COLUMN_MAPPING,
                        false)));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext(), ProjectSide.LEFT))
                    .withSession(MOCK_SESSION)
                    .on(p -> genericJoinLeftProject(p, ruleTester, joinType, leftConstraint, rightConstraint))
                    .matches(
                            project(
                                    tableScan(
                                            tableHandle -> JOIN_PUSHDOWN_SCHEMA_TABLE_NAME.equals(((MockConnectorTableHandle) tableHandle).getTableName()),
                                            expectedConstraint,
                                            ImmutableMap.of())));
        }
    }

    public static Stream<Arguments> testPreservesEnforcedConstraintParams()
    {
        Domain columnA1Domain = Domain.multipleValues(VARCHAR, List.of(utf8Slice("3L")));
        Domain columnA2Domain = Domain.multipleValues(VARCHAR, List.of(utf8Slice("10L"), utf8Slice("20L")));
        Domain columnB1Domain = Domain.multipleValues(VARCHAR, List.of(utf8Slice("30L"), utf8Slice("40L")));
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
                                equalTo(JOIN_COLUMN_A1_HANDLE), columnA1Domain.union(onlyNull(VARCHAR)),
                                equalTo(JOIN_COLUMN_A2_HANDLE), columnA2Domain.union(onlyNull(VARCHAR)),
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
                                equalTo(JOIN_COLUMN_B1_HANDLE), columnB1Domain.union(onlyNull(VARCHAR))))),
                Arguments.of(
                        FULL,
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_A1_HANDLE, columnA1Domain,
                                COLUMN_A2_HANDLE, columnA2Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                COLUMN_B1_HANDLE, columnB1Domain)),
                        TupleDomain.withColumnDomains(Map.of(
                                equalTo(JOIN_COLUMN_A1_HANDLE), columnA1Domain.union(onlyNull(VARCHAR)),
                                equalTo(JOIN_COLUMN_A2_HANDLE), columnA2Domain.union(onlyNull(VARCHAR)),
                                equalTo(JOIN_COLUMN_B1_HANDLE), columnB1Domain.union(onlyNull(VARCHAR))))));
    }

    @Test
    public void testDoesNotFireForCrossJoin()
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory(
                (session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> {
                    throw new IllegalStateException("applyJoin should not be called!");
                });
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext(), ProjectSide.LEFT))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol columnA1Symbol = p.symbol(COLUMN_A1, VARCHAR);
                        Symbol columnA2Symbol = p.symbol(COLUMN_A2, VARCHAR);
                        Symbol columnB1Symbol = p.symbol(COLUMN_B1, VARCHAR);
                        Symbol callSymbolA = p.symbol(CALL_1, VARCHAR);

                        TableScanNode leftScan = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                ImmutableList.of(columnA1Symbol, columnA2Symbol),
                                ImmutableMap.of(
                                        columnA1Symbol, COLUMN_A1_HANDLE,
                                        columnA2Symbol, COLUMN_A2_HANDLE));
                        TableScanNode rightScan = p.tableScan(
                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                ImmutableList.of(columnB1Symbol),
                                ImmutableMap.of(columnB1Symbol, COLUMN_B1_HANDLE));
                        ProjectNode leftProject = p.project(
                                Assignments.builder()
                                        .putIdentity(columnB1Symbol)
                                        .put(callSymbolA, upperCall(ImmutableList.of(columnA1Symbol.toSymbolReference())))
                                        .build(),
                                leftScan);

                        // cross-join - no criteria
                        return p.join(
                                INNER,
                                leftProject,
                                rightScan);
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testRequiresFullColumnHandleMappingInResult()
    {
        MockConnectorFactory connectorFactory = createMockConnectorFactory((session, applyJoinType, left, right, joinConditions, leftAssignments, rightAssignments) -> Optional.of(
                new JoinApplicationResult<>(
                        JOIN_CONNECTOR_TABLE_HANDLE,
                        ImmutableMap.of(COLUMN_A1_HANDLE, JOIN_COLUMN_A1_HANDLE, COLUMN_A2_HANDLE, JOIN_COLUMN_A2_HANDLE),
                        // mapping for COLUMN_B1_HANDLE is missing
                        ImmutableMap.of(),
                        false)));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            assertThatThrownBy(() -> ruleTester.assertThat(new PushJoinIntoTableScan(ruleTester.getPlannerContext(), ProjectSide.LEFT))
                    .withSession(MOCK_SESSION)
                    .on(p -> genericJoinLeftProject(
                            p,
                            ruleTester,
                            INNER,
                            TupleDomain.fromFixedValues(ImmutableMap.of(COLUMN_A2_HANDLE, NullableValue.of(VARCHAR, utf8Slice("44L")))),
                            TupleDomain.fromFixedValues(ImmutableMap.of(COLUMN_B1_HANDLE, NullableValue.of(VARCHAR, utf8Slice("45L"))))))
                    .matches(anyTree()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Column handle mappings do not match old column handles");
        }
    }

    private static TableHandle createTableHandle(ConnectorTableHandle tableHandle, CatalogHandle catalogHandle)
    {
        return new TableHandle(
                catalogHandle,
                tableHandle,
                new ConnectorTransactionHandle() {});
    }

    private MockConnectorFactory createMockConnectorFactory(ApplyJoin applyJoin)
    {
        return MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of(SCHEMA))
                .withListTables((connectorSession, schema) -> SCHEMA.equals(schema) ? ImmutableList.of(
                        TABLE_A_SCHEMA_TABLE_NAME.getTableName(),
                        TABLE_B_SCHEMA_TABLE_NAME.getTableName()) : ImmutableList.of())
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

    private io.trino.spi.connector.JoinType toSpiJoinType(JoinType joinType)
    {
        return switch (joinType) {
            case INNER -> io.trino.spi.connector.JoinType.INNER;
            case LEFT -> io.trino.spi.connector.JoinType.LEFT_OUTER;
            case RIGHT -> io.trino.spi.connector.JoinType.RIGHT_OUTER;
            case FULL -> io.trino.spi.connector.JoinType.FULL_OUTER;
        };
    }

    private FunctionName toSpiFunctionName(Operator operator)
    {
        return switch (operator) {
            case EQUAL -> new FunctionName("$equal");
            case NOT_EQUAL -> new FunctionName("$not_equal");
            case LESS_THAN -> new FunctionName("$less_than");
            case LESS_THAN_OR_EQUAL -> new FunctionName("$less_than_or_equal");
            case GREATER_THAN -> new FunctionName("$greater_than");
            case GREATER_THAN_OR_EQUAL -> new FunctionName("$greater_than_or_equal");
            case IDENTICAL -> new FunctionName("$identical");
        };
    }
}
