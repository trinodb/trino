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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.ConnectorExpressionTranslator.translate;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPushProjectionIntoTableScan
{
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));

    private static final ConnectorPartitioningHandle PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(TEST_CATALOG_NAME).setSchema(TEST_SCHEMA).build();

    @Test
    public void testDoesNotFire()
    {
        String columnName = "input_column";
        Type columnType = ROW_TYPE;
        ColumnHandle inputColumnHandle = column(columnName, columnType);

        MockConnectorFactory factory = createMockFactory(ImmutableMap.of(columnName, inputColumnHandle), Optional.empty());
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(factory).build()) {
            PushProjectionIntoTableScan optimizer = createRule(ruleTester);

            ruleTester.assertThat(optimizer)
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol symbol = p.symbol(columnName, columnType);
                        return p.project(
                                Assignments.of(p.symbol("symbol_dereference", BIGINT), new FieldReference(symbol.toSymbolReference(), 0)),
                                p.tableScan(
                                        ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                        ImmutableList.of(symbol),
                                        ImmutableMap.of(symbol, inputColumnHandle)));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testPushProjection()
    {
        // Building context for input
        String columnName = "col0";
        Type columnType = ROW_TYPE;
        Symbol baseColumn = new Symbol(columnType, columnName);
        ColumnHandle columnHandle = new TpchColumnHandle(columnName, columnType);

        // Create catalog with applyProjection enabled
        MockConnectorFactory factory = createMockFactory(ImmutableMap.of(columnName, columnHandle), Optional.of(this::mockApplyProjection));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(factory).build()) {
            // Prepare project node symbols and types
            Symbol identity = new Symbol(ROW_TYPE, "symbol_identity");
            Symbol dereference = new Symbol(BIGINT, "symbol_dereference");
            Symbol constant = new Symbol(BIGINT, "symbol_constant");
            ImmutableMap<Symbol, Type> types = ImmutableMap.of(
                    baseColumn, ROW_TYPE,
                    identity, ROW_TYPE,
                    dereference, BIGINT,
                    constant, BIGINT);

            // Prepare project node assignments
            Assignments inputProjections = Assignments.builder()
                    .put(identity, baseColumn.toSymbolReference())
                    .put(dereference, new FieldReference(baseColumn.toSymbolReference(), 0))
                    .put(constant, new Constant(BIGINT, 5L))
                    .build();

            // Compute expected symbols after applyProjection
            TransactionId transactionId = ruleTester.getPlanTester().getTransactionManager().beginTransaction(false);
            Session session = MOCK_SESSION.beginTransactionId(transactionId, ruleTester.getPlanTester().getTransactionManager(), ruleTester.getPlanTester().getAccessControl());
            ImmutableMap<Symbol, String> connectorNames = inputProjections.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, e -> translate(session, e.getValue()).get().toString()));
            ImmutableMap<Symbol, String> newNames = ImmutableMap.of(
                    identity, "projected_variable_" + connectorNames.get(identity),
                    dereference, "projected_dereference_" + connectorNames.get(dereference));
            ImmutableMap<Symbol, Expression> constants = ImmutableMap.of(
                    constant, requireNonNull(inputProjections.get(constant)));
            Map<String, ColumnHandle> expectedColumns = newNames.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getValue,
                            e -> column(e.getValue(), types.get(e.getKey()))));

            ruleTester.assertThat(createRule(ruleTester))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        // Register symbols
                        types.forEach((symbol, type) -> p.symbol(symbol.name(), type));

                        return p.project(
                                inputProjections,
                                p.tableScan(tableScan -> tableScan
                                        .setTableHandle(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE))
                                        .setSymbols(ImmutableList.copyOf(types.keySet()))
                                        .setAssignments(types.keySet().stream()
                                                .collect(Collectors.toMap(Function.identity(), v -> columnHandle)))
                                        .setStatistics(Optional.of(PlanNodeStatsEstimate.builder()
                                                .setOutputRowCount(42)
                                                .addSymbolStatistics(baseColumn, SymbolStatsEstimate.builder().setNullsFraction(0).setDistinctValuesCount(33).build())
                                                .build()))));
                    })
                    .matches(project(
                            Stream.concat(newNames.entrySet().stream(), constants.entrySet().stream())
                                    .collect(toImmutableMap(
                                            e -> e.getKey().name(),
                                            e -> {
                                                if (e.getValue() instanceof String value) {
                                                    return expression(new Reference(BIGINT, value));
                                                }
                                                if (e.getValue() instanceof Expression value) {
                                                    return expression(value);
                                                }
                                                throw new IllegalArgumentException("Unexpected value type: " + e.getValue().getClass().getName());
                                            })),
                            tableScan(
                                    new MockConnectorTableHandle(
                                            new SchemaTableName(TEST_SCHEMA, "projected_" + TEST_TABLE),
                                            TupleDomain.all(),
                                            Optional.of(ImmutableList.copyOf(expectedColumns.values())))::equals,
                                    TupleDomain.all(),
                                    expectedColumns.entrySet().stream()
                                            .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue()::equals)))));
        }
    }

    @Test
    public void testPartitioningChanged()
    {
        String columnName = "col0";
        ColumnHandle columnHandle = new TpchColumnHandle(columnName, VARCHAR);

        // Create catalog with applyProjection enabled
        MockConnectorFactory factory = createMockFactory(ImmutableMap.of(columnName, columnHandle), Optional.of(this::mockApplyProjection));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(factory).build()) {
            assertThatThrownBy(() -> ruleTester.assertThat(createRule(ruleTester))
                    .withSession(MOCK_SESSION)
                    // projection pushdown results in different table handle without partitioning
                    .on(p -> p.project(
                            Assignments.of(),
                            p.tableScan(
                                    ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                    ImmutableList.of(p.symbol("col", VARCHAR)),
                                    ImmutableMap.of(p.symbol("col", VARCHAR), columnHandle),
                                    Optional.of(true))))
                    .matches(anyTree()))
                    .hasMessage("Partitioning must not change after projection is pushed down");
        }
    }

    private MockConnectorFactory createMockFactory(Map<String, ColumnHandle> assignments, Optional<MockConnectorFactory.ApplyProjection> applyProjection)
    {
        List<ColumnMetadata> metadata = assignments.entrySet().stream()
                .map(entry -> new ColumnMetadata(entry.getKey(), ((TpchColumnHandle) entry.getValue()).type()))
                .collect(toImmutableList());

        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of(TEST_SCHEMA))
                .withListTables((connectorSession, schema) -> TEST_SCHEMA.equals(schema) ? ImmutableList.of(TEST_TABLE) : ImmutableList.of())
                .withGetColumns(schemaTableName -> metadata)
                .withGetTableProperties((session, tableHandle) -> {
                    MockConnectorTableHandle mockTableHandle = (MockConnectorTableHandle) tableHandle;
                    if (mockTableHandle.getTableName().getTableName().equals(TEST_TABLE)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.of(new ConnectorTablePartitioning(PARTITIONING_HANDLE, ImmutableList.of(column("col", VARCHAR)))),
                                Optional.empty(),
                                ImmutableList.of());
                    }

                    return new ConnectorTableProperties();
                });

        if (applyProjection.isPresent()) {
            builder = builder.withApplyProjection(applyProjection.get());
        }

        return builder.build();
    }

    private Optional<ProjectionApplicationResult<ConnectorTableHandle>> mockApplyProjection(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        // Prepare new table handle
        SchemaTableName inputSchemaTableName = ((MockConnectorTableHandle) tableHandle).getTableName();
        SchemaTableName projectedTableName = new SchemaTableName(
                inputSchemaTableName.getSchemaName(),
                "projected_" + inputSchemaTableName.getTableName());

        // Prepare new column handles
        ImmutableList.Builder<ConnectorExpression> outputExpressions = ImmutableList.builder();
        ImmutableList.Builder<Assignment> outputAssignments = ImmutableList.builder();
        ImmutableList.Builder<ColumnHandle> projectedColumnsBuilder = ImmutableList.builder();

        for (ConnectorExpression projection : projections) {
            String variablePrefix;
            if (projection instanceof Variable) {
                variablePrefix = "projected_variable_";
            }
            else if (projection instanceof FieldDereference) {
                variablePrefix = "projected_dereference_";
            }
            else if (projection instanceof Call) {
                variablePrefix = "projected_call_";
            }
            else if (projection instanceof io.trino.spi.expression.Constant) {
                throw new UnsupportedOperationException("constant expression should not be pushed to the connector");
            }
            else {
                throw new UnsupportedOperationException();
            }

            String newVariableName = variablePrefix + projection.toString();
            Variable newVariable = new Variable(newVariableName, projection.getType());
            ColumnHandle newColumnHandle = new TpchColumnHandle(newVariableName, projection.getType());
            outputExpressions.add(newVariable);
            outputAssignments.add(new Assignment(newVariableName, newColumnHandle, projection.getType()));
            projectedColumnsBuilder.add(newColumnHandle);
        }

        return Optional.of(new ProjectionApplicationResult<>(
                new MockConnectorTableHandle(projectedTableName, TupleDomain.all(), Optional.of(projectedColumnsBuilder.build())),
                outputExpressions.build(),
                outputAssignments.build(),
                false));
    }

    private static PushProjectionIntoTableScan createRule(RuleTester tester)
    {
        PlannerContext plannerContext = tester.getPlannerContext();
        return new PushProjectionIntoTableScan(
                plannerContext,
                new ScalarStatsCalculator(plannerContext));
    }

    private static ColumnHandle column(String name, Type type)
    {
        return new TpchColumnHandle(name, type);
    }
}
