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
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.ConnectorExpressionTranslator.translate;
import static io.trino.sql.planner.TypeProvider.viewOf;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPushProjectionIntoTableScan
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName TEST_SCHEMA_TABLE = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));

    private static final TableHandle TEST_TABLE_HANDLE = createTableHandle(TEST_SCHEMA, TEST_TABLE);
    private static final ConnectorPartitioningHandle PARTITIONING_HANDLE = new ConnectorPartitioningHandle() {};

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    @Test
    public void testDoesNotFire()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            String columnName = "input_column";
            Type columnType = ROW_TYPE;
            ColumnHandle inputColumnHandle = column(columnName, columnType);

            MockConnectorFactory factory = createMockFactory(ImmutableMap.of(columnName, inputColumnHandle), Optional.empty());

            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, factory, ImmutableMap.of());

            PushProjectionIntoTableScan optimizer = createRule(ruleTester);

            ruleTester.assertThat(optimizer)
                    .on(p -> {
                        Symbol symbol = p.symbol(columnName, columnType);
                        return p.project(
                                Assignments.of(p.symbol("symbol_dereference", BIGINT), new SubscriptExpression(symbol.toSymbolReference(), new LongLiteral("1"))),
                                p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(symbol), ImmutableMap.of(symbol, inputColumnHandle)));
                    })
                    .withSession(MOCK_SESSION)
                    .doesNotFire();
        }
    }

    @Test
    public void testPushProjection()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            // Building context for input
            String columnName = "col0";
            Type columnType = ROW_TYPE;
            Symbol baseColumn = new Symbol(columnName);
            ColumnHandle columnHandle = new TpchColumnHandle(columnName, columnType);

            // Create catalog with applyProjection enabled
            MockConnectorFactory factory = createMockFactory(ImmutableMap.of(columnName, columnHandle), Optional.of(this::mockApplyProjection));
            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, factory, ImmutableMap.of());

            TypeAnalyzer typeAnalyzer = new TypeAnalyzer(new SqlParser(), ruleTester.getMetadata());

            // Prepare project node symbols and types
            Symbol identity = new Symbol("symbol_identity");
            Symbol dereference = new Symbol("symbol_dereference");
            Symbol constant = new Symbol("symbol_constant");
            ImmutableMap<Symbol, Type> types = ImmutableMap.of(
                    baseColumn, ROW_TYPE,
                    identity, ROW_TYPE,
                    dereference, BIGINT,
                    constant, BIGINT);

            // Prepare project node assignments
            ImmutableMap<Symbol, Expression> inputProjections = ImmutableMap.of(
                    identity, baseColumn.toSymbolReference(),
                    dereference, new SubscriptExpression(baseColumn.toSymbolReference(), new LongLiteral("1")),
                    constant, new LongLiteral("5"));

            // Compute expected symbols after applyProjection
            ImmutableMap<Symbol, String> connectorNames = inputProjections.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, e -> translate(MOCK_SESSION, e.getValue(), typeAnalyzer, viewOf(types)).get().toString()));
            ImmutableMap<Symbol, String> newNames = ImmutableMap.of(
                    identity, "projected_variable_" + connectorNames.get(identity),
                    dereference, "projected_dereference_" + connectorNames.get(dereference),
                    constant, "projected_constant_" + connectorNames.get(constant));
            Map<String, ColumnHandle> expectedColumns = newNames.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getValue,
                            e -> column(e.getValue(), types.get(e.getKey()))));

            ruleTester.assertThat(createRule(ruleTester))
                    .on(p -> {
                        // Register symbols
                        Symbol columnSymbol = p.symbol(columnName, columnType);
                        p.symbol(identity.getName(), types.get(identity));
                        p.symbol(dereference.getName(), types.get(dereference));
                        p.symbol(constant.getName(), types.get(constant));

                        return p.project(
                                new Assignments(inputProjections),
                                p.tableScan(tableScan -> tableScan
                                        .setTableHandle(TEST_TABLE_HANDLE)
                                        .setSymbols(ImmutableList.of(columnSymbol))
                                        .setAssignments(ImmutableMap.of(columnSymbol, columnHandle))
                                        .setStatistics(Optional.of(PlanNodeStatsEstimate.builder()
                                                .setOutputRowCount(42)
                                                .addSymbolStatistics(columnSymbol, SymbolStatsEstimate.builder().setNullsFraction(0).setDistinctValuesCount(33).build())
                                                .build()))));
                    })
                    .withSession(MOCK_SESSION)
                    .matches(project(
                            newNames.entrySet().stream()
                                    .collect(toImmutableMap(
                                            e -> e.getKey().getName(),
                                            e -> expression(symbolReference(e.getValue())))),
                            tableScan(
                                    new MockConnectorTableHandle(
                                            new SchemaTableName(TEST_SCHEMA, "projected_" + TEST_TABLE),
                                            TupleDomain.all(),
                                            Optional.of(ImmutableList.copyOf(expectedColumns.values())))::equals,
                                    TupleDomain.all(),
                                    expectedColumns.entrySet().stream()
                                            .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue()::equals)),
                                    Optional.of(PlanNodeStatsEstimate.builder()
                                            .setOutputRowCount(42)
                                            .addSymbolStatistics(new Symbol(newNames.get(constant)), SymbolStatsEstimate.builder()
                                                    .setDistinctValuesCount(1)
                                                    .setNullsFraction(0)
                                                    .setLowValue(5)
                                                    .setHighValue(5)
                                                    .build())
                                            .addSymbolStatistics(new Symbol(newNames.get(identity)), SymbolStatsEstimate.builder()
                                                    .setDistinctValuesCount(33)
                                                    .setNullsFraction(0)
                                                    .build())
                                            .addSymbolStatistics(new Symbol(newNames.get(dereference)), SymbolStatsEstimate.unknown())
                                            .build())::equals)));
        }
    }

    @Test
    public void testPartitioningChanged()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            String columnName = "col0";
            ColumnHandle columnHandle = new TpchColumnHandle(columnName, VARCHAR);

            // Create catalog with applyProjection enabled
            MockConnectorFactory factory = createMockFactory(ImmutableMap.of(columnName, columnHandle), Optional.of(this::mockApplyProjection));
            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, factory, ImmutableMap.of());

            assertThatThrownBy(() -> ruleTester.assertThat(createRule(ruleTester))
                    // projection pushdown results in different table handle without partitioning
                    .on(p -> p.project(
                            Assignments.of(),
                            p.tableScan(
                                    TEST_TABLE_HANDLE,
                                    ImmutableList.of(p.symbol("col", VARCHAR)),
                                    ImmutableMap.of(p.symbol("col", VARCHAR), columnHandle),
                                    Optional.of(true))))
                    .withSession(MOCK_SESSION)
                    .matches(anyTree()))
                    .hasMessage("Partitioning must not change after projection is pushed down");
        }
    }

    private MockConnectorFactory createMockFactory(Map<String, ColumnHandle> assignments, Optional<MockConnectorFactory.ApplyProjection> applyProjection)
    {
        List<ColumnMetadata> metadata = assignments.entrySet().stream()
                .map(entry -> new ColumnMetadata(entry.getKey(), ((TpchColumnHandle) entry.getValue()).getType()))
                .collect(toImmutableList());

        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of(TEST_SCHEMA))
                .withListTables((connectorSession, schema) -> TEST_SCHEMA.equals(schema) ? ImmutableList.of(TEST_SCHEMA_TABLE) : ImmutableList.of())
                .withGetColumns(schemaTableName -> metadata)
                .withGetTableProperties((session, tableHandle) -> {
                    MockConnectorTableHandle mockTableHandle = (MockConnectorTableHandle) tableHandle;
                    if (mockTableHandle.getTableName().getTableName().equals(TEST_TABLE)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.of(new ConnectorTablePartitioning(PARTITIONING_HANDLE, ImmutableList.of(column("col", VARCHAR)))),
                                Optional.empty(),
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
            else if (projection instanceof Constant) {
                variablePrefix = "projected_constant_";
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
        Metadata metadata = tester.getMetadata();
        TypeAnalyzer typeAnalyzer = tester.getTypeAnalyzer();
        return new PushProjectionIntoTableScan(
                metadata,
                typeAnalyzer,
                new ScalarStatsCalculator(metadata, typeAnalyzer));
    }

    private static TableHandle createTableHandle(String schemaName, String tableName)
    {
        return new TableHandle(
                new CatalogName(MOCK_CATALOG),
                new MockConnectorTableHandle(new SchemaTableName(schemaName, tableName)),
                new ConnectorTransactionHandle() {},
                Optional.empty());
    }

    private static SymbolReference symbolReference(String name)
    {
        return new SymbolReference(name);
    }

    private static ColumnHandle column(String name, Type type)
    {
        return new TpchColumnHandle(name, type);
    }
}
