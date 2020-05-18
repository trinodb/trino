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
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.connector.MockConnectorFactory.MockConnectorTableHandle;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.spi.connector.Assignment;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Constant;
import io.prestosql.spi.expression.FieldDereference;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.rule.test.RuleTester;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.sql.planner.ConnectorExpressionTranslator.translate;
import static io.prestosql.sql.planner.TypeProvider.viewOf;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.asList;

public class TestPushProjectionIntoTableScan
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName TEST_SCHEMA_TABLE = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
    private static final Type ROW_TYPE = RowType.from(asList(field("a", BIGINT), field("b", BIGINT)));

    private static final TableHandle TEST_TABLE_HANDLE = createTableHandle(TEST_SCHEMA, TEST_TABLE);

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

            PushProjectionIntoTableScan optimizer = new PushProjectionIntoTableScan(
                    ruleTester.getMetadata(),
                    new TypeAnalyzer(new SqlParser(), ruleTester.getMetadata()));

            ruleTester.assertThat(optimizer)
                    .on(p -> {
                        Symbol symbol = p.symbol(columnName, columnType);
                        return p.project(
                                Assignments.of(p.symbol("symbol_dereference", BIGINT), new DereferenceExpression(symbol.toSymbolReference(), new Identifier("a"))),
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

            Metadata metadata = ruleTester.getMetadata();
            TypeAnalyzer typeAnalyzer = new TypeAnalyzer(new SqlParser(), metadata);
            PushProjectionIntoTableScan optimizer = new PushProjectionIntoTableScan(metadata, typeAnalyzer);

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
                    dereference, new DereferenceExpression(baseColumn.toSymbolReference(), new Identifier("a")),
                    constant, new LongLiteral("5"));

            // Compute expected symbols after applyProjection
            ImmutableMap<Symbol, String> connectorNames = inputProjections.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, e -> translate(MOCK_SESSION, e.getValue(), typeAnalyzer, viewOf(types)).get().toString()));
            ImmutableMap<Symbol, String> newNames = ImmutableMap.of(
                    identity, "projected_variable_" + connectorNames.get(identity),
                    dereference, "projected_dereference_" + connectorNames.get(dereference),
                    constant, "projected_constant_" + connectorNames.get(constant));

            ruleTester.assertThat(optimizer)
                    .on(p -> {
                        // Register symbols
                        Symbol columnSymbol = p.symbol(columnName, columnType);
                        p.symbol(identity.getName(), types.get(identity));
                        p.symbol(dereference.getName(), types.get(dereference));
                        p.symbol(constant.getName(), types.get(constant));

                        return p.project(
                                new Assignments(inputProjections),
                                p.tableScan(
                                        TEST_TABLE_HANDLE,
                                        ImmutableList.of(columnSymbol),
                                        ImmutableMap.of(columnSymbol, columnHandle)));
                    })
                    .withSession(MOCK_SESSION)
                    .matches(project(
                            newNames.entrySet().stream()
                                    .collect(toImmutableMap(
                                            e -> e.getKey().getName(),
                                            e -> expression(symbolReference(e.getValue())))),
                            tableScan(
                                    equalTo(createTableHandle(TEST_SCHEMA, "projected_" + TEST_TABLE).getConnectorHandle()),
                                    TupleDomain.all(),
                                    newNames.entrySet().stream()
                                            .collect(toImmutableMap(
                                            Map.Entry::getValue,
                                                    e -> equalTo(column(e.getValue(), types.get(e.getKey()))))))));
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
                .withGetColumns(schemaTableName -> metadata);

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
        MockConnectorTableHandle newTableHandle = new MockConnectorTableHandle(projectedTableName);

        // Prepare new column handles
        ImmutableList.Builder<ConnectorExpression> outputExpressions = ImmutableList.builder();
        ImmutableList.Builder<Assignment> outputAssignments = ImmutableList.builder();

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
        }

        return Optional.of(new ProjectionApplicationResult<>(newTableHandle, outputExpressions.build(), outputAssignments.build()));
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
