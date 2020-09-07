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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.FieldDereference;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.spi.type.RowType.rowType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.planner.ConnectorExpressionTranslator.translate;
import static org.testng.Assert.assertEquals;

public class TestConnectorExpressionTranslator
{
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final TypeAnalyzer TYPE_ANALYZER = new TypeAnalyzer(new SqlParser(), METADATA);
    private static final Type ROW_TYPE = rowType(field("int_symbol_1", INTEGER), field("varchar_symbol_1", createVarcharType(5)));
    private static final LiteralEncoder LITERAL_ENCODER = new LiteralEncoder(METADATA);

    private static final Map<Symbol, Type> symbols = ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("double_symbol_1"), DOUBLE)
            .put(new Symbol("row_symbol_1"), ROW_TYPE)
            .build();

    private static final TypeProvider TYPE_PROVIDER = TypeProvider.copyOf(symbols);
    private static final Map<String, Symbol> variableMappings = symbols.entrySet().stream()
            .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getKey));

    @Test
    public void testTranslationToConnectorExpression()
    {
        assertTranslationToConnectorExpression(new SymbolReference("double_symbol_1"), Optional.of(new Variable("double_symbol_1", DOUBLE)));

        assertTranslationToConnectorExpression(
                new DereferenceExpression(
                        new SymbolReference("row_symbol_1"),
                        new Identifier("int_symbol_1")),
                Optional.of(
                        new FieldDereference(
                                INTEGER,
                                new Variable("row_symbol_1", ROW_TYPE),
                                0)));
    }

    @Test
    public void testTranslationFromConnectorExpression()
    {
        assertTranslationFromConnectorExpression(new Variable("double_symbol_1", DOUBLE), new SymbolReference("double_symbol_1"));

        assertTranslationFromConnectorExpression(
                new FieldDereference(
                        INTEGER,
                        new Variable("row_symbol_1", ROW_TYPE),
                        0),
                new DereferenceExpression(
                        new SymbolReference("row_symbol_1"),
                        new Identifier("int_symbol_1")));
    }

    private void assertTranslationToConnectorExpression(Expression expression, Optional<ConnectorExpression> connectorExpression)
    {
        Optional<ConnectorExpression> translation = translate(TEST_SESSION, expression, TYPE_ANALYZER, TYPE_PROVIDER);
        assertEquals(connectorExpression.isPresent(), translation.isPresent());
        translation.ifPresent(value -> assertEquals(value, connectorExpression.get()));
    }

    private void assertTranslationFromConnectorExpression(ConnectorExpression connectorExpression, Expression expected)
    {
        Expression translation = translate(connectorExpression, variableMappings, LITERAL_ENCODER);
        assertEquals(translation, expected);
    }
}
