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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.ConnectorExpressionTranslator.translate;
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
                new SubscriptExpression(
                        new SymbolReference("row_symbol_1"),
                        new LongLiteral("1")),
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
                new SubscriptExpression(
                        new SymbolReference("row_symbol_1"),
                        new LongLiteral("1")));
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
