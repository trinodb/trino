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
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingSession;
import io.trino.transaction.TestingTransactionManager;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.ConnectorExpressionTranslator.translate;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestConnectorExpressionTranslator
{
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final TypeAnalyzer TYPE_ANALYZER = createTestingTypeAnalyzer(PLANNER_CONTEXT);
    private static final Type ROW_TYPE = rowType(field("int_symbol_1", INTEGER), field("varchar_symbol_1", createVarcharType(5)));
    private static final Type VARCHAR_TYPE = createVarcharType(25);
    private static final LiteralEncoder LITERAL_ENCODER = new LiteralEncoder(PLANNER_CONTEXT);

    private static final Map<Symbol, Type> symbols = ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("double_symbol_1"), DOUBLE)
            .put(new Symbol("row_symbol_1"), ROW_TYPE)
            .put(new Symbol("varchar_symbol_1"), VARCHAR_TYPE)
            .buildOrThrow();

    private static final TypeProvider TYPE_PROVIDER = TypeProvider.copyOf(symbols);
    private static final Map<String, Symbol> variableMappings = symbols.entrySet().stream()
            .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getKey));

    @Test
    public void testTranslationToConnectorExpression()
    {
        assertTranslationToConnectorExpression(TEST_SESSION, new SymbolReference("double_symbol_1"), Optional.of(new Variable("double_symbol_1", DOUBLE)));

        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new SubscriptExpression(
                        new SymbolReference("row_symbol_1"),
                        new LongLiteral("1")),
                Optional.of(
                        new FieldDereference(
                                INTEGER,
                                new Variable("row_symbol_1", ROW_TYPE),
                                0)));

        String pattern = "%pattern%";
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new LikePredicate(
                        new SymbolReference("varchar_symbol_1"),
                        new StringLiteral(pattern),
                        Optional.empty()),
                Optional.of(new Call(VARCHAR_TYPE,
                                     Optional.empty(),
                                     LIKE_PATTERN_FUNCTION_NAME,
                                     List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                             new Constant(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), createVarcharType(pattern.length()))))));

        transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .readOnly()
                .execute(TEST_SESSION, transactionSession -> {
                    assertTranslationToConnectorExpression(transactionSession,
                                                           FunctionCallBuilder.resolve(TEST_SESSION, PLANNER_CONTEXT.getMetadata())
                                                                   .setName(QualifiedName.of(("lower")))
                                                                   .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                                                                   .build(),
                                                           Optional.of(new Call(VARCHAR_TYPE,
                                                                                Optional.empty(),
                                                                                "lower",
                                                                                List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE)))));
                });
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

        String pattern = "%pattern%";
        assertTranslationFromConnectorExpression(
                new Call(VARCHAR_TYPE,
                         Optional.empty(),
                         LIKE_PATTERN_FUNCTION_NAME,
                         List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                 new Constant(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), createVarcharType(pattern.length())))),
                new LikePredicate(new SymbolReference("varchar_symbol_1"),
                                  new StringLiteral(pattern),
                                  Optional.empty()));

        assertTranslationFromConnectorExpression(
                new Call(VARCHAR_TYPE,
                         Optional.empty(),
                         "lower",
                         List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))),
                FunctionCallBuilder.resolve(TEST_SESSION, PLANNER_CONTEXT.getMetadata())
                                   .setName(QualifiedName.of(("lower")))
                                   .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                                   .build());
    }

    private void assertTranslationToConnectorExpression(Session session, Expression expression, Optional<ConnectorExpression> connectorExpression)
    {
        Optional<ConnectorExpression> translation = translate(session, expression, TYPE_ANALYZER, TYPE_PROVIDER, PLANNER_CONTEXT);
        assertEquals(connectorExpression.isPresent(), translation.isPresent());
        translation.ifPresent(value -> assertEquals(value, connectorExpression.get()));
    }

    private void assertTranslationFromConnectorExpression(ConnectorExpression connectorExpression, Expression expected)
    {
        Expression translation = ConnectorExpressionTranslator.translate(TEST_SESSION, connectorExpression, PLANNER_CONTEXT, variableMappings, LITERAL_ENCODER);
        assertEquals(translation, expected);
    }
}
