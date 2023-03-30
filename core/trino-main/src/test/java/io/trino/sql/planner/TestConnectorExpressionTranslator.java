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
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingSession;
import io.trino.transaction.TestingTransactionManager;
import io.trino.type.LikeFunctions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NULLIF_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.ConnectorExpressionTranslator.translate;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.LikeFunctions.likePattern;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestConnectorExpressionTranslator
{
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final TypeAnalyzer TYPE_ANALYZER = createTestingTypeAnalyzer(PLANNER_CONTEXT);
    private static final Type ROW_TYPE = rowType(field("int_symbol_1", INTEGER), field("varchar_symbol_1", createVarcharType(5)));
    private static final VarcharType VARCHAR_TYPE = createVarcharType(25);
    private static final ArrayType VARCHAR_ARRAY_TYPE = new ArrayType(VARCHAR_TYPE);

    private static final LiteralEncoder LITERAL_ENCODER = new LiteralEncoder(PLANNER_CONTEXT);

    private static final Map<Symbol, Type> symbols = ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("double_symbol_1"), DOUBLE)
            .put(new Symbol("double_symbol_2"), DOUBLE)
            .put(new Symbol("row_symbol_1"), ROW_TYPE)
            .put(new Symbol("varchar_symbol_1"), VARCHAR_TYPE)
            .put(new Symbol("boolean_symbol_1"), BOOLEAN)
            .buildOrThrow();

    private static final TypeProvider TYPE_PROVIDER = TypeProvider.copyOf(symbols);
    private static final Map<String, Symbol> variableMappings = symbols.entrySet().stream()
            .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getKey));

    @Test
    public void testTranslateConstant()
    {
        testTranslateConstant(true, BOOLEAN);

        testTranslateConstant(42L, TINYINT);
        testTranslateConstant(42L, SMALLINT);
        testTranslateConstant(42L, INTEGER);
        testTranslateConstant(42L, BIGINT);

        testTranslateConstant(42L, REAL);
        testTranslateConstant(42d, DOUBLE);

        testTranslateConstant(4200L, createDecimalType(4, 2));
        testTranslateConstant(4200L, createDecimalType(8, 2));

        testTranslateConstant(utf8Slice("abc"), createVarcharType(3));
        testTranslateConstant(utf8Slice("abc"), createVarcharType(33));
    }

    private void testTranslateConstant(Object nativeValue, Type type)
    {
        assertTranslationRoundTrips(LITERAL_ENCODER.toExpression(TEST_SESSION, nativeValue, type), new Constant(nativeValue, type));
    }

    @Test
    public void testTranslateSymbol()
    {
        assertTranslationRoundTrips(new SymbolReference("double_symbol_1"), new Variable("double_symbol_1", DOUBLE));
    }

    @Test
    public void testTranslateRowSubscript()
    {
        assertTranslationRoundTrips(
                new SubscriptExpression(
                        new SymbolReference("row_symbol_1"),
                        new LongLiteral("1")),
                new FieldDereference(
                        INTEGER,
                        new Variable("row_symbol_1", ROW_TYPE),
                        0));
    }

    @Test(dataProvider = "testTranslateLogicalExpressionDataProvider")
    public void testTranslateLogicalExpression(LogicalExpression.Operator operator)
    {
        assertTranslationRoundTrips(
                new LogicalExpression(
                        operator,
                        List.of(
                                new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference("double_symbol_1"), new SymbolReference("double_symbol_2")),
                                new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("double_symbol_1"), new SymbolReference("double_symbol_2")))),
                new Call(
                        BOOLEAN,
                        operator == LogicalExpression.Operator.AND ? StandardFunctions.AND_FUNCTION_NAME : StandardFunctions.OR_FUNCTION_NAME,
                        List.of(
                                new Call(
                                        BOOLEAN,
                                        StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
                                        List.of(new Variable("double_symbol_1", DOUBLE), new Variable("double_symbol_2", DOUBLE))),
                                new Call(
                                        BOOLEAN,
                                        StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                                        List.of(new Variable("double_symbol_1", DOUBLE), new Variable("double_symbol_2", DOUBLE))))));
    }

    @DataProvider
    public Object[][] testTranslateLogicalExpressionDataProvider()
    {
        return Stream.of(LogicalExpression.Operator.values())
                .collect(toDataProvider());
    }

    @Test(dataProvider = "testTranslateComparisonExpressionDataProvider")
    public void testTranslateComparisonExpression(ComparisonExpression.Operator operator)
    {
        assertTranslationRoundTrips(
                new ComparisonExpression(operator, new SymbolReference("double_symbol_1"), new SymbolReference("double_symbol_2")),
                new Call(
                        BOOLEAN,
                        ConnectorExpressionTranslator.functionNameForComparisonOperator(operator),
                        List.of(new Variable("double_symbol_1", DOUBLE), new Variable("double_symbol_2", DOUBLE))));
    }

    @DataProvider
    public static Object[][] testTranslateComparisonExpressionDataProvider()
    {
        return Stream.of(ComparisonExpression.Operator.values())
                .collect(toDataProvider());
    }

    @Test(dataProvider = "testTranslateArithmeticBinaryDataProvider")
    public void testTranslateArithmeticBinary(ArithmeticBinaryExpression.Operator operator)
    {
        assertTranslationRoundTrips(
                new ArithmeticBinaryExpression(operator, new SymbolReference("double_symbol_1"), new SymbolReference("double_symbol_2")),
                new Call(
                        DOUBLE,
                        ConnectorExpressionTranslator.functionNameForArithmeticBinaryOperator(operator),
                        List.of(new Variable("double_symbol_1", DOUBLE), new Variable("double_symbol_2", DOUBLE))));
    }

    @DataProvider
    public static Object[][] testTranslateArithmeticBinaryDataProvider()
    {
        return Stream.of(ArithmeticBinaryExpression.Operator.values())
                .collect(toDataProvider());
    }

    @Test
    public void testTranslateArithmeticUnaryMinus()
    {
        assertTranslationRoundTrips(
                new ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.MINUS, new SymbolReference("double_symbol_1")),
                new Call(DOUBLE, NEGATE_FUNCTION_NAME, List.of(new Variable("double_symbol_1", DOUBLE))));
    }

    @Test
    public void testTranslateArithmeticUnaryPlus()
    {
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.PLUS, new SymbolReference("double_symbol_1")),
                new Variable("double_symbol_1", DOUBLE));
    }

    @Test
    public void testTranslateBetween()
    {
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new BetweenPredicate(
                        new SymbolReference("double_symbol_1"),
                        new DoubleLiteral("1.2"),
                        new SymbolReference("double_symbol_2")),
                new Call(
                        BOOLEAN,
                        AND_FUNCTION_NAME,
                        List.of(
                                new Call(
                                        BOOLEAN,
                                        GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                                        List.of(
                                                new Variable("double_symbol_1", DOUBLE),
                                                new Constant(1.2d, DOUBLE))),
                                new Call(
                                        BOOLEAN,
                                        LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                                        List.of(
                                                new Variable("double_symbol_1", DOUBLE),
                                                new Variable("double_symbol_2", DOUBLE))))));
    }

    @Test
    public void testTranslateIsNull()
    {
        assertTranslationRoundTrips(
                new IsNullPredicate(new SymbolReference("varchar_symbol_1")),
                new Call(
                        BOOLEAN,
                        IS_NULL_FUNCTION_NAME,
                        List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))));
    }

    @Test
    public void testTranslateNotExpression()
    {
        assertTranslationRoundTrips(
                new NotExpression(new SymbolReference("boolean_symbol_1")),
                new Call(
                        BOOLEAN,
                        NOT_FUNCTION_NAME,
                        List.of(new Variable("boolean_symbol_1", BOOLEAN))));
    }

    @Test
    public void testTranslateIsNotNull()
    {
        assertTranslationRoundTrips(
                new IsNotNullPredicate(new SymbolReference("varchar_symbol_1")),
                new Call(
                        BOOLEAN,
                        NOT_FUNCTION_NAME,
                        List.of(new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))))));
    }

    @Test
    public void testTranslateCast()
    {
        assertTranslationRoundTrips(
                new Cast(new SymbolReference("varchar_symbol_1"), toSqlType(VARCHAR_TYPE)),
                new Call(
                        VARCHAR_TYPE,
                        CAST_FUNCTION_NAME,
                        List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))));

        // type-only
        VarcharType longerVarchar = createVarcharType(VARCHAR_TYPE.getBoundedLength() + 1);
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new Cast(new SymbolReference("varchar_symbol_1"), toSqlType(longerVarchar), false, true),
                new Call(
                        longerVarchar,
                        CAST_FUNCTION_NAME,
                        List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))));

        // TRY_CAST is not translated
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new Cast(
                        new SymbolReference("varchar_symbol_1"),
                        toSqlType(BIGINT),
                        true,
                        true),
                Optional.empty());
    }

    @Test
    public void testTranslateLike()
    {
        transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .readOnly()
                .execute(TEST_SESSION, transactionSession -> {
                    String pattern = "%pattern%";
                    Call translated = new Call(BOOLEAN,
                            StandardFunctions.LIKE_FUNCTION_NAME,
                            List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                    new Constant(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), createVarcharType(pattern.length()))));

                    assertTranslationToConnectorExpression(
                            transactionSession,
                            FunctionCallBuilder.resolve(transactionSession, PLANNER_CONTEXT.getMetadata())
                                    .setName(QualifiedName.of(LikeFunctions.LIKE_FUNCTION_NAME))
                                    .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                                    .addArgument(LIKE_PATTERN, LITERAL_ENCODER.toExpression(transactionSession, likePattern(utf8Slice(pattern)), LIKE_PATTERN))
                                    .build(),
                            Optional.of(translated));

                    assertTranslationFromConnectorExpression(
                            transactionSession,
                            translated,
                            FunctionCallBuilder.resolve(transactionSession, PLANNER_CONTEXT.getMetadata())
                                    .setName(QualifiedName.of(LikeFunctions.LIKE_FUNCTION_NAME))
                                    .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                                    .addArgument(LIKE_PATTERN,
                                            FunctionCallBuilder.resolve(transactionSession, PLANNER_CONTEXT.getMetadata())
                                                    .setName(QualifiedName.of(LikeFunctions.LIKE_PATTERN_FUNCTION_NAME))
                                                    .addArgument(createVarcharType(pattern.length()), new StringLiteral(pattern))
                                                    .build())
                                    .build());

                    String escape = "\\";
                    translated = new Call(BOOLEAN,
                            StandardFunctions.LIKE_FUNCTION_NAME,
                            List.of(
                                    new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                    new Constant(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), createVarcharType(pattern.length())),
                                    new Constant(Slices.wrappedBuffer(escape.getBytes(UTF_8)), createVarcharType(escape.length()))));

                    assertTranslationToConnectorExpression(
                            transactionSession,
                            FunctionCallBuilder.resolve(transactionSession, PLANNER_CONTEXT.getMetadata())
                                    .setName(QualifiedName.of(LikeFunctions.LIKE_FUNCTION_NAME))
                                    .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                                    .addArgument(LIKE_PATTERN, LITERAL_ENCODER.toExpression(transactionSession, likePattern(utf8Slice(pattern), utf8Slice(escape)), LIKE_PATTERN))
                                    .build(),
                            Optional.of(translated));

                    assertTranslationFromConnectorExpression(
                            transactionSession,
                            translated,
                            FunctionCallBuilder.resolve(transactionSession, PLANNER_CONTEXT.getMetadata())
                                    .setName(QualifiedName.of(LikeFunctions.LIKE_FUNCTION_NAME))
                                    .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                                    .addArgument(LIKE_PATTERN,
                                            FunctionCallBuilder.resolve(transactionSession, PLANNER_CONTEXT.getMetadata())
                                                    .setName(QualifiedName.of(LikeFunctions.LIKE_PATTERN_FUNCTION_NAME))
                                                    .addArgument(createVarcharType(pattern.length()), new StringLiteral(pattern))
                                                    .addArgument(createVarcharType(1), new StringLiteral(escape))
                                                    .build())
                                    .build());
                });
    }

    @Test
    public void testTranslateNullIf()
    {
        assertTranslationRoundTrips(
                new NullIfExpression(
                        new SymbolReference("varchar_symbol_1"),
                        new SymbolReference("varchar_symbol_1")),
                new Call(
                        VARCHAR_TYPE,
                        NULLIF_FUNCTION_NAME,
                        List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                new Variable("varchar_symbol_1", VARCHAR_TYPE))));
    }

    @Test
    public void testTranslateResolvedFunction()
    {
        transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .readOnly()
                .execute(TEST_SESSION, transactionSession -> {
                    assertTranslationRoundTrips(
                            transactionSession,
                            FunctionCallBuilder.resolve(TEST_SESSION, PLANNER_CONTEXT.getMetadata())
                                    .setName(QualifiedName.of(("lower")))
                                    .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                                    .build(),
                            new Call(VARCHAR_TYPE,
                                    new FunctionName("lower"),
                                    List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))));
                });
    }

    @Test
    public void testTranslateRegularExpression()
    {
        // Regular expression types (JoniRegexpType, Re2JRegexpType) are considered implementation detail of the engine
        // and are not exposed to connectors within ConnectorExpression. Instead, they are replaced with a varchar pattern.

        transaction(new TestingTransactionManager(), new AllowAllAccessControl())
                .readOnly()
                .execute(TEST_SESSION, transactionSession -> {
                    FunctionCall input = FunctionCallBuilder.resolve(TEST_SESSION, PLANNER_CONTEXT.getMetadata())
                            .setName(QualifiedName.of(("regexp_like")))
                            .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                            .addArgument(JONI_REGEXP, LITERAL_ENCODER.toExpression(TEST_SESSION, joniRegexp(utf8Slice("a+")), JONI_REGEXP))
                            .build();
                    Call translated = new Call(
                            BOOLEAN,
                            new FunctionName("regexp_like"),
                            List.of(
                                    new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                    new Constant(utf8Slice("a+"), createVarcharType(2))));
                    FunctionCall translatedBack = FunctionCallBuilder.resolve(TEST_SESSION, PLANNER_CONTEXT.getMetadata())
                            .setName(QualifiedName.of(("regexp_like")))
                            .addArgument(VARCHAR_TYPE, new SymbolReference("varchar_symbol_1"))
                            // Note: The result is not an optimized expression
                            .addArgument(JONI_REGEXP, new Cast(new StringLiteral("a+"), toSqlType(JONI_REGEXP)))
                            .build();

                    assertTranslationToConnectorExpression(transactionSession, input, translated);
                    assertTranslationFromConnectorExpression(transactionSession, translated, translatedBack);
                });
    }

    @Test
    public void testTranslateIn()
    {
        String value = "value_1";
        assertTranslationRoundTrips(
                new InPredicate(
                    new SymbolReference("varchar_symbol_1"),
                    new InListExpression(List.of(new SymbolReference("varchar_symbol_1"), new StringLiteral(value)))),
                new Call(
                    BOOLEAN,
                    StandardFunctions.IN_PREDICATE_FUNCTION_NAME,
                    List.of(
                            new Variable("varchar_symbol_1", VARCHAR_TYPE),
                            new Call(VARCHAR_ARRAY_TYPE, ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                                    List.of(
                                            new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                            new Constant(Slices.wrappedBuffer(value.getBytes(UTF_8)), createVarcharType(value.length())))))));

        // IN (null) is not translated
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new InPredicate(
                        new SymbolReference("varchar_symbol_1"),
                        new InListExpression(List.of(new SymbolReference("varchar_symbol_1"), new NullLiteral()))),
                Optional.empty());
    }

    private void assertTranslationRoundTrips(Expression expression, ConnectorExpression connectorExpression)
    {
        assertTranslationRoundTrips(TEST_SESSION, expression, connectorExpression);
    }

    private void assertTranslationRoundTrips(Session session, Expression expression, ConnectorExpression connectorExpression)
    {
        assertTranslationToConnectorExpression(session, expression, Optional.of(connectorExpression));
        assertTranslationFromConnectorExpression(session, connectorExpression, expression);
    }

    private void assertTranslationToConnectorExpression(Session session, Expression expression, ConnectorExpression connectorExpression)
    {
        assertTranslationToConnectorExpression(session, expression, Optional.of(connectorExpression));
    }

    private void assertTranslationToConnectorExpression(Session session, Expression expression, Optional<ConnectorExpression> connectorExpression)
    {
        Optional<ConnectorExpression> translation = translate(session, expression, TYPE_PROVIDER, PLANNER_CONTEXT, TYPE_ANALYZER);
        assertEquals(connectorExpression.isPresent(), translation.isPresent());
        translation.ifPresent(value -> assertEquals(value, connectorExpression.get()));
    }

    private void assertTranslationFromConnectorExpression(Session session, ConnectorExpression connectorExpression, Expression expected)
    {
        Expression translation = ConnectorExpressionTranslator.translate(session, connectorExpression, PLANNER_CONTEXT, variableMappings, LITERAL_ENCODER);
        assertEquals(translation, expected);
    }
}
