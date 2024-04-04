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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.scalar.JsonPath;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.testing.TestingSession;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.type.LikeFunctions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MODULUS_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NULLIF_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SUBTRACT_FUNCTION_NAME;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
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
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.ConnectorExpressionTranslator.translate;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.JsonPathType.JSON_PATH;
import static io.trino.type.LikeFunctions.likePattern;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConnectorExpressionTranslator
{
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final Type ROW_TYPE = rowType(field("int_symbol_1", INTEGER), field("varchar_symbol_1", createVarcharType(5)));
    private static final VarcharType VARCHAR_TYPE = createUnboundedVarcharType();
    private static final ArrayType VARCHAR_ARRAY_TYPE = new ArrayType(VARCHAR_TYPE);

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction NEGATION_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(DOUBLE));

    private static final Map<Symbol, Type> symbols = ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol(DOUBLE, "double_symbol_1"), DOUBLE)
            .put(new Symbol(DOUBLE, "double_symbol_2"), DOUBLE)
            .put(new Symbol(ROW_TYPE, "row_symbol_1"), ROW_TYPE)
            .put(new Symbol(VARCHAR_TYPE, "varchar_symbol_1"), VARCHAR_TYPE)
            .put(new Symbol(BOOLEAN, "boolean_symbol_1"), BOOLEAN)
            .buildOrThrow();

    private static final Map<String, Symbol> variableMappings = symbols.entrySet().stream()
            .collect(toImmutableMap(entry -> entry.getKey().name(), Map.Entry::getKey));

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
        assertTranslationRoundTrips(new Constant(type, nativeValue), new io.trino.spi.expression.Constant(nativeValue, type));
    }

    @Test
    public void testTranslateSymbol()
    {
        assertTranslationRoundTrips(new Reference(DOUBLE, "double_symbol_1"), new Variable("double_symbol_1", DOUBLE));
    }

    @Test
    public void testTranslateRowSubscript()
    {
        assertTranslationRoundTrips(
                new FieldReference(
                        new Reference(ROW_TYPE, "row_symbol_1"),
                        0),
                new FieldDereference(
                        INTEGER,
                        new Variable("row_symbol_1", ROW_TYPE),
                        0));
    }

    @Test
    public void testTranslateLogicalExpression()
    {
        for (Logical.Operator operator : Logical.Operator.values()) {
            assertTranslationRoundTrips(
                    new Logical(
                            operator,
                            List.of(
                                    new Comparison(Comparison.Operator.LESS_THAN, new Reference(DOUBLE, "double_symbol_1"), new Reference(DOUBLE, "double_symbol_2")),
                                    new Comparison(Comparison.Operator.EQUAL, new Reference(DOUBLE, "double_symbol_1"), new Reference(DOUBLE, "double_symbol_2")))),
                    new io.trino.spi.expression.Call(
                            BOOLEAN,
                            operator == Logical.Operator.AND ? StandardFunctions.AND_FUNCTION_NAME : StandardFunctions.OR_FUNCTION_NAME,
                            List.of(
                                    new io.trino.spi.expression.Call(
                                            BOOLEAN,
                                            StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
                                            List.of(new Variable("double_symbol_1", DOUBLE), new Variable("double_symbol_2", DOUBLE))),
                                    new io.trino.spi.expression.Call(
                                            BOOLEAN,
                                            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                                            List.of(new Variable("double_symbol_1", DOUBLE), new Variable("double_symbol_2", DOUBLE))))));
        }
    }

    @Test
    public void testTranslateComparisonExpression()
    {
        for (Comparison.Operator operator : Comparison.Operator.values()) {
            assertTranslationRoundTrips(
                    new Comparison(operator, new Reference(DOUBLE, "double_symbol_1"), new Reference(DOUBLE, "double_symbol_2")),
                    new io.trino.spi.expression.Call(
                            BOOLEAN,
                            ConnectorExpressionTranslator.functionNameForComparisonOperator(operator),
                            List.of(new Variable("double_symbol_1", DOUBLE), new Variable("double_symbol_2", DOUBLE))));
        }
    }

    @Test
    public void testTranslateArithmeticBinary()
    {
        TestingFunctionResolution resolver = new TestingFunctionResolution();
        for (OperatorType operator : EnumSet.of(ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS)) {
            assertTranslationRoundTrips(
                    new Call(resolver.resolveOperator(
                            operator,
                            ImmutableList.of(DOUBLE, DOUBLE)), ImmutableList.of(new Reference(DOUBLE, "double_symbol_1"), new Reference(DOUBLE, "double_symbol_2"))),
                    new io.trino.spi.expression.Call(
                            DOUBLE,
                            functionNameForArithmeticBinaryOperator(operator),
                            List.of(new Variable("double_symbol_1", DOUBLE), new Variable("double_symbol_2", DOUBLE))));
        }
    }

    private static FunctionName functionNameForArithmeticBinaryOperator(OperatorType operator)
    {
        return switch (operator) {
            case ADD -> ADD_FUNCTION_NAME;
            case SUBTRACT -> SUBTRACT_FUNCTION_NAME;
            case MULTIPLY -> MULTIPLY_FUNCTION_NAME;
            case DIVIDE -> DIVIDE_FUNCTION_NAME;
            case MODULUS -> MODULUS_FUNCTION_NAME;
            default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
        };
    }

    @Test
    public void testTranslateArithmeticUnaryMinus()
    {
        assertTranslationRoundTrips(
                new Call(NEGATION_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "double_symbol_1"))),
                new io.trino.spi.expression.Call(DOUBLE, NEGATE_FUNCTION_NAME, List.of(new Variable("double_symbol_1", DOUBLE))));
    }

    @Test
    public void testTranslateBetween()
    {
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new Between(
                        new Reference(DOUBLE, "double_symbol_1"),
                        new Constant(DOUBLE, 1.2),
                        new Reference(DOUBLE, "double_symbol_2")),
                new io.trino.spi.expression.Call(
                        BOOLEAN,
                        AND_FUNCTION_NAME,
                        List.of(
                                new io.trino.spi.expression.Call(
                                        BOOLEAN,
                                        GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                                        List.of(
                                                new Variable("double_symbol_1", DOUBLE),
                                                new io.trino.spi.expression.Constant(1.2d, DOUBLE))),
                                new io.trino.spi.expression.Call(
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
                new IsNull(new Reference(VARCHAR, "varchar_symbol_1")),
                new io.trino.spi.expression.Call(
                        BOOLEAN,
                        IS_NULL_FUNCTION_NAME,
                        List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))));
    }

    @Test
    public void testTranslateNotExpression()
    {
        assertTranslationRoundTrips(
                new Not(new Reference(BOOLEAN, "boolean_symbol_1")),
                new io.trino.spi.expression.Call(
                        BOOLEAN,
                        NOT_FUNCTION_NAME,
                        List.of(new Variable("boolean_symbol_1", BOOLEAN))));
    }

    @Test
    public void testTranslateIsNotNull()
    {
        assertTranslationRoundTrips(
                new Not(new IsNull(new Reference(VARCHAR, "varchar_symbol_1"))),
                new io.trino.spi.expression.Call(
                        BOOLEAN,
                        NOT_FUNCTION_NAME,
                        List.of(new io.trino.spi.expression.Call(BOOLEAN, IS_NULL_FUNCTION_NAME, List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))))));
    }

    @Test
    public void testTranslateCast()
    {
        assertTranslationRoundTrips(
                new Cast(new Reference(VARCHAR, "varchar_symbol_1"), VARCHAR_TYPE),
                new io.trino.spi.expression.Call(
                        VARCHAR_TYPE,
                        CAST_FUNCTION_NAME,
                        List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))));

        // TRY_CAST is not translated
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                new Cast(
                        new Reference(VARCHAR, "varchar_symbol_1"),
                        BIGINT,
                        true),
                Optional.empty());
    }

    @Test
    public void testTranslateLike()
    {
        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        transaction(transactionManager, metadata, new AllowAllAccessControl())
                .readOnly()
                .execute(TEST_SESSION, transactionSession -> {
                    String pattern = "%pattern%";
                    io.trino.spi.expression.Call translated = new io.trino.spi.expression.Call(BOOLEAN,
                            StandardFunctions.LIKE_FUNCTION_NAME,
                            List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                    new io.trino.spi.expression.Constant(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), createVarcharType(pattern.length()))));

                    assertTranslationToConnectorExpression(
                            transactionSession,
                            BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                                    .setName(LikeFunctions.LIKE_FUNCTION_NAME).addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                                    .addArgument(LIKE_PATTERN, new Constant(LIKE_PATTERN, likePattern(utf8Slice(pattern))))
                                    .build(),
                            Optional.of(translated));

                    assertTranslationFromConnectorExpression(
                            transactionSession,
                            translated,
                            BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                                    .setName(LikeFunctions.LIKE_FUNCTION_NAME).addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                                    .addArgument(LIKE_PATTERN,
                                            BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                                                    .setName(LikeFunctions.LIKE_PATTERN_FUNCTION_NAME)
                                                    .addArgument(VARCHAR, new Cast(new Constant(createVarcharType(pattern.length()), utf8Slice(pattern)), VARCHAR))
                                                    .build())
                                    .build());

                    String escape = "\\";
                    translated = new io.trino.spi.expression.Call(BOOLEAN,
                            StandardFunctions.LIKE_FUNCTION_NAME,
                            List.of(
                                    new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                    new io.trino.spi.expression.Constant(Slices.wrappedBuffer(pattern.getBytes(UTF_8)), createVarcharType(pattern.length())),
                                    new io.trino.spi.expression.Constant(Slices.wrappedBuffer(escape.getBytes(UTF_8)), createVarcharType(escape.length()))));

                    assertTranslationToConnectorExpression(
                            transactionSession,
                            BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                                    .setName(LikeFunctions.LIKE_FUNCTION_NAME).addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                                    .addArgument(LIKE_PATTERN, new Constant(LIKE_PATTERN, likePattern(utf8Slice(pattern), utf8Slice(escape))))
                                    .build(),
                            Optional.of(translated));

                    assertTranslationFromConnectorExpression(
                            transactionSession,
                            translated,
                            BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                                    .setName(LikeFunctions.LIKE_FUNCTION_NAME).addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                                    .addArgument(LIKE_PATTERN,
                                            BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                                                    .setName(LikeFunctions.LIKE_PATTERN_FUNCTION_NAME)
                                                    .addArgument(VARCHAR, new Cast(new Constant(createVarcharType(9), utf8Slice(pattern)), VARCHAR))
                                                    .addArgument(VARCHAR, new Cast(new Constant(createVarcharType(1), utf8Slice(escape)), VARCHAR))
                                                    .build())
                                    .build());
                });
    }

    @Test
    public void testTranslateNullIf()
    {
        assertTranslationRoundTrips(
                new NullIf(
                        new Reference(VARCHAR, "varchar_symbol_1"),
                        new Reference(VARCHAR, "varchar_symbol_1")),
                new io.trino.spi.expression.Call(
                        VARCHAR_TYPE,
                        NULLIF_FUNCTION_NAME,
                        List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                new Variable("varchar_symbol_1", VARCHAR_TYPE))));
    }

    @Test
    public void testTranslateResolvedFunction()
    {
        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        transaction(transactionManager, metadata, new AllowAllAccessControl())
                .readOnly()
                .execute(TEST_SESSION, transactionSession -> {
                    assertTranslationRoundTrips(
                            transactionSession,
                            BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                                    .setName("lower").addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                                    .build(),
                            new io.trino.spi.expression.Call(VARCHAR_TYPE,
                                    new FunctionName("lower"),
                                    List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE))));
                });
    }

    @Test
    public void testTranslateRegularExpression()
    {
        // Regular expression types (JoniRegexpType, Re2JRegexpType) are considered implementation detail of the engine
        // and are not exposed to connectors within ConnectorExpression. Instead, they are replaced with a varchar pattern.

        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        transaction(transactionManager, metadata, new AllowAllAccessControl())
                .readOnly()
                .execute(TEST_SESSION, transactionSession -> {
                    Call input = BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                            .setName("regexp_like").addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                            .addArgument(new Constant(JONI_REGEXP, joniRegexp(utf8Slice("a+"))))
                            .build();
                    io.trino.spi.expression.Call translated = new io.trino.spi.expression.Call(
                            BOOLEAN,
                            new FunctionName("regexp_like"),
                            List.of(
                                    new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                    new io.trino.spi.expression.Constant(utf8Slice("a+"), createVarcharType(2))));
                    Call translatedBack = BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                            .setName("regexp_like").addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                            // Note: The result is not an optimized expression
                            .addArgument(JONI_REGEXP, new Cast(new Constant(createVarcharType(2), utf8Slice("a+")), JONI_REGEXP))
                            .build();

                    assertTranslationToConnectorExpression(transactionSession, input, translated);
                    assertTranslationFromConnectorExpression(transactionSession, translated, translatedBack);
                });
    }

    @Test
    void testTranslateJsonPath()
    {
        io.trino.spi.expression.Call connectorExpression = new io.trino.spi.expression.Call(
                VARCHAR_TYPE,
                new FunctionName("json_extract_scalar"),
                List.of(new Variable("varchar_symbol_1", VARCHAR_TYPE),
                        new io.trino.spi.expression.Constant(utf8Slice("$.path"), createVarcharType(6))));

        // JSON path type is considered implementation detail of the engine and is not exposed to connectors
        // within ConnectorExpression. Instead, it is replaced with a varchar pattern.
        assertTranslationToConnectorExpression(
                TEST_SESSION,
                BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                        .setName("json_extract_scalar").addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                        .addArgument(JSON_PATH, new Constant(JSON_PATH, new JsonPath("$.path")))
                        .build(),
                Optional.of(connectorExpression));

        assertTranslationFromConnectorExpression(
                TEST_SESSION,
                connectorExpression,
                BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                        .setName("json_extract_scalar").addArgument(VARCHAR_TYPE, new Reference(VARCHAR_TYPE, "varchar_symbol_1"))
                        .addArgument(JSON_PATH, new Cast(new Constant(createVarcharType(6), utf8Slice("$.path")), JSON_PATH))
                        .build());
    }

    @Test
    public void testTranslateIn()
    {
        String value = "value_1";
        assertTranslationRoundTrips(
                new In(
                        new Reference(VARCHAR, "varchar_symbol_1"),
                        List.of(new Reference(VARCHAR, "varchar_symbol_1"), new Constant(VARCHAR, utf8Slice(value)))),
                new io.trino.spi.expression.Call(
                    BOOLEAN,
                    StandardFunctions.IN_PREDICATE_FUNCTION_NAME,
                    List.of(
                            new Variable("varchar_symbol_1", VARCHAR_TYPE),
                            new io.trino.spi.expression.Call(VARCHAR_ARRAY_TYPE, ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                                    List.of(
                                            new Variable("varchar_symbol_1", VARCHAR_TYPE),
                                            new io.trino.spi.expression.Constant(Slices.wrappedBuffer(value.getBytes(UTF_8)), VARCHAR_TYPE))))));
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
        Optional<ConnectorExpression> translation = translate(session, expression);
        assertThat(connectorExpression.isPresent()).isEqualTo(translation.isPresent());
        translation.ifPresent(value -> assertThat(value).isEqualTo(connectorExpression.get()));
    }

    private void assertTranslationFromConnectorExpression(Session session, ConnectorExpression connectorExpression, Expression expected)
    {
        Expression translation = ConnectorExpressionTranslator.translate(session, connectorExpression, PLANNER_CONTEXT, variableMappings);
        assertThat(translation).isEqualTo(expected);
    }
}
