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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.WorkProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.ir.Reference;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.testing.TestingSession;
import io.trino.type.LikePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.Stream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.block.BlockTestUtils.assertBlockEquals;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.gen.columnar.FilterEvaluator.createColumnarFilterEvaluator;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.constantNull;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.sql.relational.SpecialForm.Form.OR;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnarFilters
{
    private static final Random RANDOM = new Random(5376453765L);
    private static final long CONSTANT = 64992484L;
    private static final int ROW_NUM_CHANNEL = 0;
    private static final int DOUBLE_CHANNEL = 1;
    private static final int INT_CHANNEL_B = 2;
    private static final int STRING_CHANNEL = 3;
    private static final int INT_CHANNEL_A = 4;
    private static final int INT_CHANNEL_C = 5;
    private static final int ARRAY_CHANNEL = 6;
    private static final int REAL_CHANNEL = 7;
    private static final Type ARRAY_CHANNEL_TYPE = new ArrayType(INTEGER);
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));
    private static final FunctionBundle FUNCTION_BUNDLE = InternalFunctionBundle.builder()
            .scalar(NullableReturnFunction.class)
            .scalar(ConnectorSessionFunction.class)
            .scalar(InstanceFactoryFunction.class)
            .scalar(CustomIsDistinctFrom.class)
            .build();
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution(FUNCTION_BUNDLE);
    private static final ColumnarFilterCompiler COMPILER = FUNCTION_RESOLUTION.getColumnarFilterCompiler();

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testIsNotDistinctFrom(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // col IS NOT DISTINCT FROM constant
        RowExpression isNotDistinctFromFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(IDENTICAL, ImmutableList.of(INTEGER, INTEGER)),
                constant(CONSTANT, INTEGER),
                field(INT_CHANNEL_A, INTEGER));
        assertThatColumnarFilterEvaluationIsSupported(isNotDistinctFromFilter);
        verifyFilter(inputPages, isNotDistinctFromFilter);

        // colA IS NOT DISTINCT FROM NULL
        isNotDistinctFromFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(IDENTICAL, ImmutableList.of(INTEGER, INTEGER)),
                constantNull(INTEGER),
                field(INT_CHANNEL_A, INTEGER));
        assertThatColumnarFilterEvaluationIsNotSupported(isNotDistinctFromFilter);
        verifyFilter(inputPages, isNotDistinctFromFilter);

        // colA IS NOT DISTINCT FROM colB
        isNotDistinctFromFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(IDENTICAL, ImmutableList.of(INTEGER, INTEGER)),
                field(INT_CHANNEL_C, INTEGER),
                field(INT_CHANNEL_A, INTEGER));
        assertThatColumnarFilterEvaluationIsSupported(isNotDistinctFromFilter);
        verifyFilter(inputPages, isNotDistinctFromFilter);
    }

    @Test
    public void testIsDistinctFrom()
    {
        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        // col IS DISTINCT FROM constant
        RowExpression isDistinctFromFilter = createNotExpression(call(
                FUNCTION_RESOLUTION.resolveOperator(IDENTICAL, ImmutableList.of(INTEGER, INTEGER)),
                constant(CONSTANT, INTEGER),
                field(INT_CHANNEL_A, INTEGER)));
        // IS DISTINCT is not supported in columnar evaluation yet
        assertThatColumnarFilterEvaluationIsNotSupported(isDistinctFromFilter);
        verifyFilter(inputPages, isDistinctFromFilter);

        // colA IS DISTINCT FROM colB
        isDistinctFromFilter = createNotExpression(call(
                FUNCTION_RESOLUTION.resolveOperator(IDENTICAL, ImmutableList.of(INTEGER, INTEGER)),
                field(INT_CHANNEL_B, INTEGER),
                field(INT_CHANNEL_A, INTEGER)));
        // IS DISTINCT is not supported in columnar evaluation yet
        assertThatColumnarFilterEvaluationIsNotSupported(isDistinctFromFilter);
        verifyFilter(inputPages, isDistinctFromFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testIsNull(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        RowExpression isNullFilter = new SpecialForm(IS_NULL, BOOLEAN, ImmutableList.of(field(INT_CHANNEL_A, INTEGER)), ImmutableList.of());
        assertThatColumnarFilterEvaluationIsSupported(isNullFilter);
        verifyFilter(inputPages, isNullFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testNullableReturnFunction(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // custom_is_null(col, NULL)
        RowExpression customNullableReturnFilter = call(
                FUNCTION_RESOLUTION.functionCallBuilder("custom_is_null")
                        .addArgument(VARCHAR, new Reference(VARCHAR, "symbol"))
                        .build()
                        .function(),
                field(STRING_CHANNEL, VARCHAR));
        assertThatColumnarFilterEvaluationIsSupported(customNullableReturnFilter);
        verifyFilter(inputPages, customNullableReturnFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testConnectorSessionFunction(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // is_user_admin(connectorSession)
        RowExpression customConnectorSessionFilter = call(
                FUNCTION_RESOLUTION.functionCallBuilder("is_user_admin")
                        .build()
                        .function());
        assertThatColumnarFilterEvaluationIsSupported(customConnectorSessionFilter);
        verifyFilter(inputPages, customConnectorSessionFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testInstanceFactoryFunction(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // is_answer_to_universe(col)
        RowExpression customInstanceFactoryFilter = call(
                FUNCTION_RESOLUTION.functionCallBuilder("is_answer_to_universe")
                        .addArgument(INTEGER, new Reference(INTEGER, "symbol"))
                        .build()
                        .function(),
                field(INT_CHANNEL_A, INTEGER));
        assertThatColumnarFilterEvaluationIsSupported(customInstanceFactoryFilter);
        verifyFilter(inputPages, customInstanceFactoryFilter);
    }

    @Test
    public void testBooleanConstant()
    {
        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        // WHERE true
        RowExpression trueFilter = constant(true, BOOLEAN);
        assertThatColumnarFilterEvaluationIsSupported(trueFilter);
        verifyFilter(inputPages, trueFilter);

        // WHERE false
        RowExpression falseFilter = constant(false, BOOLEAN);
        assertThatColumnarFilterEvaluationIsSupported(falseFilter);
        verifyFilter(inputPages, falseFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testIsNotNull(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        RowExpression isNotNullFilter = createNotExpression(
                new SpecialForm(IS_NULL, BOOLEAN, ImmutableList.of(field(INT_CHANNEL_A, INTEGER)), ImmutableList.of()));
        assertThatColumnarFilterEvaluationIsSupported(isNotNullFilter);
        verifyFilter(inputPages, isNotNullFilter);
    }

    @Test
    public void testNot()
    {
        List<Page> inputPages = createInputPages(NullsProvider.RANDOM_NULLS, false);
        RowExpression notNullFilter = createNotExpression(call(
                FUNCTION_RESOLUTION.resolveOperator(EQUAL, ImmutableList.of(INTEGER, INTEGER)),
                constant(CONSTANT, INTEGER),
                field(INT_CHANNEL_A, INTEGER)));
        // NOT is not supported in columnar evaluation yet
        assertThatColumnarFilterEvaluationIsNotSupported(notNullFilter);
        verifyFilter(inputPages, notNullFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testLike(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        RowExpression likeFilter = call(
                FUNCTION_RESOLUTION.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                field(STRING_CHANNEL, VARCHAR),
                constant(LikePattern.compile(Long.toString(CONSTANT), Optional.empty()), LIKE_PATTERN));
        assertThatColumnarFilterEvaluationIsSupported(likeFilter);
        verifyFilter(inputPages, likeFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testLessThan(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // constant < col
        RowExpression lessThanFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(INTEGER, INTEGER)),
                constant(CONSTANT, INTEGER),
                field(INT_CHANNEL_A, INTEGER));
        assertThatColumnarFilterEvaluationIsSupported(lessThanFilter);
        verifyFilter(inputPages, lessThanFilter);

        // col < constant
        lessThanFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(DOUBLE, DOUBLE)),
                field(DOUBLE_CHANNEL, DOUBLE),
                constant((double) CONSTANT, DOUBLE));
        assertThatColumnarFilterEvaluationIsSupported(lessThanFilter);
        verifyFilter(inputPages, lessThanFilter);

        // colA < colB
        lessThanFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(INTEGER, INTEGER)),
                field(INT_CHANNEL_C, INTEGER),
                field(INT_CHANNEL_A, INTEGER));
        assertThatColumnarFilterEvaluationIsSupported(lessThanFilter);
        verifyFilter(inputPages, lessThanFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testEq(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // constant = col
        RowExpression lessThanFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(EQUAL, ImmutableList.of(INTEGER, INTEGER)),
                constant(CONSTANT, INTEGER),
                field(INT_CHANNEL_A, INTEGER));
        assertThatColumnarFilterEvaluationIsSupported(lessThanFilter);
        verifyFilter(inputPages, lessThanFilter);

        // col = constant
        lessThanFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(EQUAL, ImmutableList.of(DOUBLE, DOUBLE)),
                field(DOUBLE_CHANNEL, DOUBLE),
                constant((double) CONSTANT, DOUBLE));
        assertThatColumnarFilterEvaluationIsSupported(lessThanFilter);
        verifyFilter(inputPages, lessThanFilter);

        // colA = colB
        lessThanFilter = call(
                FUNCTION_RESOLUTION.resolveOperator(EQUAL, ImmutableList.of(INTEGER, INTEGER)),
                field(INT_CHANNEL_C, INTEGER),
                field(INT_CHANNEL_A, INTEGER));
        assertThatColumnarFilterEvaluationIsSupported(lessThanFilter);
        verifyFilter(inputPages, lessThanFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testBetween(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // col BETWEEN constantA AND constantB
        RowExpression betweenFilter = new SpecialForm(
                BETWEEN,
                BOOLEAN,
                ImmutableList.of(field(INT_CHANNEL_A, INTEGER), constant(CONSTANT - 5, INTEGER), constant(CONSTANT + 5, INTEGER)),
                ImmutableList.of(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(INTEGER, INTEGER))));
        assertThatColumnarFilterEvaluationIsSupported(betweenFilter);
        verifyFilter(inputPages, betweenFilter);

        // colA BETWEEN colB AND constant
        betweenFilter = new SpecialForm(
                BETWEEN,
                BOOLEAN,
                ImmutableList.of(field(INT_CHANNEL_A, INTEGER), field(INT_CHANNEL_B, INTEGER), constant(CONSTANT + 5, INTEGER)),
                ImmutableList.of(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(INTEGER, INTEGER))));
        assertThatColumnarFilterEvaluationIsSupported(betweenFilter);
        verifyFilter(inputPages, betweenFilter);

        // colA BETWEEN colB AND colC
        betweenFilter = new SpecialForm(
                BETWEEN,
                BOOLEAN,
                ImmutableList.of(field(INT_CHANNEL_A, INTEGER), field(INT_CHANNEL_B, INTEGER), field(INT_CHANNEL_C, INTEGER)),
                ImmutableList.of(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(INTEGER, INTEGER))));
        assertThatColumnarFilterEvaluationIsSupported(betweenFilter);
        verifyFilter(inputPages, betweenFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testOr(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        ResolvedFunction customIsDistinctFrom = FUNCTION_RESOLUTION.functionCallBuilder("custom_is_distinct_from")
                .addArgument(INTEGER, new Reference(INTEGER, "left"))
                .addArgument(INTEGER, new Reference(INTEGER, "right"))
                .build()
                .function();
        RowExpression orFilter = new SpecialForm(
                OR,
                BOOLEAN,
                ImmutableList.of(
                        call(
                                customIsDistinctFrom,
                                field(INT_CHANNEL_A, INTEGER),
                                constant(CONSTANT - 5, INTEGER)),
                        call(
                                customIsDistinctFrom,
                                field(INT_CHANNEL_C, INTEGER),
                                constant(CONSTANT + 5, INTEGER)),
                        call(
                                customIsDistinctFrom,
                                field(INT_CHANNEL_B, INTEGER),
                                constant(CONSTANT, INTEGER))),
                ImmutableList.of());
        assertThatColumnarFilterEvaluationIsSupported(orFilter);
        verifyFilter(inputPages, orFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testAnd(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        ResolvedFunction customIsDistinctFromIntegers = FUNCTION_RESOLUTION.functionCallBuilder("custom_is_distinct_from")
                .addArgument(INTEGER, new Reference(INTEGER, "left"))
                .addArgument(INTEGER, new Reference(INTEGER, "right"))
                .build()
                .function();
        ResolvedFunction customIsDistinctFromVarchars = FUNCTION_RESOLUTION.functionCallBuilder("custom_is_distinct_from")
                .addArgument(VARCHAR, new Reference(VARCHAR, "left"))
                .addArgument(VARCHAR, new Reference(VARCHAR, "right"))
                .build()
                .function();
        RowExpression andFilter = new SpecialForm(
                AND,
                BOOLEAN,
                ImmutableList.of(
                        call(
                                customIsDistinctFromIntegers,
                                field(INT_CHANNEL_A, INTEGER),
                                constant(CONSTANT - 5, INTEGER)),
                        call(
                                customIsDistinctFromVarchars,
                                field(STRING_CHANNEL, VARCHAR),
                                constant(Slices.utf8Slice(Long.toString(CONSTANT + 5)), VARCHAR)),
                        call(
                                customIsDistinctFromIntegers,
                                field(INT_CHANNEL_B, INTEGER),
                                constant(CONSTANT, INTEGER))),
                ImmutableList.of());
        assertThatColumnarFilterEvaluationIsSupported(andFilter);
        verifyFilter(inputPages, andFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testIn(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        List<ResolvedFunction> functionalDependencies = getInFunctionalDependencies(INTEGER);
        // INTEGER type with small number of discontinuous constants
        // Uses switch case
        List<RowExpression> arguments = ImmutableList.<RowExpression>builder()
                .add(field(INT_CHANNEL_A, INTEGER))
                .add(constant(null, INTEGER))
                .add(constant(CONSTANT + 1, INTEGER))
                .add(constant(CONSTANT + 5, INTEGER))
                .add(constant(CONSTANT + 10, INTEGER))
                .build();
        RowExpression inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // INTEGER type with large number of discontinuous constants
        // Uses LongBitSetFilter
        arguments = ImmutableList.<RowExpression>builder()
                .add(field(INT_CHANNEL_A, INTEGER))
                .add(constant(null, INTEGER))
                .add(constant(CONSTANT - 10, INTEGER))
                .addAll(buildConstantsList(INTEGER, 100))
                .add(constant(CONSTANT + 110, INTEGER))
                .build();
        inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // INTEGER type with large number of discontinuous constants from a wide range
        // Uses LongOpenHashSet
        arguments = ImmutableList.<RowExpression>builder()
                .add(field(INT_CHANNEL_A, INTEGER))
                .add(constant(null, INTEGER))
                .add(constant(CONSTANT - 10, INTEGER))
                .addAll(buildConstantsList(INTEGER, 100))
                .add(constant(CONSTANT + 1073741824, INTEGER))
                .build();
        inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // INTEGER type with continuous constants
        arguments = ImmutableList.<RowExpression>builder()
                .add(field(INT_CHANNEL_A, INTEGER))
                .add(constant(null, INTEGER))
                .addAll(buildConstantsList(INTEGER, 100))
                .build();
        inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // INTEGER type with only null constant
        arguments = ImmutableList.<RowExpression>builder()
                .add(field(INT_CHANNEL_A, INTEGER))
                .add(constant(null, INTEGER))
                .build();
        inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // REAL type with large number of discontinuous constants
        // Uses LongOpenCustomHashSet
        arguments = ImmutableList.<RowExpression>builder()
                .add(field(REAL_CHANNEL, REAL))
                .add(constant(null, REAL))
                .add(constant(CONSTANT - 10, REAL))
                .addAll(buildConstantsList(REAL, 100))
                .add(constant(CONSTANT + 110, REAL))
                .build();
        inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        functionalDependencies = getInFunctionalDependencies(VARCHAR);
        // VARCHAR type with small number of constants
        arguments = ImmutableList.<RowExpression>builder()
                .add(field(STRING_CHANNEL, VARCHAR))
                .add(constant(null, VARCHAR))
                .addAll(buildConstantsList(VARCHAR, 3))
                .build();
        inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // VARCHAR type with large number of constants
        arguments = ImmutableList.<RowExpression>builder()
                .add(field(STRING_CHANNEL, VARCHAR))
                .add(constant(null, VARCHAR))
                .addAll(buildConstantsList(VARCHAR, 100))
                .build();
        inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        assertThatColumnarFilterEvaluationIsSupported(inFilter);
        verifyFilter(inputPages, inFilter);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testInStructuralType(NullsProvider nullsProvider)
    {
        List<Page> inputPages = createInputPages(nullsProvider, false);
        List<ResolvedFunction> functionalDependencies = getInFunctionalDependencies(ARRAY_CHANNEL_TYPE);
        // Structural type with indeterminate constants and small list
        List<RowExpression> arguments = ImmutableList.<RowExpression>builder()
                .add(field(ARRAY_CHANNEL, ARRAY_CHANNEL_TYPE))
                .add(constant(null, ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT, null), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT + 2), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT, CONSTANT + 1), ARRAY_CHANNEL_TYPE))
                .build();
        RowExpression inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        // Structural types in "IN" clause are not supported for columnar evaluation yet
        assertThatColumnarFilterEvaluationIsNotSupported(inFilter);
        verifyFilter(inputPages, inFilter);

        // Structural type with indeterminate constants and large list
        arguments = ImmutableList.<RowExpression>builder()
                .add(field(ARRAY_CHANNEL, ARRAY_CHANNEL_TYPE))
                .add(constant(null, ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT, null), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT + 2), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT, CONSTANT + 1), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT, CONSTANT + 1, CONSTANT + 2), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT + 2, null), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT - 2, CONSTANT, CONSTANT - 1), ARRAY_CHANNEL_TYPE))
                .add(constant(createIntArray(CONSTANT, CONSTANT + 1), ARRAY_CHANNEL_TYPE))
                .build();
        inFilter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);
        // Structural types in "IN" clause are not supported for columnar evaluation yet
        assertThatColumnarFilterEvaluationIsNotSupported(inFilter);
        verifyFilter(inputPages, inFilter);
    }

    public enum NullsProvider
    {
        NO_NULLS {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                return Optional.empty();
            }
        },
        NO_NULLS_WITH_MAY_HAVE_NULL {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                return Optional.of(new boolean[positionCount]);
            }
        },
        ALL_NULLS {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                Arrays.fill(nulls, true);
                return Optional.of(nulls);
            }
        },
        RANDOM_NULLS {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                for (int i = 0; i < positionCount; i++) {
                    nulls[i] = RANDOM.nextBoolean();
                }
                return Optional.of(nulls);
            }
        },
        GROUPED_NULLS {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                int maxGroupSize = 23;
                int position = 0;
                while (position < positionCount) {
                    int remaining = positionCount - position;
                    int groupSize = Math.min(RANDOM.nextInt(maxGroupSize) + 1, remaining);
                    Arrays.fill(nulls, position, position + groupSize, RANDOM.nextBoolean());
                    position += groupSize;
                }
                return Optional.of(nulls);
            }
        };

        abstract Optional<boolean[]> getNulls(int positionCount);
    }

    private static Object[][] inputProviders()
    {
        return cartesianProduct(nullsProviders(), trueFalse());
    }

    private static Object[][] nullsProviders()
    {
        return Stream.of(NullsProvider.values()).collect(toDataProvider());
    }

    private static RowExpression createNotExpression(RowExpression expression)
    {
        return call(FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)), expression);
    }

    private static List<Page> processFilter(List<Page> inputPages, boolean columnarEvaluationEnabled, RowExpression filter)
    {
        PageProcessor compiledProcessor = FUNCTION_RESOLUTION.getExpressionCompiler().compilePageProcessor(
                        columnarEvaluationEnabled,
                        Optional.of(filter),
                        Optional.empty(),
                        ImmutableList.of(field(ROW_NUM_CHANNEL, BIGINT)),
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        ImmutableList.Builder<Page> outputPagesBuilder = ImmutableList.builder();
        for (Page inputPage : inputPages) {
            WorkProcessor<Page> workProcessor = compiledProcessor.createWorkProcessor(
                    FULL_CONNECTOR_SESSION,
                    new DriverYieldSignal(),
                    context,
                    new PageProcessorMetrics(),
                    inputPage);
            if (workProcessor.process() && !workProcessor.isFinished()) {
                outputPagesBuilder.add(workProcessor.getResult());
            }
        }
        return outputPagesBuilder.build();
    }

    private static List<Page> createInputPages(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        ImmutableList.Builder<Page> builder = ImmutableList.builder();
        long rowCount = 0;
        for (int pageCount = 0; pageCount < 20; pageCount++) {
            int positionsCount = RANDOM.nextInt(1024, 8192);
            long finalRowCount = rowCount;
            builder.add(new Page(
                    positionsCount,
                    createRowNumberBlock(finalRowCount, positionsCount),
                    lazyBlock(positionsCount, () -> createDoublesBlock(positionsCount, nullsProvider, dictionaryEncoded)),
                    lazyBlock(positionsCount, () -> createIntsBlock(positionsCount, nullsProvider, dictionaryEncoded)),
                    lazyBlock(positionsCount, () -> createStringsBlock(positionsCount, nullsProvider, dictionaryEncoded)),
                    lazyBlock(positionsCount, () -> createIntsBlock(positionsCount, nullsProvider, dictionaryEncoded)),
                    lazyBlock(positionsCount, () -> createIntsBlock(positionsCount, nullsProvider, dictionaryEncoded)),
                    lazyBlock(positionsCount, () -> createArraysBlock(positionsCount, nullsProvider)),
                    lazyBlock(positionsCount, () -> createIntsBlock(positionsCount, nullsProvider, dictionaryEncoded))));
            rowCount += positionsCount;
        }
        return builder.build();
    }

    private static Block lazyBlock(int positionCount, LazyBlockLoader loader)
    {
        return new LazyBlock(positionCount, loader);
    }

    private static Block createRowNumberBlock(long start, int positionsCount)
    {
        long[] values = new long[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            values[i] = start + i;
        }
        return new LongArrayBlock(positionsCount, Optional.empty(), values);
    }

    private static Block createIntsBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 20;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            int[] dictionaryValues = new int[dictionarySize];
            for (int i = 0; i < nonNullDictionarySize; i++) {
                dictionaryValues[i] = toIntExact(CONSTANT - 10 + i);
            }
            Optional<boolean[]> dictionaryIsNull = getDictionaryIsNull(nullsProvider, dictionarySize);
            Block dictionary = new IntArrayBlock(dictionarySize, dictionaryIsNull, dictionaryValues);
            return createDictionaryBlock(positionsCount, nullsProvider, dictionary);
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        int[] values = new int[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isEmpty() || !isNull.get()[i]) {
                values[i] = toIntExact(RANDOM.nextLong(CONSTANT - 10, CONSTANT + 10));
            }
        }
        return new IntArrayBlock(positionsCount, isNull, values);
    }

    private static Block createDoublesBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 200;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            long[] dictionaryValues = new long[dictionarySize];
            for (int i = 0; i < nonNullDictionarySize; i++) {
                dictionaryValues[i] = doubleToLongBits(CONSTANT - 100 + i);
            }
            Optional<boolean[]> dictionaryIsNull = getDictionaryIsNull(nullsProvider, dictionarySize);
            Block dictionary = new LongArrayBlock(dictionarySize, dictionaryIsNull, dictionaryValues);
            return createDictionaryBlock(positionsCount, nullsProvider, dictionary);
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        long[] values = new long[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isEmpty() || !isNull.get()[i]) {
                values[i] = doubleToLongBits(RANDOM.nextDouble(CONSTANT - 100, CONSTANT + 100));
            }
        }
        return new LongArrayBlock(positionsCount, isNull, values);
    }

    private static Block createStringsBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 20;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, dictionarySize, dictionarySize * 10);
            for (int i = 0; i < nonNullDictionarySize; i++) {
                builder.writeEntry(Slices.utf8Slice(Long.toString(CONSTANT - 10 + i)));
            }
            if (containsNulls) {
                builder.appendNull();
            }
            return createDictionaryBlock(positionsCount, nullsProvider, builder.build());
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positionsCount, positionsCount * 10);
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(Slices.utf8Slice(Long.toString(RANDOM.nextLong(CONSTANT - 10, CONSTANT + 10))));
            }
        }
        return builder.build();
    }

    private static Block createArraysBlock(int positionsCount, NullsProvider nullsProvider)
    {
        ArrayBlockBuilder builder = new ArrayBlockBuilder(INTEGER, null, positionsCount);
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        for (int position = 0; position < positionsCount; position++) {
            if (isNull.isPresent() && isNull.get()[position]) {
                builder.appendNull();
            }
            else {
                builder.buildEntry(elementBuilder -> {
                    int valuesCount = RANDOM.nextInt(4);
                    for (int i = 0; i < valuesCount; i++) {
                        INTEGER.writeInt(elementBuilder, toIntExact(CONSTANT + i));
                    }
                    // Add a NULL value in the array 10% of the time
                    if (RANDOM.nextInt(100) < 10) {
                        elementBuilder.appendNull();
                    }
                });
            }
        }
        return builder.build();
    }

    private static Optional<boolean[]> getDictionaryIsNull(NullsProvider nullsProvider, int dictionarySize)
    {
        Optional<boolean[]> dictionaryIsNull = Optional.empty();
        if (nullsProvider != NullsProvider.NO_NULLS) {
            dictionaryIsNull = Optional.of(new boolean[dictionarySize]);
            if (nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL) {
                dictionaryIsNull.get()[dictionarySize - 1] = true;
            }
        }
        return dictionaryIsNull;
    }

    private static Block createDictionaryBlock(int positionsCount, NullsProvider nullsProvider, Block dictionary)
    {
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
        int dictionarySize = dictionary.getPositionCount();
        int nonNullDictionarySize = dictionarySize - (containsNulls ? 1 : 0);
        int[] ids = new int[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                ids[i] = dictionarySize - 1;
            }
            else {
                ids[i] = RANDOM.nextInt(nonNullDictionarySize);
            }
        }
        return DictionaryBlock.create(positionsCount, dictionary, ids);
    }

    private static List<ResolvedFunction> getInFunctionalDependencies(Type type)
    {
        return ImmutableList.of(
                FUNCTION_RESOLUTION.resolveOperator(EQUAL, ImmutableList.of(type, type)),
                FUNCTION_RESOLUTION.resolveOperator(HASH_CODE, ImmutableList.of(type)),
                FUNCTION_RESOLUTION.resolveOperator(INDETERMINATE, ImmutableList.of(type)));
    }

    private static List<RowExpression> buildConstantsList(Type type, int size)
    {
        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        for (long i = 0; i < size; i++) {
            if (type == INTEGER) {
                builder.add(constant(CONSTANT + i, type));
            }
            else if (type == REAL) {
                builder.add(constant(CONSTANT + i, type));
            }
            else if (type == VARCHAR) {
                builder.add(constant(Slices.utf8Slice(Long.toString(RANDOM.nextLong(CONSTANT + i))), type));
            }
            else {
                throw new UnsupportedOperationException();
            }
        }
        return builder.build();
    }

    private static Block createIntArray(Long... values)
    {
        IntArrayBlockBuilder builder = new IntArrayBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                INTEGER.writeInt(builder, toIntExact(value));
            }
        }
        return builder.build();
    }

    private static void verifyFilter(List<Page> inputPages, RowExpression filter)
    {
        // Tests the ColumnarFilter#filterPositionsRange implementation
        verifyFilterInternal(inputPages, filter);

        // Tests the ColumnarFilter#filterPositionsList implementation
        ResolvedFunction customIsDistinctFrom = FUNCTION_RESOLUTION.functionCallBuilder("custom_is_distinct_from")
                .addArgument(INTEGER, new Reference(INTEGER, "left"))
                .addArgument(INTEGER, new Reference(INTEGER, "right"))
                .build()
                .function();
        RowExpression andFilter = new SpecialForm(
                AND,
                BOOLEAN,
                ImmutableList.of(call(customIsDistinctFrom, constant(CONSTANT + 3, INTEGER), field(INT_CHANNEL_A, INTEGER)), filter),
                ImmutableList.of());
        // Adding an IS DISTINCT FROM filter first creates a list of filtered positions as input to
        // the filter implementation being tested while also keeping NULLs as input
        verifyFilterInternal(inputPages, andFilter);
    }

    private static void verifyFilterInternal(List<Page> inputPages, RowExpression filter)
    {
        List<Page> outputPagesExpected = processFilter(inputPages, false, filter);
        List<Page> outputPagesActual = processFilter(inputPages, true, filter);
        assertThat(outputPagesExpected.size()).isEqualTo(outputPagesActual.size());

        for (int pageCount = 0; pageCount < outputPagesActual.size(); pageCount++) {
            assertPageEquals(ImmutableList.of(BIGINT), outputPagesActual.get(pageCount), outputPagesExpected.get(pageCount));
        }
    }

    private static void assertPageEquals(List<Type> types, Page actual, Page expected)
    {
        assertThat(actual.getChannelCount()).isEqualTo(expected.getChannelCount());
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        assertThat(types.size()).isEqualTo(actual.getChannelCount());

        for (int channel = 0; channel < types.size(); channel++) {
            assertBlockEquals(types.get(channel), actual.getBlock(channel), expected.getBlock(channel));
        }
    }

    private static void assertThatColumnarFilterEvaluationIsSupported(RowExpression filterExpression)
    {
        assertThat(createColumnarFilterEvaluator(filterExpression, COMPILER)).isPresent();
    }

    private static void assertThatColumnarFilterEvaluationIsNotSupported(RowExpression filterExpression)
    {
        assertThat(createColumnarFilterEvaluator(filterExpression, COMPILER)).isEmpty();
    }

    @ScalarFunction("custom_is_distinct_from")
    public static final class CustomIsDistinctFrom
    {
        private CustomIsDistinctFrom() {}

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFromLong(@SqlNullable @SqlType("T") Long left, @SqlNullable @SqlType("T") Long right)
        {
            if (left == null && right == null) {
                return false;
            }
            if (left == null || right == null) {
                return true;
            }
            return left.equals(right);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFromSlice(@SqlNullable @SqlType("T") Slice left, @SqlNullable @SqlType("T") Slice right)
        {
            if (left == null && right == null) {
                return false;
            }
            if (left == null || right == null) {
                return true;
            }
            return left.equals(right);
        }
    }

    @ScalarFunction("custom_is_null")
    public static final class NullableReturnFunction
    {
        private NullableReturnFunction() {}

        @LiteralParameters("x")
        @SqlType(StandardTypes.BOOLEAN)
        @SqlNullable
        public static Boolean customIsNullVarchar(@SqlNullable @SqlType("varchar(x)") Slice slice)
        {
            return slice == null ? null : false;
        }
    }

    @ScalarFunction("is_user_admin")
    public static final class ConnectorSessionFunction
    {
        private ConnectorSessionFunction() {}

        @LiteralParameters("x")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isUserAdmin(ConnectorSession session)
        {
            return "admin".equals(session.getUser());
        }
    }

    @ScalarFunction("is_answer_to_universe")
    public static final class InstanceFactoryFunction
    {
        private final long precomputed;

        public InstanceFactoryFunction()
        {
            this.precomputed = Long.parseLong("42");
        }

        @SqlType(StandardTypes.BOOLEAN)
        public boolean isAnswerToUniverse(@SqlType(StandardTypes.INTEGER) long value)
        {
            return precomputed == value;
        }
    }
}
