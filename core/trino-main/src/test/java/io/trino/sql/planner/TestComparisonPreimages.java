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

import io.trino.Session;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.DomainProjection;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionPreimage;
import io.trino.spi.function.PreimageFunctionDependencies;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.sql.planner.iterative.rule.UnwrapFunctionInComparison.unwrap;
import static io.trino.testing.TestingSession.testSession;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestComparisonPreimages
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(InternalFunctionBundle.builder().functions(TestFunctions.class).build());
    private static final Session SESSION = testSession();
    private static final Reference SMALLINT_INPUT = new Reference(SMALLINT, "value");
    private static final Reference BIGINT_INPUT = new Reference(BIGINT, "value");
    private static final AtomicInteger EVALUATIONS = new AtomicInteger();

    private final ComparisonPreimages preimages = new ComparisonPreimages(FUNCTIONS.getPlannerContext(), SESSION);
    private final IrExpressionEvaluator evaluator = new IrExpressionEvaluator(FUNCTIONS.getPlannerContext());

    @Test
    void testWideningIntegralComparisons()
    {
        List<Type> types = List.of(TINYINT, SMALLINT, INTEGER, BIGINT);
        for (int source = 0; source < types.size() - 1; source++) {
            Type sourceType = types.get(source);
            Type.Range bounds = sourceType.getRange().orElseThrow();
            long min = (long) bounds.getMin();
            long max = (long) bounds.getMax();
            List<Long> inputs = List.of(min, min + 1, -1L, 0L, 1L, max - 1, max);
            for (int target = source + 1; target < types.size(); target++) {
                Type targetType = types.get(target);
                for (ComparisonOperator operator : ComparisonOperator.values()) {
                    for (long value : List.of(min - 1, min, min + 1, -1L, 0L, 1L, max - 1, max, max + 1)) {
                        assertEquivalent(compare(operator, new Cast(new Reference(sourceType, "value"), targetType), new Constant(targetType, value)), inputs);
                    }
                    assertEquivalent(compare(operator, new Cast(new Reference(sourceType, "value"), targetType), new Constant(targetType, null)), inputs);
                }
            }
        }
    }

    @Test
    void testTinyintExhaustive()
    {
        List<Long> values = LongStream.rangeClosed(Byte.MIN_VALUE, Byte.MAX_VALUE).boxed().toList();
        for (ComparisonOperator operator : ComparisonOperator.values()) {
            assertEquivalent(compare(operator, new Cast(new Reference(TINYINT, "value"), BIGINT), new Constant(BIGINT, 7L)), values);
        }
    }

    @Test
    void testConservativeProjection()
    {
        Expression predicate = compare(EQUAL, call("preimage_conservative", BIGINT_INPUT), new Constant(BIGINT, 1L));
        assertThat(preimages.rewrite(predicate, emptySymbolAllocator())).isEmpty();
    }

    @Test
    void testNestedExactProjection()
    {
        Expression predicate = compare(EQUAL, call("preimage_identity", new Cast(SMALLINT_INPUT, BIGINT)), new Constant(BIGINT, 1L));
        Expression first = rewrite(predicate);
        Expression second = rewrite(first);
        assertThat(second).isEqualTo(compare(EQUAL, SMALLINT_INPUT, new Constant(SMALLINT, 1L)));
    }

    @Test
    void testNullableInputComparisonsAreUnsupported()
    {
        Reference input = new Reference(RowType.anonymous(List.of(BIGINT)), "value");
        Call function = call("preimage_row", input);
        assertThat(FUNCTIONS.getMetadata().getDomainProjection(SESSION, function.function())).isPresent();
        assertThat(preimages.hasProjection(function)).isFalse();
        assertThat(preimages.rewrite(compare(EQUAL, function, new Constant(BIGINT, 1L)), emptySymbolAllocator())).isEmpty();
    }

    @Test
    void testParametersAndFunctionIdentity()
    {
        assertEquivalent(compare(EQUAL, call("preimage_parameter", BIGINT_INPUT, new Constant(BIGINT, 42L)), new Constant(BIGINT, 1L)), List.of(0L, 1L, 2L));
        for (Expression parameter : List.of(new Constant(BIGINT, 41L), new Constant(BIGINT, null), BIGINT_INPUT)) {
            Expression predicate = compare(EQUAL, call("preimage_parameter", BIGINT_INPUT, parameter), new Constant(BIGINT, 1L));
            assertThat(preimages.rewrite(predicate, emptySymbolAllocator())).isEmpty();
        }
        assertThat(preimages.hasProjection(call("preimage_identity_alias", BIGINT_INPUT))).isTrue();
        assertThat(preimages.hasProjection(call("year", BIGINT_INPUT))).isFalse();
        Expression unrelated = compare(EQUAL, call("year", BIGINT_INPUT), new Constant(BIGINT, 1L));
        assertThat(preimages.rewrite(unrelated, emptySymbolAllocator())).isEmpty();
    }

    @Test
    void testProviderAdmitsInferredArgument()
    {
        // Both arguments have the same type, but this provider only projects the first.
        Expression unsupported = compare(EQUAL, call("preimage_parameter", new Constant(BIGINT, 1L), BIGINT_INPUT), new Constant(BIGINT, 1L));
        assertThat(preimages.rewrite(unsupported, emptySymbolAllocator())).isEmpty();
        assertThat(preimages.extract(unsupported, false)).isEmpty();
        assertThat(preimages.extract(unsupported, true)).isEmpty();
        // Fold parameter expressions before deciding which argument is nonconstant.
        Expression parameter = call("bitwise_xor", new Constant(BIGINT, 40L), new Constant(BIGINT, 2L));
        assertEquivalent(compare(EQUAL, call("preimage_parameter", BIGINT_INPUT, parameter), new Constant(BIGINT, 1L)), List.of(0L, 1L, 2L));
    }

    @Test
    void testProviderCanAdmitEitherArgument()
    {
        for (Call function : List.of(
                call("preimage_xor", BIGINT_INPUT, new Constant(BIGINT, 0L)),
                call("preimage_xor", new Constant(BIGINT, 0L), BIGINT_INPUT))) {
            for (ComparisonOperator operator : ComparisonOperator.values()) {
                assertEquivalent(compare(operator, function, new Constant(BIGINT, 1L)), List.of(Long.MIN_VALUE, -1L, 0L, 1L, 2L, Long.MAX_VALUE));
            }
        }
        for (Call function : List.of(
                call("preimage_xor", BIGINT_INPUT, BIGINT_INPUT),
                call("preimage_xor", BIGINT_INPUT, new Constant(BIGINT, null)),
                call("preimage_xor", new Constant(BIGINT, 0L), new Constant(BIGINT, 1L)))) {
            assertThat(preimages.hasProjection(function)).isFalse();
        }
        assertThat(rewrite(call("preimage_xor", new Constant(BIGINT, 0L), new Constant(BIGINT, 1L)))).isEqualTo(new Constant(BIGINT, 1L));
    }

    @Test
    void testProviderResultsAreValidated()
    {
        Call identity = call("preimage_identity", BIGINT_INPUT);
        Context context = new Context(SESSION.toConnectorSession(), identity.function().signature(), 0, List.of(Optional.empty()), EXACT, new PreimageFunctionDependencies()
        {
            @Override
            public Object invoke(List<Object> arguments)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Function<Object, Object>> coercion(Type source, Type target)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Comparator<Object> resultComparator()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean canCoerce(Type source, Type target)
            {
                throw new UnsupportedOperationException();
            }
        });
        assertThat(new DomainProjection((_, _) -> Optional.empty()).preimage(context, Domain.none(BIGINT))).isEmpty();
        assertThat(new DomainProjection(new IdentityPreimage()).preimage(context, Domain.none(BIGINT)))
                .contains(new PreimageResult(Domain.none(BIGINT), EXACT));
        assertThatThrownBy(() -> new DomainProjection((_, _) -> Optional.of(new PreimageResult(Domain.none(DATE), EXACT)))
                .preimage(context, Domain.none(BIGINT))).hasMessageContaining("preimage type");
        assertThatThrownBy(() -> new DomainProjection((_, _) -> Optional.of(new PreimageResult(Domain.all(BIGINT), EXACT)))
                .preimage(context, Domain.notNull(BIGINT))).hasMessageContaining("null-reflecting");
        assertThatThrownBy(() -> new DomainProjection((_, _) -> Optional.of(new PreimageResult(Domain.none(BIGINT), CONSERVATIVE)))
                .preimage(context, Domain.onlyNull(BIGINT))).hasMessageContaining("excludes a matching null");
    }

    @Test
    void testInvalidRegistration()
    {
        DomainProjection projection = new DomainProjection(new IdentityPreimage());
        assertThatThrownBy(() -> builder().nondeterministic().domainProjection(projection).build()).hasMessageContaining("deterministic scalar");
        assertThatThrownBy(() -> builder().nullable().domainProjection(projection).build()).hasMessageContaining("non-nullable return");
        assertThatThrownBy(() -> builder().argumentNullability(true).domainProjection(projection).build()).hasMessageContaining("null-propagating");
        assertThatThrownBy(() -> FunctionMetadata.scalarBuilder("test_preimage").signature(Signature.builder().returnType(BIGINT).build()).description("").domainProjection(projection).build())
                .hasMessageContaining("nonempty fixed-arity signature");
        assertThatThrownBy(() -> builder().domainProjection(projection).domainProjection(projection)).hasMessageContaining("already declared");
        assertThat(builder().domainProjection(projection).build().getDomainProjection()).contains(projection);
    }

    private static FunctionMetadata.Builder builder()
    {
        return FunctionMetadata.scalarBuilder("test_preimage").signature(Signature.builder().returnType(BIGINT).argumentType(BIGINT).build()).description("").neverFails();
    }

    private void assertEquivalent(Expression original, List<?> inputs)
    {
        Expression rewritten = rewrite(original);
        Expression different = negate(compare(IDENTICAL, original, rewritten));
        for (Object input : inputs) {
            assertThat(evaluator.evaluate(different, SESSION, singletonMap("value", input)))
                    .describedAs("%s versus %s for %s", original, rewritten, input)
                    .isEqualTo(false);
        }
        assertThat(evaluator.evaluate(different, SESSION, singletonMap("value", null)))
                .describedAs("%s versus %s for null", original, rewritten)
                .isEqualTo(false);
    }

    @Test
    void testNumberNaNPreimages()
    {
        Reference input = new Reference(NUMBER, "value");
        Expression function = call("preimage_number", input);
        Object nan = FloatingPointValueSet.nanValue(NUMBER);
        List<Object> values = List.of(
                TrinoNumber.from(BigDecimal.ZERO),
                TrinoNumber.from(BigDecimal.ONE),
                TrinoNumber.from(BigDecimal.ONE.negate()),
                TrinoNumber.from(new TrinoNumber.Infinity(true)),
                TrinoNumber.from(new TrinoNumber.Infinity(false)),
                nan);
        for (ComparisonOperator operator : ComparisonOperator.values()) {
            for (Object constant : List.of(nan, TrinoNumber.from(BigDecimal.ZERO))) {
                assertEquivalent(compare(operator, function, new Constant(NUMBER, constant)), values);
            }
        }
    }

    @Test
    void testIntegralFloatingRoundingFibers()
    {
        for (Type source : List.of(INTEGER, BIGINT)) {
            long min = (long) source.getRange().orElseThrow().getMin();
            long max = (long) source.getRange().orElseThrow().getMax();
            List<Long> inputs = new ArrayList<>();
            for (long distance : List.of(0L, 1L, 2L, 63L, 64L, 65L, 511L, 512L, 513L, (1L << 38) - 1, 1L << 38, (1L << 38) + 1)) {
                if (distance <= max) {
                    inputs.add(min + distance);
                    inputs.add(max - distance);
                }
            }
            for (long boundary : List.of(0L, 1L << 24, 1L << 53)) {
                if (boundary < max) {
                    for (long offset : List.of(-2L, -1L, 0L, 1L, 2L)) {
                        inputs.add(boundary + offset);
                        inputs.add(-boundary + offset);
                    }
                }
            }
            for (Type target : List.of(REAL, DOUBLE)) {
                for (long boundary : List.of(min, max, 0L, -(1L << 24), 1L << 24, -(1L << 53), 1L << 53)) {
                    Object value;
                    if (target.equals(REAL)) {
                        value = (long) floatToRawIntBits((float) boundary);
                    }
                    else {
                        value = (double) boundary;
                    }
                    for (ComparisonOperator operator : ComparisonOperator.values()) {
                        assertEquivalent(compare(operator, new Cast(new Reference(source, "value"), target), new Constant(target, value)), inputs);
                    }
                }
            }
        }
    }

    @Test
    void testLargeInWithNaNPreimage()
    {
        Expression function = call("preimage_nan_to_one", new Reference(DOUBLE, "value"));
        for (int size : List.of(10, 11)) {
            List<Expression> constants = new ArrayList<>();
            constants.add(new Constant(DOUBLE, 1.0));
            for (int value = 3; value <= size + 1; value++) {
                constants.add(new Constant(DOUBLE, (double) value));
            }
            if (size == 10) {
                assertEquivalent(new In(function, constants), List.of(Double.NaN, 0.0, 1.0, 2.0, 3.0, 12.0));
            }
            else {
                for (boolean withNull : List.of(false, true)) {
                    List<Expression> items = new ArrayList<>(constants);
                    if (withNull) {
                        items.add(new Constant(DOUBLE, null));
                    }
                    Expression original = new In(function, items);
                    Expression rewritten = unwrap(FUNCTIONS.getPlannerContext(), SESSION, emptySymbolAllocator(), original);
                    assertThat(evaluator.evaluate(rewritten, SESSION, singletonMap("value", Double.NaN))).isEqualTo(true);
                    assertThat(rewritten).isEqualTo(original);
                    Expression negated = unwrap(FUNCTIONS.getPlannerContext(), SESSION, emptySymbolAllocator(), negate(original));
                    assertThat(evaluator.evaluate(negated, SESSION, singletonMap("value", Double.NaN))).isEqualTo(false);
                }
            }
        }
    }

    @Test
    void testOverlappingFailurePreimages()
    {
        Expression original = compare(EQUAL, call("preimage_nonnegative", BIGINT_INPUT), new Constant(BIGINT, 1L));
        Expression rewritten = unwrap(FUNCTIONS.getPlannerContext(), SESSION, emptySymbolAllocator(), original);
        assertThat(rewritten).isEqualTo(original);
        for (long value : List.of(0L, 1L, 2L)) {
            assertThat(evaluator.evaluate(rewritten, SESSION, singletonMap("value", value))).isEqualTo(value == 1);
        }
    }

    @Test
    void testFloatingPointCasts()
    {
        Reference input = new Reference(REAL, "value");
        List<Long> values = Stream.of(Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, -1.0f, -0.0f, 0.0f, Float.MIN_VALUE, 1.0f, Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.NaN)
                .map(value -> (long) Float.floatToRawIntBits((float) value)).toList();
        for (ComparisonOperator operator : ComparisonOperator.values()) {
            for (double value : List.of(Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -1.1, -1.0, -0.0, 0.0, Double.MIN_VALUE, 1.0, 1.1, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN)) {
                assertEquivalent(compare(operator, new Cast(input, DOUBLE), new Constant(DOUBLE, value)), values);
            }
        }
        for (List<Expression> items : List.of(
                List.<Expression>of(new Constant(DOUBLE, 1.0), new Constant(DOUBLE, Double.NaN)),
                List.<Expression>of(new Constant(DOUBLE, 1.0), new Constant(DOUBLE, null)))) {
            Expression predicate = new In(new Cast(input, DOUBLE), items);
            assertEquivalent(predicate, values);
            assertEquivalent(negate(predicate), values);
        }
    }

    @Test
    void testLargeInWithoutExpansion()
    {
        List<Expression> constants = LongStream.range(0, 100).mapToObj(value -> (Expression) new Constant(BIGINT, value)).toList();
        Expression original = new In(new Cast(SMALLINT_INPUT, BIGINT), constants);
        assertThat(preimages.rewrite(original, emptySymbolAllocator())).hasValueSatisfying(value -> assertThat(value).isInstanceOf(In.class));
        assertEquivalent(original, List.of(-1L, 0L, 50L, 99L, 100L));
    }

    @Test
    void testTimePrecisionCast()
    {
        var source = TimeType.createTimeType(3);
        var target = TimeType.createTimeType(12);
        Reference input = new Reference(source, "value");
        List<Long> values = List.of(0L, 1_000_000_000L, 1_000_000_000_000L, (long) source.getRange().orElseThrow().getMax() - 1_000_000_000L);
        for (ComparisonOperator operator : ComparisonOperator.values()) {
            for (long value : List.of(0L, 1L, 1_000_000_000L, 1_000_000_000_001L, (long) target.getRange().orElseThrow().getMax() - 1)) {
                assertEquivalent(compare(operator, new Cast(input, target), new Constant(target, value)), values);
            }
        }
    }

    private static Block array(Type elementType, Object... values)
    {
        BlockBuilder builder = elementType.createBlockBuilder(null, values.length);
        for (Object value : values) {
            writeNativeValue(elementType, builder, value);
        }
        return builder.build();
    }

    private static SqlRow row(RowType type, Object... values)
    {
        Block[] fields = new Block[values.length];
        for (int index = 0; index < values.length; index++) {
            fields[index] = array(type.getFieldTypes().get(index), values[index]);
        }
        return new SqlRow(0, fields);
    }

    private Expression rewrite(Expression expression)
    {
        Expression rewritten = unwrap(FUNCTIONS.getPlannerContext(), SESSION, emptySymbolAllocator(), expression);
        assertThat(rewritten).isNotEqualTo(expression);
        return rewritten;
    }

    private static Expression compare(ComparisonOperator operator, Expression left, Expression right)
    {
        return comparison(FUNCTIONS.getMetadata(), getCharVarcharCoercion(SESSION), operator, left, right);
    }

    private static Expression negate(Expression expression)
    {
        return not(FUNCTIONS.getMetadata(), getCharVarcharCoercion(SESSION), expression);
    }

    private static Call call(String name, Expression... arguments)
    {
        return new Call(FUNCTIONS.resolveFunction(name, fromTypes(List.of(arguments).stream().map(Expression::type).toList())), List.of(arguments));
    }

    private static long date(String date)
    {
        return LocalDate.parse(date).toEpochDay();
    }

    public static final class TestFunctions
    {
        @ScalarFunction(value = "preimage_nan_to_one", neverFails = true)
        @FunctionPreimage(NaNToOnePreimage.class)
        @SqlType("double")
        public static double nanToOne(@SqlType("double") double value)
        {
            if (Double.isNaN(value)) {
                return 1;
            }
            return value == 1 ? 2 : value;
        }

        @ScalarFunction("preimage_nonnegative")
        @FunctionPreimage(OverlappingFailurePreimage.class)
        @SqlType("bigint")
        public static long nonnegative(@SqlType("bigint") long value)
        {
            if (value < 0) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "negative input");
            }
            return value;
        }

        @ScalarFunction(value = "preimage_number", neverFails = true)
        @FunctionPreimage(IdentityPreimage.class)
        @SqlType("number")
        public static TrinoNumber number(@SqlType("number") TrinoNumber value)
        {
            return value;
        }

        @ScalarFunction(value = "preimage_xor", neverFails = true)
        @FunctionPreimage(XorPreimage.class)
        @SqlType("bigint")
        public static long xor(@SqlType("bigint") long first, @SqlType("bigint") long second)
        {
            return first ^ second;
        }

        @ScalarFunction(value = "preimage_identity", alias = "preimage_identity_alias", neverFails = true)
        @FunctionPreimage(IdentityPreimage.class)
        @SqlType("bigint")
        public static long identity(@SqlType("bigint") long value)
        {
            return value;
        }

        @ScalarFunction(value = "preimage_conservative", neverFails = true)
        @FunctionPreimage(ConservativePreimage.class)
        @SqlType("bigint")
        public static long conservative(@SqlType("bigint") long value)
        {
            return value;
        }

        @ScalarFunction(value = "preimage_parameter", neverFails = true)
        @FunctionPreimage(ParameterPreimage.class)
        @SqlType("bigint")
        public static long parameter(@SqlType("bigint") long value, @SqlType("bigint") long parameter)
        {
            return value;
        }

        @ScalarFunction(value = "year", neverFails = true)
        @SqlType("bigint")
        public static long unrelatedYear(@SqlType("bigint") long value)
        {
            return value;
        }

        @ScalarFunction(value = "preimage_row", neverFails = true)
        @FunctionPreimage(RowPreimage.class)
        @SqlType("bigint")
        public static long row(@SqlType("row(bigint)") SqlRow value)
        {
            return 1;
        }

        @ScalarFunction(value = "preimage_counter_date", deterministic = false, neverFails = true)
        @SqlType("date")
        public static long counterDate()
        {
            EVALUATIONS.incrementAndGet();
            return date("2025-06-01");
        }

        @ScalarFunction(value = "preimage_counter_array", deterministic = false, neverFails = true)
        @SqlType("array(integer)")
        public static Block counterArray()
        {
            EVALUATIONS.incrementAndGet();
            return array(INTEGER, 1L);
        }

        @ScalarFunction("preimage_fail_date")
        @SqlType("date")
        public static long failingDate(@SqlType("date") long value)
        {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "expected failure");
        }
    }

    public static final class NaNToOnePreimage
            implements DomainPreimage
    {
        @Override
        public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
        {
            ValueSet values = resultDomain.getValues().subtract(ValueSet.of(DOUBLE, 1.0, Double.NaN));
            if (resultDomain.includesNullableValue(2.0)) {
                values = values.union(ValueSet.of(DOUBLE, 1.0));
            }
            Domain input = Domain.create(values, resultDomain.isNullAllowed());
            if (resultDomain.includesNullableValue(1.0)) {
                input = input.union(Domain.singleValue(DOUBLE, Double.NaN));
            }
            return Optional.of(new PreimageResult(input, EXACT));
        }
    }

    public static final class OverlappingFailurePreimage
            implements DomainPreimage
    {
        @Override
        public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
        {
            Domain failures = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L)), false);
            return Optional.of(new PreimageResult(resultDomain.union(failures), EXACT));
        }
    }

    public static final class IdentityPreimage
            implements DomainPreimage
    {
        @Override
        public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
        {
            return Optional.of(new PreimageResult(resultDomain, EXACT));
        }
    }

    public static final class ConservativePreimage
            implements DomainPreimage
    {
        @Override
        public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
        {
            return Optional.of(new PreimageResult(resultDomain.union(Domain.singleValue(BIGINT, 7L)), CONSERVATIVE));
        }
    }

    public static final class RowPreimage
            implements DomainPreimage
    {
        @Override
        public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
        {
            Type inputType = context.signature().getArgumentTypes().getFirst();
            return Optional.of(new PreimageResult(Domain.create(
                    resultDomain.includesNullableValue(1L) ? ValueSet.all(inputType) : ValueSet.none(inputType),
                    resultDomain.isNullAllowed()), EXACT));
        }
    }

    public static final class ParameterPreimage
            implements DomainPreimage
    {
        @Override
        public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
        {
            if (context.inputArgument() != 0 || !context.arguments().get(1).orElseThrow().getValue().equals(42L)) {
                return Optional.empty();
            }
            return Optional.of(new PreimageResult(resultDomain, EXACT));
        }
    }

    public static final class XorPreimage
            implements DomainPreimage
    {
        @Override
        public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
        {
            if (!context.arguments().get(1 - context.inputArgument()).orElseThrow().getValue().equals(0L)) {
                return Optional.empty();
            }
            return Optional.of(new PreimageResult(resultDomain, EXACT));
        }
    }
}
