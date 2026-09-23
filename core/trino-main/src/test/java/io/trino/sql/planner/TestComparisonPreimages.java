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
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestComparisonPreimages
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(InternalFunctionBundle.builder().functions(TestFunctions.class).build());
    private static final Session SESSION = testSession();
    private static final Reference BIGINT_INPUT = new Reference(BIGINT, "value");
    private static final AtomicInteger EVALUATIONS = new AtomicInteger();

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
