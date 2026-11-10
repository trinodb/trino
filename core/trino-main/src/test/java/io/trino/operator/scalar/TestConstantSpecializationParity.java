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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionSpecializationContext;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Verifies that each built-in function with a constant specialization returns the same
 * result when the constant arguments are literals as when they are runtime values.
 */
@TestInstance(PER_CLASS)
public class TestConstantSpecializationParity
{
    private static final TimeZoneKey ZONE = TimeZoneKey.getTimeZoneKey("America/Los_Angeles");
    private static final long MILLIS = LocalDateTime.of(2022, 5, 10, 12, 34, 56, 789_000_000).toInstant(ZoneOffset.UTC).toEpochMilli();
    private static final long DATE = LocalDate.of(2022, 5, 10).toEpochDay();

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @Test
    public void testDateFunctions()
    {
        List<Typed> temporalValues = List.of(
                new Typed(DateType.DATE, DATE),
                new Typed(TIMESTAMP_MILLIS, MILLIS * 1000),
                new Typed(TIMESTAMP_NANOS, new LongTimestamp(MILLIS * 1000, 123_000)),
                new Typed(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(MILLIS, ZONE)),
                new Typed(TIMESTAMP_TZ_NANOS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(MILLIS, 123_000_000, ZONE)));

        for (String unit : List.of("day", "month", "year")) {
            for (Typed value : temporalValues) {
                assertParity("date_trunc", Set.of(0), varchar(unit), value);
                assertParity("date_add", Set.of(0), varchar(unit), new Typed(BIGINT, 3L), value);
                assertParity("date_add", Set.of(0, 1), varchar(unit), new Typed(BIGINT, -2L), value);
                assertParity("date_diff", Set.of(0), varchar(unit), value, value);
            }
        }
    }

    @Test
    public void testTimestampFormatFunctions()
    {
        List<Typed> timestamps = List.of(
                new Typed(TIMESTAMP_MILLIS, MILLIS * 1000),
                new Typed(TIMESTAMP_NANOS, new LongTimestamp(MILLIS * 1000, 123_000)),
                new Typed(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(MILLIS, ZONE)),
                new Typed(TIMESTAMP_TZ_NANOS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(MILLIS, 123_000_000, ZONE)));

        for (Typed timestamp : timestamps) {
            assertParity("date_format", Set.of(1), timestamp, varchar("%Y-%m-%d %H:%i:%s"));
            assertParity("format_datetime", Set.of(1), timestamp, varchar("yyyy-MM-dd HH:mm:ss"));
        }
    }

    @Test
    public void testParseFunctions()
    {
        assertParity("date_parse", Set.of(1), varchar("2020-05-10"), varchar("%Y-%m-%d"));
        assertParity("date_parse", Set.of(1), varchar("12:34:56"), varchar("%H:%i:%s"));
        assertParity("parse_datetime", Set.of(1), varchar("2020-05-10 12:34 +0000"), varchar("yyyy-MM-dd HH:mm Z"));
    }

    @Test
    public void testStringFunctions()
    {
        assertParity("word_stem", Set.of(1), varchar("authorized"), varchar("en"));
        assertParity("translate", Set.of(1, 2), varchar("abcda"), varchar("a"), varchar("z"));
        assertParity("normalize", Set.of(1), varchar("café"), varchar("NFC"));
        assertParity("from_utf8", Set.of(1), new Typed(VarbinaryType.VARBINARY, wrappedBuffer((byte) 0x58, (byte) 0xBF)), varchar("#"));
        assertParity("from_utf8", Set.of(1), new Typed(VarbinaryType.VARBINARY, wrappedBuffer((byte) 0x58, (byte) 0xBF)), new Typed(BIGINT, 35L));
    }

    private void assertParity(String functionName, Set<Integer> constantPositions, Typed... arguments)
    {
        List<Type> types = List.of(arguments).stream().map(Typed::type).toList();
        ResolvedFunction function = functionResolution.resolveFunction(functionName, fromTypes(types));

        Object runtimeResult = evaluate(function, arguments, Set.of());
        Object constantResult = evaluate(function, arguments, constantPositions);
        assertThat(constantResult)
                .describedAs("%s(%s) with constant positions %s", functionName, types, constantPositions)
                .isEqualTo(runtimeResult);

        assertThat(selectsSpecialization(function, arguments, constantPositions))
                .describedAs("%s(%s) uses a constant specialization for positions %s", functionName, types, constantPositions)
                .isTrue();
    }

    private Object evaluate(ResolvedFunction function, Typed[] arguments, Set<Integer> constantPositions)
    {
        List<Expression> expressions = new ArrayList<>();
        List<Block> columns = new ArrayList<>();
        Map<Symbol, Integer> layout = new HashMap<>();
        for (int index = 0; index < arguments.length; index++) {
            if (constantPositions.contains(index)) {
                expressions.add(new Constant(arguments[index].type(), arguments[index].value()));
            }
            else {
                String name = "c" + index;
                layout.put(new Symbol(arguments[index].type(), name), columns.size());
                expressions.add(new Reference(arguments[index].type(), name));
                columns.add(block(arguments[index]));
            }
        }

        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(new Call(function, expressions), layout, SQL_STANDARD, Optional.empty())
                .get();
        Page page = columns.isEmpty() ? new Page(1) : new Page(columns.toArray(Block[]::new));
        SourcePage inputPage = projection.getInputChannels().getInputChannels(SourcePage.create(page));
        Block result = projection.project(SESSION, inputPage, SelectedPositions.positionsRange(0, 1));
        return function.signature().getReturnType().getObjectValue(result, 0);
    }

    private boolean selectsSpecialization(ResolvedFunction function, Typed[] arguments, Set<Integer> constantPositions)
    {
        List<Optional<io.trino.spi.expression.Constant>> constants = IntStream.range(0, arguments.length)
                .mapToObj(index -> constantPositions.contains(index)
                        ? Optional.of(new io.trino.spi.expression.Constant(arguments[index].value(), arguments[index].type()))
                        : Optional.<io.trino.spi.expression.Constant>empty())
                .toList();
        InvocationConvention convention = new InvocationConvention(
                nCopies(arguments.length, NEVER_NULL),
                FAIL_ON_NULL,
                true,
                true);
        return functionResolution.getPlannerContext().getFunctionManager()
                .getConstantSpecializedScalarFunctionImplementation(function, new ScalarFunctionSpecializationContext(convention, constants))
                .isPresent();
    }

    private static Block block(Typed typed)
    {
        Type type = typed.type();
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class) {
            type.writeLong(builder, (Long) typed.value());
        }
        else if (javaType == Slice.class) {
            type.writeSlice(builder, (Slice) typed.value());
        }
        else {
            type.writeObject(builder, typed.value());
        }
        return builder.build();
    }

    private static Typed varchar(String value)
    {
        return new Typed(createVarcharType(value.length()), utf8Slice(value));
    }

    private record Typed(Type type, Object value) {}
}
