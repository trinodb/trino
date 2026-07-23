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
package io.trino.plugin.trino;

import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.IpAddressType;
import io.trino.type.JsonType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTrinoParameterBindingFactory
{
    @Test
    void testCreateWriteMappingUsesTypedTemporalTransports()
    {
        TimeType time = createTimeType(12);
        WriteMapping timeMapping = TrinoParameterBindingFactory.createWriteMapping(time).orElseThrow();
        assertThat(timeMapping.getDataType()).isEqualTo("time(12)");
        assertThat(timeMapping.getWriteFunction().getBindExpression()).isEqualTo("CAST(? AS time(12))");

        TimestampWithTimeZoneType shortTimestampWithTimeZone = createTimestampWithTimeZoneType(3);
        WriteMapping shortMapping = TrinoParameterBindingFactory.createWriteMapping(shortTimestampWithTimeZone).orElseThrow();
        assertThat(shortMapping.getDataType()).isEqualTo("timestamp(3) with time zone");
        assertThat(shortMapping.getWriteFunction().getBindExpression()).isEqualTo("CAST(? AS timestamp(3) with time zone)");

        TimestampWithTimeZoneType longTimestampWithTimeZone = createTimestampWithTimeZoneType(12);
        WriteMapping longMapping = TrinoParameterBindingFactory.createWriteMapping(longTimestampWithTimeZone).orElseThrow();
        assertThat(longMapping.getDataType()).isEqualTo("timestamp(12) with time zone");
        assertThat(longMapping.getWriteFunction().getBindExpression()).isEqualTo("CAST(? AS timestamp(12) with time zone)");
    }

    @Test
    void testBindConstantPreservesLogicalType()
    {
        assertBinding(
                new Constant(utf8Slice("x"), createCharType(3)),
                "CAST(? AS char(3))");
        assertBinding(
                new Constant(1234L, createDecimalType(6, 2)),
                "CAST(? AS decimal(6,2))");
        assertBinding(
                new Constant(1_234_567_890_123L, createTimeType(12)),
                "CAST(? AS time(12))");
        assertBinding(
                new Constant(new LongTimestamp(1_234_567L, 890_123), createTimestampType(12)),
                "CAST(? AS timestamp(12))");
        assertBinding(
                new Constant(TrinoNumber.from(new BigDecimal("123.45")), NUMBER),
                "CAST(? AS number)");
        assertBinding(
                new Constant(14L, IntervalYearMonthType.INTERVAL_YEAR_MONTH),
                "CAST(INTERVAL '1' MONTH * CAST(? AS INTEGER) AS interval year to month)");
        assertBinding(
                new Constant(1_234L, IntervalDayTimeType.INTERVAL_DAY_TIME),
                "CAST(INTERVAL '0.001' SECOND * CAST(? AS BIGINT) AS interval day to second)");
    }

    @Test
    void testDayToSecondIntervalUsesExactSignedBigintBinding()
            throws Exception
    {
        long value = -9_007_199_254_740_993L;
        LongWriteFunction writeFunction = TemporalTransportCodec.intervalTransportWriteFunction(IntervalDayTimeType.INTERVAL_DAY_TIME);
        AtomicReference<String> invokedMethod = new AtomicReference<>();
        AtomicLong boundValue = new AtomicLong();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (_, method, arguments) -> {
                    if (method.getName().startsWith("set")) {
                        invokedMethod.set(method.getName());
                        boundValue.set((long) arguments[1]);
                    }
                    return null;
                });

        writeFunction.set(statement, 1, value);

        assertThat(writeFunction.getBindExpression()).isEqualTo("INTERVAL '0.001' SECOND * CAST(? AS BIGINT)");
        assertThat(invokedMethod).hasValue("setLong");
        assertThat(boundValue).hasValue(value);
    }

    @Test
    void testTimestampTransportAcceptsExplicitExpandedYearSign()
    {
        assertThat(TemporalTransportCodec.parseShortTimestamp("+10000-01-01 00:00:00"))
                .isEqualTo(LocalDateTime.of(10_000, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L);
        assertThat(TemporalTransportCodec.parseShortTimestamp("-10000-01-01 00:00:00"))
                .isEqualTo(LocalDateTime.of(-10_000, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L);

        LongTimestamp expanded = TemporalTransportCodec.parseLongTimestamp("+10000-01-01 00:00:00.123456789012");
        assertThat(expanded.getEpochMicros())
                .isEqualTo(LocalDateTime.of(10_000, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 123_456L);
        assertThat(expanded.getPicosOfMicro()).isEqualTo(789_012);
    }

    @Test
    void testDateTransportAcceptsUnsignedExpandedYear()
    {
        assertThat(TemporalTransportCodec.parseDate("10000-01-01"))
                .isEqualTo(LocalDate.of(10_000, 1, 1));
        assertThat(TemporalTransportCodec.parseDate("+10000-01-01"))
                .isEqualTo(LocalDate.of(10_000, 1, 1));
        assertThat(TemporalTransportCodec.parseDate("-10000-01-01"))
                .isEqualTo(LocalDate.of(-10_000, 1, 1));
    }

    @Test
    void testBindTimestampWithTimeZoneConstantPreservesInstantAndZone()
    {
        TimeZoneKey zone = TimeZoneKey.getTimeZoneKey("Europe/Warsaw");

        TimestampWithTimeZoneType shortType = createTimestampWithTimeZoneType(3);
        long shortValue = DateTimeEncoding.packDateTimeWithZone(1_667_093_400_000L, zone);
        ParameterizedExpression shortBinding = TrinoParameterBindingFactory.bindConstant(new Constant(shortValue, shortType)).orElseThrow();
        assertTimestampWithTimeZoneBinding(shortBinding, shortType, shortValue, "Europe/Warsaw");

        TimestampWithTimeZoneType longType = createTimestampWithTimeZoneType(12);
        LongTimestampWithTimeZone longValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                1_667_093_400_123L,
                456_789_000,
                zone);
        ParameterizedExpression longBinding = TrinoParameterBindingFactory.bindConstant(new Constant(longValue, longType)).orElseThrow();
        assertTimestampWithTimeZoneBinding(longBinding, longType, longValue, "Europe/Warsaw");
    }

    @Test
    void testUnsupportedConstantsFallBackLocally()
    {
        List<Type> unsupportedTypes = List.of(
                UuidType.UUID,
                IpAddressType.IPADDRESS,
                JsonType.JSON,
                new ArrayType(BIGINT));

        for (Type type : unsupportedTypes) {
            assertThat(TrinoParameterBindingFactory.createWriteMapping(type)).isEmpty();
            assertThat(TrinoParameterBindingFactory.bindConstant(new Constant(new Object(), type))).isEmpty();
        }
    }

    @Test
    void testNullConstantUsesTypedLiteralWithoutParameter()
    {
        ParameterizedExpression binding = TrinoParameterBindingFactory.bindConstant(new Constant(null, new ArrayType(BIGINT))).orElseThrow();

        assertThat(binding.expression()).isEqualTo("CAST(NULL AS array(bigint))");
        assertThat(binding.parameters()).isEmpty();
    }

    private static void assertBinding(Constant constant, String expectedExpression)
    {
        ParameterizedExpression binding = TrinoParameterBindingFactory.bindConstant(constant).orElseThrow();
        assertThat(binding.expression()).isEqualTo(expectedExpression);
        assertThat(binding.parameters()).hasSize(1);
        assertThat(binding.parameters().getFirst().getType()).isEqualTo(constant.getType());
        assertThat(binding.parameters().getFirst().getValue()).contains(constant.getValue());
    }

    private static void assertTimestampWithTimeZoneBinding(
            ParameterizedExpression binding,
            TimestampWithTimeZoneType type,
            Object value,
            String zoneId)
    {
        assertThat(binding.expression()).isEqualTo(
                "at_timezone(CAST(? AS " + type.getDisplayName() + "), CAST(? AS varchar))");
        assertThat(binding.parameters()).hasSize(2);
        assertThat(binding.parameters().getFirst().getType()).isEqualTo(type);
        assertThat(binding.parameters().getFirst().getValue()).contains(value);
        assertThat(binding.parameters().get(1).getType()).isEqualTo(VARCHAR);
        assertThat(binding.parameters().get(1).getValue()).contains(utf8Slice(zoneId));
    }
}
