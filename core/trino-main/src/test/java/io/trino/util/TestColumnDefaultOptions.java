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
package io.trino.util;

import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.DateTimes.longTimestampWithTimeZone;
import static io.trino.util.ColumnDefaultOptions.evaluateDefaultValue;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

final class TestColumnDefaultOptions
{
    private static final NodeLocation LOCATION = new NodeLocation(1, 1);

    private final PlannerContext plannerContext;

    public TestColumnDefaultOptions()
    {
        plannerContext = plannerContextBuilder().build();
    }

    @Test
    void testNull()
    {
        assertLiteral(BOOLEAN, new NullLiteral(LOCATION), null);
    }

    @Test
    void testBoolean()
    {
        assertLiteral(BOOLEAN, new BooleanLiteral(LOCATION, "true"), true);
        assertLiteral(BOOLEAN, new BooleanLiteral(LOCATION, "false"), false);
    }

    @Test
    void testTinyint()
    {
        assertLiteral(TINYINT, new LongLiteral(LOCATION, "-128"), -128L);
        assertLiteral(TINYINT, new LongLiteral(LOCATION, "127"), 127L);

        assertInvalidLiteralFails(TINYINT, new LongLiteral(LOCATION, "-129"), "Out of range for tinyint: -129");
        assertInvalidLiteralFails(TINYINT, new LongLiteral(LOCATION, "128"), "Out of range for tinyint: 128");
    }

    @Test
    void testSmallint()
    {
        assertLiteral(SMALLINT, new LongLiteral(LOCATION, "-32768"), -32768L);
        assertLiteral(SMALLINT, new LongLiteral(LOCATION, "32767"), 32767L);

        assertInvalidLiteralFails(SMALLINT, new LongLiteral(LOCATION, "-32769"), " Out of range for smallint: -32769");
        assertInvalidLiteralFails(SMALLINT, new LongLiteral(LOCATION, "32768"), " Out of range for smallint: 32768");
    }

    @Test
    void testInteger()
    {
        assertLiteral(INTEGER, new LongLiteral(LOCATION, "-2147483648"), -2147483648L);
        assertLiteral(INTEGER, new LongLiteral(LOCATION, "2147483647"), 2147483647L);

        assertInvalidLiteralFails(INTEGER, new LongLiteral(LOCATION, "-2147483649"), "Out of range for integer: -2147483649");
        assertInvalidLiteralFails(INTEGER, new LongLiteral(LOCATION, "2147483648"), "Out of range for integer: 2147483648");
    }

    @Test
    void testBigint()
    {
        assertLiteral(BIGINT, new LongLiteral(LOCATION, "-9223372036854775808"), -9223372036854775808L);
        assertLiteral(BIGINT, new LongLiteral(LOCATION, "9223372036854775807"), 9223372036854775807L);

        // LongLiteral disallows values outside the range of a long
    }

    @Test
    void testReal()
    {
        assertLiteral(REAL, new GenericLiteral(LOCATION, "REAL", "3.14"), (long) floatToIntBits(3.14f));
        assertLiteral(REAL, new GenericLiteral(LOCATION, "REAL", "10.3e0"), (long) floatToIntBits(10.3e0f));
        assertLiteral(REAL, new LongLiteral(LOCATION, "123"), (long) floatToIntBits(123.0f));

        assertThat(intBitsToFloat(toIntExact((Long) evaluate(new GenericLiteral(LOCATION, "REAL", "NaN"), REAL).getValue())))
                .isNaN();
        assertThat(intBitsToFloat(toIntExact((Long) evaluate(new GenericLiteral(LOCATION, "REAL", "+Infinity"), REAL).getValue())))
                .isInfinite();
        assertThat(intBitsToFloat(toIntExact((Long) evaluate(new GenericLiteral(LOCATION, "REAL", "-Infinity"), REAL).getValue())))
                .isInfinite();
    }

    @Test
    void testDouble()
    {
        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "3.14"), 3.14);
        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "1.0E100"), 1.0E100);
        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "1.23456E12"), 1.23456E12);
        assertLiteral(DOUBLE, new LongLiteral(LOCATION, "123"), 123.0);

        assertThat((Double) evaluate(new DoubleLiteral(LOCATION, "NaN"), DOUBLE).getValue())
                .isNaN();
        assertThat((Double) evaluate(new DoubleLiteral(LOCATION, "+Infinity"), DOUBLE).getValue())
                .isInfinite();
        assertThat((Double) evaluate(new DoubleLiteral(LOCATION, "-Infinity"), DOUBLE).getValue())
                .isInfinite();
    }

    @Test
    void testDecimal()
    {
        assertLiteral(createDecimalType(3), new DecimalLiteral(LOCATION, "193"), 193L);
        assertLiteral(createDecimalType(3), new DecimalLiteral(LOCATION, "-193"), -193L);
        assertLiteral(createDecimalType(3, 1), new DecimalLiteral(LOCATION, "10.0"), 100L);
        assertLiteral(createDecimalType(3, 1), new DecimalLiteral(LOCATION, "-10.1"), -101L);
        assertLiteral(createDecimalType(30, 5), new DecimalLiteral(LOCATION, "3141592653589793238462643.38327"), Int128.valueOf("314159265358979323846264338327"));
        assertLiteral(createDecimalType(30, 5), new DecimalLiteral(LOCATION, "-3141592653589793238462643.38327"), Int128.valueOf("-314159265358979323846264338327"));
        assertLiteral(createDecimalType(38), new DecimalLiteral(LOCATION, "27182818284590452353602874713526624977"), Int128.valueOf("27182818284590452353602874713526624977"));
        assertLiteral(createDecimalType(38), new DecimalLiteral(LOCATION, "-27182818284590452353602874713526624977"), Int128.valueOf("-27182818284590452353602874713526624977"));
    }

    @Test
    void testInvalidDecimal()
    {
        assertInvalidLiteralFails(
                createDecimalType(3, 2),
                new DecimalLiteral(new NodeLocation(1, 1), "12.456"),
                "Cannot cast DECIMAL(5, 3) '12.456' to DECIMAL(3, 2)");

        assertInvalidLiteralFails(
                createDecimalType(19, 0),
                new DecimalLiteral(new NodeLocation(1, 1), "12345678901234567890"),
                "Cannot cast DECIMAL(20, 0) '12345678901234567890' to DECIMAL(19, 0)");
    }

    @Test
    void testChar()
    {
        assertLiteral(createCharType(4), new StringLiteral(LOCATION, "test"), utf8Slice("test"));
        assertLiteral(createCharType(10), new StringLiteral(LOCATION, "test"), utf8Slice("test"));
        assertLiteral(createCharType(5), new StringLiteral(LOCATION, "攻殻機動隊"), utf8Slice("攻殻機動隊"));
        assertLiteral(createCharType(1), new StringLiteral(LOCATION, "😂"), utf8Slice("😂"));

        // Trim trailing spaces
        assertLiteral(createCharType(4), new StringLiteral(LOCATION, "test "), utf8Slice("test"));
    }

    @Test
    void testInvalidChar()
    {
        assertInvalidLiteralFails(
                createCharType(4),
                new StringLiteral(new NodeLocation(1, 1), " abcd"),
                "Cannot truncate non-space characters when casting from varchar(5) to char(4) on INSERT");
    }

    @Test
    void testVarchar()
    {
        assertLiteral(createVarcharType(4), new StringLiteral(LOCATION, "test"), utf8Slice("test"));
        assertLiteral(createVarcharType(10), new StringLiteral(LOCATION, "test"), utf8Slice("test"));
        assertLiteral(VARCHAR, new StringLiteral(LOCATION, "test"), utf8Slice("test"));
        assertLiteral(createVarcharType(5), new StringLiteral(LOCATION, "攻殻機動隊"), utf8Slice("攻殻機動隊"));
        assertLiteral(createVarcharType(1), new StringLiteral(LOCATION, "😂"), utf8Slice("😂"));

        // Trim trailing spaces
        assertLiteral(createVarcharType(4), new StringLiteral(LOCATION, "test "), utf8Slice("test"));
    }

    @Test
    void testInvalidVarchar()
    {
        assertInvalidLiteralFails(
                createVarcharType(4),
                new StringLiteral(new NodeLocation(1, 1), " abcd"),
                "Cannot truncate non-space characters when casting from varchar(5) to varchar(4) on INSERT");
    }

    @Test
    void testTime()
    {
        assertLiteral(createTimeType(0), new GenericLiteral(LOCATION, "TIME", "00:00:01"), 1000000000000L);

        assertLiteral(createTimeType(0), new GenericLiteral(LOCATION, "TIME", "00:00:00"), 0L);
        assertLiteral(createTimeType(1), new GenericLiteral(LOCATION, "TIME", "00:00:00.1"), 100000000000L);
        assertLiteral(createTimeType(2), new GenericLiteral(LOCATION, "TIME", "00:00:00.12"), 120000000000L);
        assertLiteral(createTimeType(3), new GenericLiteral(LOCATION, "TIME", "00:00:00.123"), 123000000000L);
        assertLiteral(createTimeType(4), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234"), 123400000000L);
        assertLiteral(createTimeType(5), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345"), 123450000000L);
        assertLiteral(createTimeType(6), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456"), 123456000000L);
        assertLiteral(createTimeType(7), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234567"), 123456700000L);
        assertLiteral(createTimeType(8), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345678"), 123456780000L);
        assertLiteral(createTimeType(9), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456789"), 123456789000L);
        assertLiteral(createTimeType(10), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234567890"), 123456789000L);
        assertLiteral(createTimeType(11), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345678901"), 123456789010L);
        assertLiteral(createTimeType(12), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456789012"), 123456789012L);
    }

    @Test
    void testDate()
    {
        assertLiteral(DATE, new GenericLiteral(LOCATION, "DATE", "0001-01-01"), LocalDate.parse("0001-01-01").toEpochDay());
        assertLiteral(DATE, new GenericLiteral(LOCATION, "DATE", "1969-12-31"), LocalDate.parse("1969-12-31").toEpochDay());
        assertLiteral(DATE, new GenericLiteral(LOCATION, "DATE", "1970-01-01"), LocalDate.parse("1970-01-01").toEpochDay());
        assertLiteral(DATE, new GenericLiteral(LOCATION, "DATE", "9999-12-31"), LocalDate.parse("9999-12-31").toEpochDay());
    }

    @Test
    void testTimestamp()
    {
        assertLiteral(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP(0)", "1970-01-01 00:00:00"), 0L);
        assertLiteral(createTimestampType(1), new GenericLiteral(LOCATION, "TIMESTAMP(1)", "1970-01-01 00:00:00.9"), 900000L);
        assertLiteral(createTimestampType(2), new GenericLiteral(LOCATION, "TIMESTAMP(2)", "1970-01-01 00:00:00.99"), 990000L);
        assertLiteral(createTimestampType(3), new GenericLiteral(LOCATION, "TIMESTAMP(3)", "1970-01-01 00:00:00.999"), 999000L);
        assertLiteral(createTimestampType(4), new GenericLiteral(LOCATION, "TIMESTAMP(4)", "1970-01-01 00:00:00.9999"), 999900L);
        assertLiteral(createTimestampType(5), new GenericLiteral(LOCATION, "TIMESTAMP(5)", "1970-01-01 00:00:00.99999"), 999990L);
        assertLiteral(createTimestampType(6), new GenericLiteral(LOCATION, "TIMESTAMP(6)", "1970-01-01 00:00:00.999999"), 999999L);
        assertLiteral(createTimestampType(7), new GenericLiteral(LOCATION, "TIMESTAMP(7)", "1970-01-01 00:00:00.9999999"), new LongTimestamp(999999L, 900000));
        assertLiteral(createTimestampType(8), new GenericLiteral(LOCATION, "TIMESTAMP(8)", "1970-01-01 00:00:00.99999999"), new LongTimestamp(999999L, 990000));
        assertLiteral(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP(9)", "1970-01-01 00:00:00.999999999"), new LongTimestamp(999999L, 999000));
        assertLiteral(createTimestampType(10), new GenericLiteral(LOCATION, "TIMESTAMP(10)", "1970-01-01 00:00:00.9999999999"), new LongTimestamp(999999L, 999900));
        assertLiteral(createTimestampType(11), new GenericLiteral(LOCATION, "TIMESTAMP(11)", "1970-01-01 00:00:00.99999999999"), new LongTimestamp(999999L, 999990));
        assertLiteral(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP(12)", "1970-01-01 00:00:00.999999999999"), new LongTimestamp(999999L, 999999));

        // Round fractional seconds
        assertLiteral(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP(1)", "1970-01-01 00:00:00.4"), 0L);
        assertLiteral(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP(1)", "1970-01-01 00:00:00.5"), 1000000L);
        assertLiteral(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP(9)", "1970-01-01 00:00:00.9999999994"), new LongTimestamp(999999L, 999000));
        assertLiteral(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP(9)", "1970-01-01 00:00:00.9999999995"), new LongTimestamp(1000000L, 0));
    }

    @Test
    void testTimestampWithTimeZone()
    {
        assertLiteral(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP(0) WITH TIME ZONE", "1970-01-01 00:00:00 UTC"), packDateTimeWithZone(0, UTC_KEY));
        assertLiteral(createTimestampWithTimeZoneType(1), new GenericLiteral(LOCATION, "TIMESTAMP(1) WITH TIME ZONE", "1970-01-01 00:00:00.9 UTC"), packDateTimeWithZone(900, UTC_KEY));
        assertLiteral(createTimestampWithTimeZoneType(2), new GenericLiteral(LOCATION, "TIMESTAMP(2) WITH TIME ZONE", "1970-01-01 00:00:00.99 UTC"), packDateTimeWithZone(990, UTC_KEY));
        assertLiteral(createTimestampWithTimeZoneType(3), new GenericLiteral(LOCATION, "TIMESTAMP(3) WITH TIME ZONE", "1970-01-01 00:00:00.999 UTC"), packDateTimeWithZone(999, UTC_KEY));
        assertLiteral(createTimestampWithTimeZoneType(4), new GenericLiteral(LOCATION, "TIMESTAMP(4) WITH TIME ZONE", "1970-01-01 00:00:00.9999 UTC"), longTimestampWithTimeZone(0, 999900000000L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(5), new GenericLiteral(LOCATION, "TIMESTAMP(5) WITH TIME ZONE", "1970-01-01 00:00:00.99999 UTC"), longTimestampWithTimeZone(0, 999990000000L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(6), new GenericLiteral(LOCATION, "TIMESTAMP(6) WITH TIME ZONE", "1970-01-01 00:00:00.999999 UTC"), longTimestampWithTimeZone(0, 999999000000L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(7), new GenericLiteral(LOCATION, "TIMESTAMP(7) WITH TIME ZONE", "1970-01-01 00:00:00.9999999 UTC"), longTimestampWithTimeZone(0, 999999900000L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(8), new GenericLiteral(LOCATION, "TIMESTAMP(8) WITH TIME ZONE", "1970-01-01 00:00:00.99999999 UTC"), longTimestampWithTimeZone(0, 999999990000L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP(9) WITH TIME ZONE", "1970-01-01 00:00:00.999999999 UTC"), longTimestampWithTimeZone(0, 999999999000L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(10), new GenericLiteral(LOCATION, "TIMESTAMP(10) WITH TIME ZONE", "1970-01-01 00:00:00.9999999999 UTC"), longTimestampWithTimeZone(0, 999999999900L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(11), new GenericLiteral(LOCATION, "TIMESTAMP(11) WITH TIME ZONE", "1970-01-01 00:00:00.99999999999 UTC"), longTimestampWithTimeZone(0, 999999999990L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(12), new GenericLiteral(LOCATION, "TIMESTAMP(12) WITH TIME ZONE", "1970-01-01 00:00:00.999999999999 UTC"), longTimestampWithTimeZone(0, 999999999999L, UTC));

        // Round fractional seconds
        assertLiteral(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP(1) WITH TIME ZONE", "1970-01-01 00:00:00.4 UTC"), packDateTimeWithZone(0, UTC_KEY));
        assertLiteral(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP(1) WITH TIME ZONE", "1970-01-01 00:00:00.5 UTC"), packDateTimeWithZone(1000, UTC_KEY));
        assertLiteral(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP(10) WITH TIME ZONE", "1970-01-01 00:00:00.9999999994 UTC"), longTimestampWithTimeZone(0, 999999999000L, UTC));
        assertLiteral(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP(10) WITH TIME ZONE", "1970-01-01 00:00:00.9999999995 UTC"), longTimestampWithTimeZone(1, 0L, UTC));
    }

    private void assertLiteral(Type columnType, Literal literal, Object expected)
    {
        assertThat(evaluate(literal, columnType))
                .isEqualTo(new Constant(expected, columnType));
    }

    private void assertInvalidLiteralFails(Type type, Literal literal, String stackTrace)
    {
        assertTrinoExceptionThrownBy(() -> evaluate(literal, type))
                .hasErrorCode(INVALID_LITERAL)
                .hasMessageMatching("line 1:1: .* is not a valid .* literal")
                .hasStackTraceContaining(stackTrace);
    }

    private Constant evaluate(Literal literal, Type type)
    {
        return (Constant) evaluateDefaultValue(TEST_SESSION, plannerContext, new AllowAllAccessControl(), Map.of(), WarningCollector.NOOP, type, literal);
    }
}
