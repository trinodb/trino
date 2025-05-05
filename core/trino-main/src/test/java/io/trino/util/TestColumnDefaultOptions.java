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

import io.trino.spi.expression.Constant;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.LiteralInterpreter;
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
import static io.trino.util.ColumnDefaultOptions.evaluateLiteral;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

final class TestColumnDefaultOptions
{
    private static final NodeLocation LOCATION = new NodeLocation(1, 1);

    private final LiteralInterpreter literalInterpreter;

    public TestColumnDefaultOptions()
    {
        PlannerContext plannerContext = plannerContextBuilder().build();
        literalInterpreter = new LiteralInterpreter(plannerContext, TEST_SESSION);
    }

    @Test
    void testNull()
    {
        assertLiteral(new NullLiteral(LOCATION), BOOLEAN, null);
    }

    @Test
    void testBoolean()
    {
        assertLiteral(new BooleanLiteral(LOCATION, "true"), BOOLEAN, true);
        assertLiteral(new BooleanLiteral(LOCATION, "false"), BOOLEAN, false);
    }

    @Test
    void testTinyint()
    {
        assertLiteral(new LongLiteral(LOCATION, "-128"), TINYINT, -128L);
        assertLiteral(new LongLiteral(LOCATION, "127"), TINYINT, 127L);
    }

    @Test
    void testSmallint()
    {
        assertLiteral(new LongLiteral(LOCATION, "-32768"), SMALLINT, -32768L);
        assertLiteral(new LongLiteral(LOCATION, "32767"), SMALLINT, 32767L);
    }

    @Test
    void testInteger()
    {
        assertLiteral(new LongLiteral(LOCATION, "-2147483648"), INTEGER, -2147483648L);
        assertLiteral(new LongLiteral(LOCATION, "2147483647"), INTEGER, 2147483647L);
    }

    @Test
    void testBigint()
    {
        assertLiteral(new LongLiteral(LOCATION, "-9223372036854775808"), BIGINT, -9223372036854775808L);
        assertLiteral(new LongLiteral(LOCATION, "9223372036854775807"), BIGINT, 9223372036854775807L);
    }

    @Test
    void testReal()
    {
        assertLiteral(new GenericLiteral(LOCATION, "REAL", "3.14"), REAL, (long) floatToIntBits(3.14f));
        assertLiteral(new GenericLiteral(LOCATION, "REAL", "10.3e0"), REAL, (long) floatToIntBits(10.3e0f));

        assertThat(intBitsToFloat(toIntExact((Long) evaluateLiteral(literalInterpreter, new GenericLiteral(LOCATION, "REAL", "NaN"), REAL).getValue())))
                .isNaN();
        assertThat(intBitsToFloat(toIntExact((Long) evaluateLiteral(literalInterpreter, new GenericLiteral(LOCATION, "REAL", "+Infinity"), REAL).getValue())))
                .isInfinite();
        assertThat(intBitsToFloat(toIntExact((Long) evaluateLiteral(literalInterpreter, new GenericLiteral(LOCATION, "REAL", "-Infinity"), REAL).getValue())))
                .isInfinite();
    }

    @Test
    void testDouble()
    {
        assertLiteral(new DoubleLiteral(LOCATION, "3.14"), DOUBLE, 3.14);
        assertLiteral(new DoubleLiteral(LOCATION, "1.0E100"), DOUBLE, 1.0E100);
        assertLiteral(new DoubleLiteral(LOCATION, "1.23456E12"), DOUBLE, 1.23456E12);

        assertThat((Double) evaluateLiteral(literalInterpreter, new DoubleLiteral(LOCATION, "NaN"), DOUBLE).getValue())
                .isNaN();
        assertThat((Double) evaluateLiteral(literalInterpreter, new DoubleLiteral(LOCATION, "+Infinity"), DOUBLE).getValue())
                .isInfinite();
        assertThat((Double) evaluateLiteral(literalInterpreter, new DoubleLiteral(LOCATION, "-Infinity"), DOUBLE).getValue())
                .isInfinite();
    }

    @Test
    void testDecimal()
    {
        assertLiteral(new DecimalLiteral(LOCATION, "193"), createDecimalType(3), 193L);
        assertLiteral(new DecimalLiteral(LOCATION, "-193"), createDecimalType(3), -193L);
        assertLiteral(new DecimalLiteral(LOCATION, "10.0"), createDecimalType(3, 1), 100L);
        assertLiteral(new DecimalLiteral(LOCATION, "-10.1"), createDecimalType(3, 1), -101L);
        assertLiteral(new DecimalLiteral(LOCATION, "3141592653589793238462643.38327"), createDecimalType(30, 5), Int128.valueOf("314159265358979323846264338327"));
        assertLiteral(new DecimalLiteral(LOCATION, "-3141592653589793238462643.38327"), createDecimalType(30, 5), Int128.valueOf("-314159265358979323846264338327"));
        assertLiteral(new DecimalLiteral(LOCATION, "27182818284590452353602874713526624977"), createDecimalType(38), Int128.valueOf("27182818284590452353602874713526624977"));
        assertLiteral(new DecimalLiteral(LOCATION, "-27182818284590452353602874713526624977"), createDecimalType(38), Int128.valueOf("-27182818284590452353602874713526624977"));
    }

    @Test
    void testInvalidDecimal()
    {
        assertInvalidLiteralFails(
                createDecimalType(3, 2),
                new DecimalLiteral(new NodeLocation(1, 1), "12.456"),
                "Value too large");

        assertInvalidLiteralFails(
                createDecimalType(19, 0),
                new DecimalLiteral(new NodeLocation(1, 1), "12345678901234567890"),
                "Value too large");
    }

    @Test
    void testChar()
    {
        assertLiteral(new StringLiteral(LOCATION, "test"), createCharType(4), utf8Slice("test"));
        assertLiteral(new StringLiteral(LOCATION, "test"), createCharType(10), utf8Slice("test"));
        assertLiteral(new StringLiteral(LOCATION, "攻殻機動隊"), createCharType(5), utf8Slice("攻殻機動隊"));
        assertLiteral(new StringLiteral(LOCATION, "😂"), createCharType(1), utf8Slice("😂"));
    }

    @Test
    void testInvalidChar()
    {
        assertInvalidLiteralFails(
                createCharType(4),
                new StringLiteral(new NodeLocation(1, 1), " abcd"),
                "Cannot truncate characters when casting value ' abcd' to char(4)");
        assertInvalidLiteralFails(
                createCharType(4),
                new StringLiteral(new NodeLocation(1, 1), "abcd "),
                "Cannot truncate characters when casting value 'abcd ' to char(4)");
    }

    @Test
    void testVarchar()
    {
        assertLiteral(new StringLiteral(LOCATION, "test"), createVarcharType(4), utf8Slice("test"));
        assertLiteral(new StringLiteral(LOCATION, "test"), createVarcharType(10), utf8Slice("test"));
        assertLiteral(new StringLiteral(LOCATION, "test"), VARCHAR, utf8Slice("test"));
        assertLiteral(new StringLiteral(LOCATION, "攻殻機動隊"), createVarcharType(5), utf8Slice("攻殻機動隊"));
        assertLiteral(new StringLiteral(LOCATION, "😂"), createVarcharType(1), utf8Slice("😂"));
    }

    @Test
    void testInvalidVarchar()
    {
        assertInvalidLiteralFails(
                createVarcharType(4),
                new StringLiteral(new NodeLocation(1, 1), " abcd"),
                "Cannot truncate characters when casting value ' abcd' to varchar(4)");
        assertInvalidLiteralFails(
                createVarcharType(4),
                new StringLiteral(new NodeLocation(1, 1), "abcd "),
                "Cannot truncate characters when casting value 'abcd ' to varchar(4)");
    }

    @Test
    void testTime()
    {
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:01"), createTimeType(0), 1000000000000L);

        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00"), createTimeType(0), 0L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.1"), createTimeType(1), 100000000000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.12"), createTimeType(2), 120000000000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.123"), createTimeType(3), 123000000000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.1234"), createTimeType(4), 123400000000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.12345"), createTimeType(5), 123450000000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.123456"), createTimeType(6), 123456000000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.1234567"), createTimeType(7), 123456700000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.12345678"), createTimeType(8), 123456780000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.123456789"), createTimeType(9), 123456789000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.1234567890"), createTimeType(10), 123456789000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.12345678901"), createTimeType(11), 123456789010L);
        assertLiteral(new GenericLiteral(LOCATION, "TIME", "00:00:00.123456789012"), createTimeType(12), 123456789012L);
    }

    @Test
    void testDate()
    {
        assertLiteral(new GenericLiteral(LOCATION, "DATE", "0001-01-01"), DATE, LocalDate.parse("0001-01-01").toEpochDay());
        assertLiteral(new GenericLiteral(LOCATION, "DATE", "1969-12-31"), DATE, LocalDate.parse("1969-12-31").toEpochDay());
        assertLiteral(new GenericLiteral(LOCATION, "DATE", "1970-01-01"), DATE, LocalDate.parse("1970-01-01").toEpochDay());
        assertLiteral(new GenericLiteral(LOCATION, "DATE", "9999-12-31"), DATE, LocalDate.parse("9999-12-31").toEpochDay());
    }

    @Test
    void testTimestamp()
    {
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(0)", "1970-01-01 00:00:00"), createTimestampType(0), 0L);
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(1)", "1970-01-01 00:00:00.9"), createTimestampType(1), 900000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(2)", "1970-01-01 00:00:00.99"), createTimestampType(2), 990000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(3)", "1970-01-01 00:00:00.999"), createTimestampType(3), 999000L);
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(4)", "1970-01-01 00:00:00.9999"), createTimestampType(4), 999900L);
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(5)", "1970-01-01 00:00:00.99999"), createTimestampType(5), 999990L);
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(6)", "1970-01-01 00:00:00.999999"), createTimestampType(6), 999999L);
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(7)", "1970-01-01 00:00:00.9999999"), createTimestampType(7), new LongTimestamp(999999L, 900000));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(8)", "1970-01-01 00:00:00.99999999"), createTimestampType(8), new LongTimestamp(999999L, 990000));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(9)", "1970-01-01 00:00:00.999999999"), createTimestampType(9), new LongTimestamp(999999L, 999000));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(10)", "1970-01-01 00:00:00.9999999999"), createTimestampType(10), new LongTimestamp(999999L, 999900));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(11)", "1970-01-01 00:00:00.99999999999"), createTimestampType(11), new LongTimestamp(999999L, 999990));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(12)", "1970-01-01 00:00:00.999999999999"), createTimestampType(12), new LongTimestamp(999999L, 999999));
    }

    @Test
    void testInvalidTimestamp()
    {
        assertInvalidLiteralFails(
                createTimestampType(0),
                new GenericLiteral(new NodeLocation(1, 1), "TIMESTAMP(0)", "1970-01-01 00:00:00.1"),
                "Value too large");

        assertInvalidLiteralFails(
                createTimestampType(9),
                new GenericLiteral(new NodeLocation(1, 1), "TIMESTAMP(0)", "1970-01-01 00:00:00.0123456789"),
                "Value too large");
    }

    @Test
    void testTimestampWithTimeZone()
    {
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(0) WITH TIME ZONE", "1970-01-01 00:00:00 UTC"), createTimestampWithTimeZoneType(0), packDateTimeWithZone(0, UTC_KEY));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(1) WITH TIME ZONE", "1970-01-01 00:00:00.9 UTC"), createTimestampWithTimeZoneType(1), packDateTimeWithZone(900, UTC_KEY));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(2) WITH TIME ZONE", "1970-01-01 00:00:00.99 UTC"), createTimestampWithTimeZoneType(2), packDateTimeWithZone(990, UTC_KEY));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(3) WITH TIME ZONE", "1970-01-01 00:00:00.999 UTC"), createTimestampWithTimeZoneType(3), packDateTimeWithZone(999, UTC_KEY));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(4) WITH TIME ZONE", "1970-01-01 00:00:00.9999 UTC"), createTimestampWithTimeZoneType(4), longTimestampWithTimeZone(0, 999900000000L, UTC));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(5) WITH TIME ZONE", "1970-01-01 00:00:00.99999 UTC"), createTimestampWithTimeZoneType(5), longTimestampWithTimeZone(0, 999990000000L, UTC));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(6) WITH TIME ZONE", "1970-01-01 00:00:00.999999 UTC"), createTimestampWithTimeZoneType(6), longTimestampWithTimeZone(0, 999999000000L, UTC));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(7) WITH TIME ZONE", "1970-01-01 00:00:00.9999999 UTC"), createTimestampWithTimeZoneType(7), longTimestampWithTimeZone(0, 999999900000L, UTC));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(8) WITH TIME ZONE", "1970-01-01 00:00:00.99999999 UTC"), createTimestampWithTimeZoneType(8), longTimestampWithTimeZone(0, 999999990000L, UTC));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(9) WITH TIME ZONE", "1970-01-01 00:00:00.999999999 UTC"), createTimestampWithTimeZoneType(9), longTimestampWithTimeZone(0, 999999999000L, UTC));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(10) WITH TIME ZONE", "1970-01-01 00:00:00.9999999999 UTC"), createTimestampWithTimeZoneType(10), longTimestampWithTimeZone(0, 999999999900L, UTC));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(11) WITH TIME ZONE", "1970-01-01 00:00:00.99999999999 UTC"), createTimestampWithTimeZoneType(11), longTimestampWithTimeZone(0, 999999999990L, UTC));
        assertLiteral(new GenericLiteral(LOCATION, "TIMESTAMP(12) WITH TIME ZONE", "1970-01-01 00:00:00.999999999999 UTC"), createTimestampWithTimeZoneType(12), longTimestampWithTimeZone(0, 999999999999L, UTC));
    }

    @Test
    void testInvalidTimestampWithTimeZone()
    {
        assertInvalidLiteralFails(
                createTimestampWithTimeZoneType(0),
                new GenericLiteral(new NodeLocation(1, 1), "TIMESTAMP(0) WITH TIME ZONE", "1970-01-01 00:00:00.1 UTC"),
                "Value too large");

        assertInvalidLiteralFails(
                createTimestampWithTimeZoneType(9),
                new GenericLiteral(new NodeLocation(1, 1), "TIMESTAMP(9) WITH TIME ZONE", "1970-01-01 00:00:00.0123456789 UTC"),
                "Value too large");
    }

    private void assertLiteral(Literal literal, Type type, Object expected)
    {
        assertThat(evaluateLiteral(literalInterpreter, literal, type))
                .isEqualTo(new Constant(expected, type));
    }

    private void assertInvalidLiteralFails(Type type, Literal literal, String expectedMessage)
    {
        assertTrinoExceptionThrownBy(() ->
                evaluateLiteral(literalInterpreter, literal, type))
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:1: '%s' is not a valid %s literal", literal, type.getDisplayName().toUpperCase(ENGLISH))
                .hasStackTraceContaining(expectedMessage);
    }
}
