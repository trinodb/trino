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

import java.util.Map;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.util.ColumnDefaultOptions.evaluateDefaultValue;
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
        assertLiteral(BOOLEAN, new BooleanLiteral(LOCATION, "true"), "true");
        assertLiteral(BOOLEAN, new BooleanLiteral(LOCATION, "false"), "false");
    }

    @Test
    void testTinyint()
    {
        assertLiteral(TINYINT, new LongLiteral(LOCATION, "-128"), "-128");
        assertLiteral(TINYINT, new LongLiteral(LOCATION, "127"), "127");

        assertInvalidLiteralFails(TINYINT, new LongLiteral(LOCATION, "-129"), "Out of range for tinyint: -129");
        assertInvalidLiteralFails(TINYINT, new LongLiteral(LOCATION, "128"), "Out of range for tinyint: 128");
    }

    @Test
    void testSmallint()
    {
        assertLiteral(SMALLINT, new LongLiteral(LOCATION, "-32768"), "-32768");
        assertLiteral(SMALLINT, new LongLiteral(LOCATION, "32767"), "32767");

        assertInvalidLiteralFails(SMALLINT, new LongLiteral(LOCATION, "-32769"), " Out of range for smallint: -32769");
        assertInvalidLiteralFails(SMALLINT, new LongLiteral(LOCATION, "32768"), " Out of range for smallint: 32768");
    }

    @Test
    void testInteger()
    {
        assertLiteral(INTEGER, new LongLiteral(LOCATION, "-2147483648"), "-2147483648");
        assertLiteral(INTEGER, new LongLiteral(LOCATION, "2147483647"), "2147483647");

        assertInvalidLiteralFails(INTEGER, new LongLiteral(LOCATION, "-2147483649"), "Out of range for integer: -2147483649");
        assertInvalidLiteralFails(INTEGER, new LongLiteral(LOCATION, "2147483648"), "Out of range for integer: 2147483648");
    }

    @Test
    void testBigint()
    {
        assertLiteral(BIGINT, new LongLiteral(LOCATION, "-9223372036854775808"), "-9223372036854775808");
        assertLiteral(BIGINT, new LongLiteral(LOCATION, "9223372036854775807"), "9223372036854775807");

        // LongLiteral disallows values outside the range of a long
    }

    @Test
    void testReal()
    {
        assertLiteral(REAL, new GenericLiteral(LOCATION, "REAL", "3.14"), "3.14E0");
        assertLiteral(REAL, new GenericLiteral(LOCATION, "REAL", "10.3e0"), "1.03E1");
        assertLiteral(REAL, new LongLiteral(LOCATION, "123"), "1.23E2");

        assertLiteral(REAL, new GenericLiteral(LOCATION, "REAL", "NaN"), "NaN");
        assertLiteral(REAL, new GenericLiteral(LOCATION, "REAL", "+Infinity"), "Infinity");
        assertLiteral(REAL, new GenericLiteral(LOCATION, "REAL", "-Infinity"), "-Infinity");
    }

    @Test
    void testDouble()
    {
        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "3.14"), "3.14E0");
        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "1.0E100"), "1.0E100");
        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "1.23456E12"), "1.23456E12");
        assertLiteral(DOUBLE, new LongLiteral(LOCATION, "123"), "1.23E2");

        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "NaN"), "NaN");
        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "+Infinity"), "Infinity");
        assertLiteral(DOUBLE, new DoubleLiteral(LOCATION, "-Infinity"), "-Infinity");
    }

    @Test
    void testDecimal()
    {
        assertLiteral(createDecimalType(3), new DecimalLiteral(LOCATION, "193"), "193");
        assertLiteral(createDecimalType(3), new DecimalLiteral(LOCATION, "-193"), "-193");
        assertLiteral(createDecimalType(3, 1), new DecimalLiteral(LOCATION, "10.0"), "10.0");
        assertLiteral(createDecimalType(3, 1), new DecimalLiteral(LOCATION, "-10.1"), "-10.1");
        assertLiteral(createDecimalType(30, 5), new DecimalLiteral(LOCATION, "3141592653589793238462643.38327"), "3141592653589793238462643.38327");
        assertLiteral(createDecimalType(30, 5), new DecimalLiteral(LOCATION, "-3141592653589793238462643.38327"), "-3141592653589793238462643.38327");
        assertLiteral(createDecimalType(38), new DecimalLiteral(LOCATION, "27182818284590452353602874713526624977"), "27182818284590452353602874713526624977");
        assertLiteral(createDecimalType(38), new DecimalLiteral(LOCATION, "-27182818284590452353602874713526624977"), "-27182818284590452353602874713526624977");
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
        assertLiteral(createCharType(4), new StringLiteral(LOCATION, "test"), "test");
        assertLiteral(createCharType(10), new StringLiteral(LOCATION, "test"), "test");
        assertLiteral(createCharType(5), new StringLiteral(LOCATION, "攻殻機動隊"), "攻殻機動隊");
        assertLiteral(createCharType(1), new StringLiteral(LOCATION, "😂"), "😂");

        // Trim trailing spaces
        assertLiteral(createCharType(4), new StringLiteral(LOCATION, "test "), "test");
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
        assertLiteral(createVarcharType(4), new StringLiteral(LOCATION, "test"), "test");
        assertLiteral(createVarcharType(10), new StringLiteral(LOCATION, "test"), "test");
        assertLiteral(VARCHAR, new StringLiteral(LOCATION, "test"), "test");
        assertLiteral(createVarcharType(5), new StringLiteral(LOCATION, "攻殻機動隊"), "攻殻機動隊");
        assertLiteral(createVarcharType(1), new StringLiteral(LOCATION, "😂"), "😂");

        // Trim trailing spaces
        assertLiteral(createVarcharType(4), new StringLiteral(LOCATION, "test "), "test");
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
        assertLiteral(createTimeType(0), new GenericLiteral(LOCATION, "TIME", "00:00:01"), "00:00:01");

        assertLiteral(createTimeType(0), new GenericLiteral(LOCATION, "TIME", "00:00:00"), "00:00:00");
        assertLiteral(createTimeType(1), new GenericLiteral(LOCATION, "TIME", "00:00:00.1"), "00:00:00.1");
        assertLiteral(createTimeType(2), new GenericLiteral(LOCATION, "TIME", "00:00:00.12"), "00:00:00.12");
        assertLiteral(createTimeType(3), new GenericLiteral(LOCATION, "TIME", "00:00:00.123"), "00:00:00.123");
        assertLiteral(createTimeType(4), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234"), "00:00:00.1234");
        assertLiteral(createTimeType(5), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345"), "00:00:00.12345");
        assertLiteral(createTimeType(6), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456"), "00:00:00.123456");
        assertLiteral(createTimeType(7), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234567"), "00:00:00.1234567");
        assertLiteral(createTimeType(8), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345678"), "00:00:00.12345678");
        assertLiteral(createTimeType(9), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456789"), "00:00:00.123456789");
        assertLiteral(createTimeType(10), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234567890"), "00:00:00.1234567890");
        assertLiteral(createTimeType(11), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345678901"), "00:00:00.12345678901");
        assertLiteral(createTimeType(12), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456789012"), "00:00:00.123456789012");
    }

    @Test
    void testDate()
    {
        assertLiteral(DATE, new GenericLiteral(LOCATION, "DATE", "0001-01-01"), "0001-01-01");
        assertLiteral(DATE, new GenericLiteral(LOCATION, "DATE", "1969-12-31"), "1969-12-31");
        assertLiteral(DATE, new GenericLiteral(LOCATION, "DATE", "1970-01-01"), "1970-01-01");
        assertLiteral(DATE, new GenericLiteral(LOCATION, "DATE", "9999-12-31"), "9999-12-31");
    }

    @Test
    void testTimestamp()
    {
        assertLiteral(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP(0)", "1970-01-01 00:00:00"), "1970-01-01 00:00:00");
        assertLiteral(createTimestampType(1), new GenericLiteral(LOCATION, "TIMESTAMP(1)", "1970-01-01 00:00:00.9"), "1970-01-01 00:00:00.9");
        assertLiteral(createTimestampType(2), new GenericLiteral(LOCATION, "TIMESTAMP(2)", "1970-01-01 00:00:00.99"), "1970-01-01 00:00:00.99");
        assertLiteral(createTimestampType(3), new GenericLiteral(LOCATION, "TIMESTAMP(3)", "1970-01-01 00:00:00.999"), "1970-01-01 00:00:00.999");
        assertLiteral(createTimestampType(4), new GenericLiteral(LOCATION, "TIMESTAMP(4)", "1970-01-01 00:00:00.9999"), "1970-01-01 00:00:00.9999");
        assertLiteral(createTimestampType(5), new GenericLiteral(LOCATION, "TIMESTAMP(5)", "1970-01-01 00:00:00.99999"), "1970-01-01 00:00:00.99999");
        assertLiteral(createTimestampType(6), new GenericLiteral(LOCATION, "TIMESTAMP(6)", "1970-01-01 00:00:00.999999"), "1970-01-01 00:00:00.999999");
        assertLiteral(createTimestampType(7), new GenericLiteral(LOCATION, "TIMESTAMP(7)", "1970-01-01 00:00:00.9999999"), "1970-01-01 00:00:00.9999999");
        assertLiteral(createTimestampType(8), new GenericLiteral(LOCATION, "TIMESTAMP(8)", "1970-01-01 00:00:00.99999999"), "1970-01-01 00:00:00.99999999");
        assertLiteral(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP(9)", "1970-01-01 00:00:00.999999999"), "1970-01-01 00:00:00.999999999");
        assertLiteral(createTimestampType(10), new GenericLiteral(LOCATION, "TIMESTAMP(10)", "1970-01-01 00:00:00.9999999999"), "1970-01-01 00:00:00.9999999999");
        assertLiteral(createTimestampType(11), new GenericLiteral(LOCATION, "TIMESTAMP(11)", "1970-01-01 00:00:00.99999999999"), "1970-01-01 00:00:00.99999999999");
        assertLiteral(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP(12)", "1970-01-01 00:00:00.999999999999"), "1970-01-01 00:00:00.999999999999");

        // Round fractional seconds
        assertLiteral(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP(1)", "1970-01-01 00:00:00.4"), "1970-01-01 00:00:00");
        assertLiteral(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP(1)", "1970-01-01 00:00:00.5"), "1970-01-01 00:00:01");
        assertLiteral(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP(9)", "1970-01-01 00:00:00.9999999994"), "1970-01-01 00:00:00.999999999");
        assertLiteral(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP(9)", "1970-01-01 00:00:00.9999999995"), "1970-01-01 00:00:01.000000000");
    }

    @Test
    void testTimestampWithTimeZone()
    {
        assertLiteral(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP(0) WITH TIME ZONE", "1970-01-01 00:00:00 UTC"), "1970-01-01 00:00:00 UTC");
        assertLiteral(createTimestampWithTimeZoneType(1), new GenericLiteral(LOCATION, "TIMESTAMP(1) WITH TIME ZONE", "1970-01-01 00:00:00.9 UTC"), "1970-01-01 00:00:00.9 UTC");
        assertLiteral(createTimestampWithTimeZoneType(2), new GenericLiteral(LOCATION, "TIMESTAMP(2) WITH TIME ZONE", "1970-01-01 00:00:00.99 UTC"), "1970-01-01 00:00:00.99 UTC");
        assertLiteral(createTimestampWithTimeZoneType(3), new GenericLiteral(LOCATION, "TIMESTAMP(3) WITH TIME ZONE", "1970-01-01 00:00:00.999 UTC"), "1970-01-01 00:00:00.999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(4), new GenericLiteral(LOCATION, "TIMESTAMP(4) WITH TIME ZONE", "1970-01-01 00:00:00.9999 UTC"), "1970-01-01 00:00:00.9999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(5), new GenericLiteral(LOCATION, "TIMESTAMP(5) WITH TIME ZONE", "1970-01-01 00:00:00.99999 UTC"), "1970-01-01 00:00:00.99999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(6), new GenericLiteral(LOCATION, "TIMESTAMP(6) WITH TIME ZONE", "1970-01-01 00:00:00.999999 UTC"), "1970-01-01 00:00:00.999999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(7), new GenericLiteral(LOCATION, "TIMESTAMP(7) WITH TIME ZONE", "1970-01-01 00:00:00.9999999 UTC"), "1970-01-01 00:00:00.9999999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(8), new GenericLiteral(LOCATION, "TIMESTAMP(8) WITH TIME ZONE", "1970-01-01 00:00:00.99999999 UTC"), "1970-01-01 00:00:00.99999999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP(9) WITH TIME ZONE", "1970-01-01 00:00:00.999999999 UTC"), "1970-01-01 00:00:00.999999999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(10), new GenericLiteral(LOCATION, "TIMESTAMP(10) WITH TIME ZONE", "1970-01-01 00:00:00.9999999999 UTC"), "1970-01-01 00:00:00.9999999999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(11), new GenericLiteral(LOCATION, "TIMESTAMP(11) WITH TIME ZONE", "1970-01-01 00:00:00.99999999999 UTC"), "1970-01-01 00:00:00.99999999999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(12), new GenericLiteral(LOCATION, "TIMESTAMP(12) WITH TIME ZONE", "1970-01-01 00:00:00.999999999999 UTC"), "1970-01-01 00:00:00.999999999999 UTC");

        // Round fractional seconds
        assertLiteral(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP(1) WITH TIME ZONE", "1970-01-01 00:00:00.4 UTC"), "1970-01-01 00:00:00 UTC");
        assertLiteral(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP(1) WITH TIME ZONE", "1970-01-01 00:00:00.5 UTC"), "1970-01-01 00:00:01 UTC");
        assertLiteral(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP(10) WITH TIME ZONE", "1970-01-01 00:00:00.9999999994 UTC"), "1970-01-01 00:00:00.999999999 UTC");
        assertLiteral(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP(10) WITH TIME ZONE", "1970-01-01 00:00:00.9999999995 UTC"), "1970-01-01 00:00:01.000000000 UTC");
    }

    private void assertLiteral(Type columnType, Literal literal, String expected)
    {
        assertThat(evaluate(literal, columnType))
                .isEqualTo(expected);
    }

    private void assertInvalidLiteralFails(Type type, Literal literal, String stackTrace)
    {
        assertTrinoExceptionThrownBy(() -> evaluate(literal, type))
                .hasErrorCode(INVALID_LITERAL)
                .hasMessageMatching("line 1:1: .* is not a valid .* literal")
                .hasStackTraceContaining(stackTrace);
    }

    private String evaluate(Literal literal, Type type)
    {
        return evaluateDefaultValue(TEST_SESSION, plannerContext, new AllowAllAccessControl(), Map.of(), WarningCollector.NOOP, type, literal.toString());
    }
}
