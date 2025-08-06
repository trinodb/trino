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
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.INVALID_DEFAULT_COLUMN_VALUE;
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
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeDefaultColumnValue;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

final class TestColumnDefaultOptions
{
    private static final NodeLocation LOCATION = new NodeLocation(1, 1);

    private final PlannerContext plannerContext;

    public TestColumnDefaultOptions()
    {
        plannerContext = plannerContextBuilder().build();
    }

    @Test
    void testBoolean()
    {
        assertDefaultColumnValue(BOOLEAN, new BooleanLiteral(LOCATION, "true"));
        assertDefaultColumnValue(BOOLEAN, new BooleanLiteral(LOCATION, "false"));

        assertInvalidDefaultColumnValue(BOOLEAN, new LongLiteral(LOCATION, "1"), "Value must be true or false '1'");
        assertInvalidDefaultColumnValue(BOOLEAN, new LongLiteral(LOCATION, "0"), "Value must be true or false '0'");
        assertInvalidDefaultColumnValue(BOOLEAN, new NullLiteral(LOCATION), "Value must be true or false 'null'");
    }

    @Test
    void testTinyint()
    {
        assertDefaultColumnValue(TINYINT, new LongLiteral(LOCATION, "-128"));
        assertDefaultColumnValue(TINYINT, new LongLiteral(LOCATION, "127"));
        assertDefaultColumnValue(TINYINT, new NullLiteral(LOCATION));

        assertInvalidDefaultColumnValue(TINYINT, new LongLiteral(LOCATION, "-129"), "Out of range for tinyint: -129");
        assertInvalidDefaultColumnValue(TINYINT, new LongLiteral(LOCATION, "128"), "Out of range for tinyint: 128");
    }

    @Test
    void testSmallint()
    {
        assertDefaultColumnValue(SMALLINT, new LongLiteral(LOCATION, "-32768"));
        assertDefaultColumnValue(SMALLINT, new LongLiteral(LOCATION, "32767"));
        assertDefaultColumnValue(SMALLINT, new NullLiteral(LOCATION));

        assertInvalidDefaultColumnValue(SMALLINT, new LongLiteral(LOCATION, "-32769"), " Out of range for smallint: -32769");
        assertInvalidDefaultColumnValue(SMALLINT, new LongLiteral(LOCATION, "32768"), " Out of range for smallint: 32768");
    }

    @Test
    void testInteger()
    {
        assertDefaultColumnValue(INTEGER, new LongLiteral(LOCATION, "-2147483648"));
        assertDefaultColumnValue(INTEGER, new LongLiteral(LOCATION, "2147483647"));
        assertDefaultColumnValue(INTEGER, new NullLiteral(LOCATION));

        assertInvalidDefaultColumnValue(INTEGER, new LongLiteral(LOCATION, "-2147483649"), "Out of range for integer: -2147483649");
        assertInvalidDefaultColumnValue(INTEGER, new LongLiteral(LOCATION, "2147483648"), "Out of range for integer: 2147483648");
    }

    @Test
    void testBigint()
    {
        assertDefaultColumnValue(BIGINT, new LongLiteral(LOCATION, "-9223372036854775808"));
        assertDefaultColumnValue(BIGINT, new LongLiteral(LOCATION, "9223372036854775807"));
        assertDefaultColumnValue(BIGINT, new NullLiteral(LOCATION));

        // LongLiteral disallows values outside the range of a long
    }

    @Test
    void testReal()
    {
        assertDefaultColumnValue(REAL, new GenericLiteral(LOCATION, "REAL", "3.14"));
        assertDefaultColumnValue(REAL, new GenericLiteral(LOCATION, "REAL", "10.3e0"));
        assertDefaultColumnValue(REAL, new LongLiteral(LOCATION, "123"));

        assertDefaultColumnValue(REAL, new GenericLiteral(LOCATION, "REAL", "NaN"));
        assertDefaultColumnValue(REAL, new GenericLiteral(LOCATION, "REAL", "+Infinity"));
        assertDefaultColumnValue(REAL, new GenericLiteral(LOCATION, "REAL", "-Infinity"));
        assertDefaultColumnValue(REAL, new NullLiteral(LOCATION));
    }

    @Test
    void testDouble()
    {
        assertDefaultColumnValue(DOUBLE, new DoubleLiteral(LOCATION, "3.14"));
        assertDefaultColumnValue(DOUBLE, new DoubleLiteral(LOCATION, "1.0E100"));
        assertDefaultColumnValue(DOUBLE, new DoubleLiteral(LOCATION, "1.23456E12"));
        assertDefaultColumnValue(DOUBLE, new LongLiteral(LOCATION, "123"));

        assertDefaultColumnValue(DOUBLE, new GenericLiteral(LOCATION, "DOUBLE", "NaN"));
        assertDefaultColumnValue(DOUBLE, new GenericLiteral(LOCATION, "DOUBLE", "+Infinity"));
        assertDefaultColumnValue(DOUBLE, new GenericLiteral(LOCATION, "DOUBLE", "-Infinity"));
        assertDefaultColumnValue(DOUBLE, new NullLiteral(LOCATION));
    }

    @Test
    void testDecimal()
    {
        assertDefaultColumnValue(createDecimalType(3), new DecimalLiteral(LOCATION, "193"));
        assertDefaultColumnValue(createDecimalType(3), new DecimalLiteral(LOCATION, "-193"));
        assertDefaultColumnValue(createDecimalType(3, 1), new DecimalLiteral(LOCATION, "10.0"));
        assertDefaultColumnValue(createDecimalType(3, 1), new DecimalLiteral(LOCATION, "-10.1"));
        assertDefaultColumnValue(createDecimalType(30, 5), new DecimalLiteral(LOCATION, "3141592653589793238462643.38327"));
        assertDefaultColumnValue(createDecimalType(30, 5), new DecimalLiteral(LOCATION, "-3141592653589793238462643.38327"));
        assertDefaultColumnValue(createDecimalType(38), new DecimalLiteral(LOCATION, "27182818284590452353602874713526624977"));
        assertDefaultColumnValue(createDecimalType(38), new DecimalLiteral(LOCATION, "-27182818284590452353602874713526624977"));
        assertDefaultColumnValue(createDecimalType(3), new NullLiteral(LOCATION));
        assertDefaultColumnValue(createDecimalType(38), new NullLiteral(LOCATION));
    }

    @Test
    void testInvalidDecimal()
    {
        assertInvalidDefaultColumnValue(
                createDecimalType(3, 2),
                new DecimalLiteral(new NodeLocation(1, 1), "12.456"),
                "Cannot cast DECIMAL(5, 3) '12.456' to DECIMAL(3, 2)");

        assertInvalidDefaultColumnValue(
                createDecimalType(19, 0),
                new DecimalLiteral(new NodeLocation(1, 1), "12345678901234567890"),
                "Cannot cast DECIMAL(20, 0) '12345678901234567890' to DECIMAL(19, 0)");
    }

    @Test
    void testChar()
    {
        assertDefaultColumnValue(createCharType(4), new StringLiteral(LOCATION, "test"));
        assertDefaultColumnValue(createCharType(10), new StringLiteral(LOCATION, "test"));
        assertDefaultColumnValue(createCharType(5), new StringLiteral(LOCATION, "攻殻機動隊"));
        assertDefaultColumnValue(createCharType(1), new StringLiteral(LOCATION, "😂"));
        assertDefaultColumnValue(createCharType(3), new NullLiteral(LOCATION));
    }

    @Test
    void testInvalidChar()
    {
        assertInvalidDefaultColumnValue(
                createCharType(4),
                new StringLiteral(new NodeLocation(1, 1), "abcd "),
                "''abcd '' is not a valid CHAR(4) literal");
        assertInvalidDefaultColumnValue(
                createCharType(4),
                new StringLiteral(new NodeLocation(1, 1), " abcd"),
                "'' abcd'' is not a valid CHAR(4) literal");
    }

    @Test
    void testVarchar()
    {
        assertDefaultColumnValue(createVarcharType(4), new StringLiteral(LOCATION, "test"));
        assertDefaultColumnValue(createVarcharType(10), new StringLiteral(LOCATION, "test"));
        assertDefaultColumnValue(VARCHAR, new StringLiteral(LOCATION, "test"));
        assertDefaultColumnValue(createVarcharType(5), new StringLiteral(LOCATION, "攻殻機動隊"));
        assertDefaultColumnValue(createVarcharType(1), new StringLiteral(LOCATION, "😂"));
        assertDefaultColumnValue(VARCHAR, new NullLiteral(LOCATION));
        assertDefaultColumnValue(createVarcharType(255), new NullLiteral(LOCATION));
    }

    @Test
    void testInvalidVarchar()
    {
        assertInvalidDefaultColumnValue(
                createVarcharType(4),
                new StringLiteral(new NodeLocation(1, 1), "abcd "),
                "''abcd '' is not a valid VARCHAR(4) literal");
        assertInvalidDefaultColumnValue(
                createVarcharType(4),
                new StringLiteral(new NodeLocation(1, 1), " abcd"),
                "'' abcd'' is not a valid VARCHAR(4) literal");
    }

    @Test
    void testTime()
    {
        assertDefaultColumnValue(createTimeType(0), new GenericLiteral(LOCATION, "TIME", "00:00:01"));

        assertDefaultColumnValue(createTimeType(0), new GenericLiteral(LOCATION, "TIME", "00:00:00"));
        assertDefaultColumnValue(createTimeType(1), new GenericLiteral(LOCATION, "TIME", "00:00:00.1"));
        assertDefaultColumnValue(createTimeType(2), new GenericLiteral(LOCATION, "TIME", "00:00:00.12"));
        assertDefaultColumnValue(createTimeType(3), new GenericLiteral(LOCATION, "TIME", "00:00:00.123"));
        assertDefaultColumnValue(createTimeType(4), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234"));
        assertDefaultColumnValue(createTimeType(5), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345"));
        assertDefaultColumnValue(createTimeType(6), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456"));
        assertDefaultColumnValue(createTimeType(7), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234567"));
        assertDefaultColumnValue(createTimeType(8), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345678"));
        assertDefaultColumnValue(createTimeType(9), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456789"));
        assertDefaultColumnValue(createTimeType(10), new GenericLiteral(LOCATION, "TIME", "00:00:00.1234567890"));
        assertDefaultColumnValue(createTimeType(11), new GenericLiteral(LOCATION, "TIME", "00:00:00.12345678901"));
        assertDefaultColumnValue(createTimeType(12), new GenericLiteral(LOCATION, "TIME", "00:00:00.123456789012"));

        assertDefaultColumnValue(createTimeType(0), new NullLiteral(LOCATION));
        assertDefaultColumnValue(createTimeType(12), new NullLiteral(LOCATION));
    }

    @Test
    void testDate()
    {
        assertDefaultColumnValue(DATE, new GenericLiteral(LOCATION, "DATE", "0001-01-01"));
        assertDefaultColumnValue(DATE, new GenericLiteral(LOCATION, "DATE", "1969-12-31"));
        assertDefaultColumnValue(DATE, new GenericLiteral(LOCATION, "DATE", "1970-01-01"));
        assertDefaultColumnValue(DATE, new GenericLiteral(LOCATION, "DATE", "9999-12-31"));
        assertDefaultColumnValue(DATE, new NullLiteral(LOCATION));
    }

    @Test
    void testTimestamp()
    {
        assertDefaultColumnValue(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00"));
        assertDefaultColumnValue(createTimestampType(1), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9"));
        assertDefaultColumnValue(createTimestampType(2), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99"));
        assertDefaultColumnValue(createTimestampType(3), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999"));
        assertDefaultColumnValue(createTimestampType(4), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999"));
        assertDefaultColumnValue(createTimestampType(5), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99999"));
        assertDefaultColumnValue(createTimestampType(6), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999999"));
        assertDefaultColumnValue(createTimestampType(7), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999999"));
        assertDefaultColumnValue(createTimestampType(8), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99999999"));
        assertDefaultColumnValue(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999999999"));
        assertDefaultColumnValue(createTimestampType(10), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999999999"));
        assertDefaultColumnValue(createTimestampType(11), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99999999999"));
        assertDefaultColumnValue(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999999999999"));

        // short timestamp literal on long timestamp column
        assertDefaultColumnValue(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00"));
        assertDefaultColumnValue(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9"));
        assertDefaultColumnValue(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99"));
        assertDefaultColumnValue(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999"));
        assertDefaultColumnValue(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999"));
        assertDefaultColumnValue(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99999"));
        assertDefaultColumnValue(createTimestampType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999999"));

        assertDefaultColumnValue(createTimestampType(0), new NullLiteral(LOCATION));
        assertDefaultColumnValue(createTimestampType(12), new NullLiteral(LOCATION));
    }

    @Test
    void testInvalidTimestamp()
    {
        assertInvalidDefaultColumnValue(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.4"), "Value too large");
        assertInvalidDefaultColumnValue(createTimestampType(0), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.5"), "Value too large");
        assertInvalidDefaultColumnValue(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999999994"), "Value too large");
        assertInvalidDefaultColumnValue(createTimestampType(9), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999999995"), "Value too large");
    }

    @Test
    void testTimestampWithTimeZone()
    {
        assertDefaultColumnValue(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(1), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(2), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(3), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(4), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(5), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(6), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(7), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(8), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99999999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999999999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(10), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999999999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(11), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99999999999 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999999999999 UTC"));

        // short timestamptz literal on long timestamptz column
        assertDefaultColumnValue(createTimestampWithTimeZoneType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.99 UTC"));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(12), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.999 UTC"));

        assertDefaultColumnValue(createTimestampWithTimeZoneType(0), new NullLiteral(LOCATION));
        assertDefaultColumnValue(createTimestampWithTimeZoneType(12), new NullLiteral(LOCATION));
    }

    @Test
    void testInvalidTimestampWithTimeZone()
    {
        assertInvalidDefaultColumnValue(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.4 UTC"), "Value too large");
        assertInvalidDefaultColumnValue(createTimestampWithTimeZoneType(0), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.5 UTC"), "Value too large");
        assertInvalidDefaultColumnValue(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999999994 UTC"), "Value too large");
        assertInvalidDefaultColumnValue(createTimestampWithTimeZoneType(9), new GenericLiteral(LOCATION, "TIMESTAMP", "1970-01-01 00:00:00.9999999995 UTC"), "Value too large");
    }

    private void assertDefaultColumnValue(Type columnType, Expression expression)
    {
        analyzeDefaultColumnValue(TEST_SESSION, plannerContext, new AllowAllAccessControl(), Map.of(), WarningCollector.NOOP, columnType, expression);
    }

    private void assertInvalidDefaultColumnValue(Type type, Expression expression, String stackTrace)
    {
        assertTrinoExceptionThrownBy(() -> analyzeDefaultColumnValue(TEST_SESSION, plannerContext, new AllowAllAccessControl(), Map.of(), WarningCollector.NOOP, type, expression))
                .hasErrorCode(INVALID_DEFAULT_COLUMN_VALUE)
                .hasMessageMatching("line 1:1: .* is not a valid .* literal")
                .hasStackTraceContaining(stackTrace);
    }
}
