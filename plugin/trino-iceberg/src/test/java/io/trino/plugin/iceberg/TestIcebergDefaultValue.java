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
package io.trino.plugin.iceberg;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergDefaultValue
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "3")
                .build();
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testBooleanWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "BOOLEAN", "true", "true");
        testWriteDefaultValue(format, "BOOLEAN", "false", "false");
        // Boolean NULL is disallowed at the engine level
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testBooleanInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.BooleanType.get(), Literal.of(true), "true");
        testInitialDefaultValue(format, Types.BooleanType.get(), Literal.of(false), "false");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testIntegerWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "INTEGER", "-2147483648", "-2147483648");
        testWriteDefaultValue(format, "INTEGER", "2147483647", "2147483647");
        testWriteDefaultValue(format, "INTEGER", "NULL", "CAST(NULL AS INTEGER)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testIntegerInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.IntegerType.get(), Literal.of(-2147483648), "-2147483648");
        testInitialDefaultValue(format, Types.IntegerType.get(), Literal.of(2147483647), "2147483647");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testBigintWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "BIGINT", "-9223372036854775808", "BIGINT '-9223372036854775808'");
        testWriteDefaultValue(format, "BIGINT", "9223372036854775807", "BIGINT '9223372036854775807'");
        testWriteDefaultValue(format, "BIGINT", "NULL", "CAST(NULL AS BIGINT)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testBigintInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.LongType.get(), Literal.of(-9223372036854775808L), "BIGINT '-9223372036854775808'");
        testInitialDefaultValue(format, Types.LongType.get(), Literal.of(9223372036854775807L), "BIGINT '9223372036854775807'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testRealWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "REAL", "REAL '3.14'", "REAL '3.14'");
        testWriteDefaultValue(format, "REAL", "REAL '10.3e0'", "REAL '10.3e0'");
        testWriteDefaultValue(format, "REAL", "123", "REAL '123'");
        testWriteDefaultValue(format, "REAL", "NULL", "CAST(NULL AS REAL)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testRealInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.FloatType.get(), Literal.of(3.14), "REAL '3.14'");
        testInitialDefaultValue(format, Types.FloatType.get(), Literal.of(10.3e0), "REAL '10.3e0'");
        testInitialDefaultValue(format, Types.FloatType.get(), Literal.of(123), "REAL '123'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDoubleWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "DOUBLE", "DOUBLE '3.14'", "DOUBLE '3.14'");
        testWriteDefaultValue(format, "DOUBLE", "DOUBLE '1.0E100'", "DOUBLE '1.0E100'");
        testWriteDefaultValue(format, "DOUBLE", "DOUBLE '1.23456E12'", "DOUBLE '1.23456E12'");
        testWriteDefaultValue(format, "DOUBLE", "123", "DOUBLE '123'");
        testWriteDefaultValue(format, "DOUBLE", "NULL", "CAST(NULL AS DOUBLE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDoubleInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.DoubleType.get(), Literal.of(3.14), "DOUBLE '3.14'");
        testInitialDefaultValue(format, Types.DoubleType.get(), Literal.of(1.0E100), "DOUBLE '1.0E100'");
        testInitialDefaultValue(format, Types.DoubleType.get(), Literal.of(1.23456E12), "DOUBLE '1.23456E12'");
        testInitialDefaultValue(format, Types.DoubleType.get(), Literal.of(123), "DOUBLE '123'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDecimalWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "DECIMAL(3,0)", "DECIMAL '193'", "DECIMAL '193'");
        testWriteDefaultValue(format, "DECIMAL(3,0)", "DECIMAL '-193'", "DECIMAL '-193'");
        testWriteDefaultValue(format, "DECIMAL(3,1)", "DECIMAL '10.0'", "DECIMAL '10.0'");
        testWriteDefaultValue(format, "DECIMAL(3,1)", "DECIMAL '-10.1'", "DECIMAL '-10.1'");
        testWriteDefaultValue(format, "DECIMAL(30,5)", "DECIMAL '3141592653589793238462643.38327'", "DECIMAL '3141592653589793238462643.38327'");
        testWriteDefaultValue(format, "DECIMAL(30,5)", "DECIMAL '-3141592653589793238462643.38327'", "DECIMAL '-3141592653589793238462643.38327'");
        testWriteDefaultValue(format, "DECIMAL(38,0)", "DECIMAL '27182818284590452353602874713526624977'", "DECIMAL '27182818284590452353602874713526624977'");
        testWriteDefaultValue(format, "DECIMAL(38,0)", "DECIMAL '-27182818284590452353602874713526624977'", "DECIMAL '-27182818284590452353602874713526624977'");
        testWriteDefaultValue(format, "DECIMAL(3,0)", "NULL", "CAST(NULL AS DECIMAL(3,0))");
        testWriteDefaultValue(format, "DECIMAL(38,0)", "NULL", "CAST(NULL AS DECIMAL(38,0))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDecimalInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.DecimalType.of(3, 0), Literal.of(new BigDecimal("193")), "DECIMAL '193'");
        testInitialDefaultValue(format, Types.DecimalType.of(3, 0), Literal.of(new BigDecimal("-193")), "DECIMAL '-193'");
        testInitialDefaultValue(format, Types.DecimalType.of(3, 1), Literal.of(new BigDecimal("10.0")), "DECIMAL '10.0'");
        testInitialDefaultValue(format, Types.DecimalType.of(3, 1), Literal.of(new BigDecimal("-10.1")), "DECIMAL '-10.1'");
        testInitialDefaultValue(format, Types.DecimalType.of(30, 5), Literal.of(new BigDecimal("3141592653589793238462643.38327")), "DECIMAL '3141592653589793238462643.38327'");
        testInitialDefaultValue(format, Types.DecimalType.of(30, 5), Literal.of(new BigDecimal("-3141592653589793238462643.38327")), "DECIMAL '-3141592653589793238462643.38327'");
        testInitialDefaultValue(format, Types.DecimalType.of(38, 0), Literal.of(new BigDecimal("27182818284590452353602874713526624977")), "DECIMAL '27182818284590452353602874713526624977'");
        testInitialDefaultValue(format, Types.DecimalType.of(38, 0), Literal.of(new BigDecimal("-27182818284590452353602874713526624977")), "DECIMAL '-27182818284590452353602874713526624977'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDateWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "DATE", "DATE '0001-01-01'", "DATE '0001-01-01'");
        testWriteDefaultValue(format, "DATE", "DATE '1969-12-31'", "DATE '1969-12-31'");
        testWriteDefaultValue(format, "DATE", "DATE '1970-01-01'", "DATE '1970-01-01'");
        testWriteDefaultValue(format, "DATE", "DATE '9999-12-31'", "DATE '9999-12-31'");
        testWriteDefaultValue(format, "DATE", "NULL", "CAST(NULL AS DATE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testDateInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.DateType.get(), Literal.of("1970-01-01").to(Types.DateType.get()), "DATE '1970-01-01'");
        testInitialDefaultValue(format, Types.DateType.get(), Literal.of("1969-12-31").to(Types.DateType.get()), "DATE '1969-12-31'");
        testInitialDefaultValue(format, Types.DateType.get(), Literal.of("1970-01-01").to(Types.DateType.get()), "DATE '1970-01-01'");
        testInitialDefaultValue(format, Types.DateType.get(), Literal.of("9999-12-31").to(Types.DateType.get()), "DATE '9999-12-31'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimeWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00'", "TIME '00:00:00.000000'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.1'", "TIME '00:00:00.100000'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.12'", "TIME '00:00:00.120000'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.123'", "TIME '00:00:00.123000'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.1234'", "TIME '00:00:00.123400'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.12345'", "TIME '00:00:00.123450'");
        testWriteDefaultValue(format, "TIME", "TIME '00:00:00.123456'", "TIME '00:00:00.123456'");
        testWriteDefaultValue(format, "TIME", "TIME '23:59:59.999999'", "TIME '23:59:59.999999'");
        testWriteDefaultValue(format, "TIME", "NULL", "CAST(NULL AS TIME(6))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimeInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.TimeType.get(), Literal.of("00:00:00").to(Types.TimeType.get()), "TIME '00:00:00.000000'");
        testInitialDefaultValue(format, Types.TimeType.get(), Literal.of("00:00:00.1").to(Types.TimeType.get()), "TIME '00:00:00.100000'");
        testInitialDefaultValue(format, Types.TimeType.get(), Literal.of("00:00:00.12").to(Types.TimeType.get()), "TIME '00:00:00.120000'");
        testInitialDefaultValue(format, Types.TimeType.get(), Literal.of("00:00:00.123").to(Types.TimeType.get()), "TIME '00:00:00.123000'");
        testInitialDefaultValue(format, Types.TimeType.get(), Literal.of("00:00:00.1234").to(Types.TimeType.get()), "TIME '00:00:00.123400'");
        testInitialDefaultValue(format, Types.TimeType.get(), Literal.of("00:00:00.12345").to(Types.TimeType.get()), "TIME '00:00:00.123450'");
        testInitialDefaultValue(format, Types.TimeType.get(), Literal.of("00:00:00.123456").to(Types.TimeType.get()), "TIME '00:00:00.123456'");
        testInitialDefaultValue(format, Types.TimeType.get(), Literal.of("23:59:59.999999").to(Types.TimeType.get()), "TIME '23:59:59.999999'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56'", "TIMESTAMP '2025-01-23 12:34:56.000000'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.1'", "TIMESTAMP '2025-01-23 12:34:56.100000'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.12'", "TIMESTAMP '2025-01-23 12:34:56.120000'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.123'", "TIMESTAMP '2025-01-23 12:34:56.123000'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.1234'", "TIMESTAMP '2025-01-23 12:34:56.123400'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.12345'", "TIMESTAMP '2025-01-23 12:34:56.123450'");
        testWriteDefaultValue(format, "TIMESTAMP", "TIMESTAMP '2025-01-23 12:34:56.123456'", "TIMESTAMP '2025-01-23 12:34:56.123456'");

        // short timestamp literal on long timestamp type
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56'", "TIMESTAMP '2025-01-23 12:34:56.000000'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.1'", "TIMESTAMP '2025-01-23 12:34:56.100000'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.12'", "TIMESTAMP '2025-01-23 12:34:56.120000'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.123'", "TIMESTAMP '2025-01-23 12:34:56.123000'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.1234'", "TIMESTAMP '2025-01-23 12:34:56.123400'");
        testWriteDefaultValue(format, "TIMESTAMP(6)", "TIMESTAMP '2025-01-23 12:34:56.12345'", "TIMESTAMP '2025-01-23 12:34:56.123450'");

        testWriteDefaultValue(format, "TIMESTAMP", "NULL", "CAST(NULL AS TIMESTAMP(6))");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.TimestampType.withoutZone(), Literal.of("2025-01-23T12:34:56"), "TIMESTAMP '2025-01-23 12:34:56.000000'");
        testInitialDefaultValue(format, Types.TimestampType.withoutZone(), Literal.of("2025-01-23T12:34:56.1"), "TIMESTAMP '2025-01-23 12:34:56.100000'");
        testInitialDefaultValue(format, Types.TimestampType.withoutZone(), Literal.of("2025-01-23T12:34:56.12"), "TIMESTAMP '2025-01-23 12:34:56.120000'");
        testInitialDefaultValue(format, Types.TimestampType.withoutZone(), Literal.of("2025-01-23T12:34:56.123"), "TIMESTAMP '2025-01-23 12:34:56.123000'");
        testInitialDefaultValue(format, Types.TimestampType.withoutZone(), Literal.of("2025-01-23T12:34:56.1234"), "TIMESTAMP '2025-01-23 12:34:56.123400'");
        testInitialDefaultValue(format, Types.TimestampType.withoutZone(), Literal.of("2025-01-23T12:34:56.12345"), "TIMESTAMP '2025-01-23 12:34:56.123450'");
        testInitialDefaultValue(format, Types.TimestampType.withoutZone(), Literal.of("2025-01-23T12:34:56.123456"), "TIMESTAMP '2025-01-23 12:34:56.123456'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampWithTimeZoneWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '0000-01-01 00:00:00 UTC'", "TIMESTAMP '0000-01-01 00:00:00.000000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'", "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'");

        // short timestamptz literal on long timestamptz type
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.000000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.100000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.120000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123000 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.1234 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123400 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.12345 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123450 UTC'");
        testWriteDefaultValue(format, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2025-01-23 12:34:56.123456 Europe/Warsaw'", "TIMESTAMP '2025-01-23 11:34:56.123456 UTC'");

        testWriteDefaultValue(format, "TIMESTAMP WITH TIME ZONE", "NULL", "CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testTimestampWithTimeZoneInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("0000-01-01T00:00:00+00:00"), "TIMESTAMP '0000-01-01 00:00:00.000000 UTC'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("9999-12-31T23:59:59.999999+00:00"), "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'");

        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2025-01-23T12:34:56+01:00"), "TIMESTAMP '2025-01-23 11:34:56.000000 UTC'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2025-01-23T12:34:56.1+01:00"), "TIMESTAMP '2025-01-23 11:34:56.100000 UTC'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2025-01-23T12:34:56.12+01:00"), "TIMESTAMP '2025-01-23 11:34:56.120000 UTC'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2025-01-23T12:34:56.123+01:00"), "TIMESTAMP '2025-01-23 11:34:56.123000 UTC'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2025-01-23T12:34:56.1234+01:00"), "TIMESTAMP '2025-01-23 11:34:56.123400 UTC'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2025-01-23T12:34:56.12345+01:00"), "TIMESTAMP '2025-01-23 11:34:56.123450 UTC'");
        testInitialDefaultValue(format, Types.TimestampType.withZone(), Literal.of("2025-01-23T12:34:56.123456+01:00"), "TIMESTAMP '2025-01-23 11:34:56.123456 UTC'");
    }

    @Test
    void testTimestampWithTimeZoneNanosLiteralUnsupportedRange()
    {
        // Update the above test once this test starts passing
        assertThatThrownBy(() -> Literal.of("1677-09-21T00:12:43.145224191+00:00").to(Types.TimestampNanoType.withZone()))
                .hasMessage("long overflow");
        assertThatThrownBy(() -> Literal.of("2262-04-11T23:47:16.854775808+00:00").to(Types.TimestampNanoType.withZone()))
                .hasMessage("long overflow");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVarcharWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "VARCHAR", "'test varchar'", "VARCHAR 'test varchar'");
        testWriteDefaultValue(format, "VARCHAR", "''", "VARCHAR ''");
        testWriteDefaultValue(format, "VARCHAR", "'攻殻機動隊'", "VARCHAR '攻殻機動隊'");
        testWriteDefaultValue(format, "VARCHAR", "'😂'", "VARCHAR '😂'");
        testWriteDefaultValue(format, "VARCHAR", "'a''singlequote'", "VARCHAR 'a''singlequote'");

        testWriteDefaultValue(format, "VARCHAR", "NULL", "CAST(NULL AS VARCHAR)");
        testWriteDefaultValue(format, "VARCHAR(255)", "NULL", "CAST(NULL AS VARCHAR)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVarcharInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.StringType.get(), Literal.of("test varchar"), "VARCHAR 'test varchar'");
        testInitialDefaultValue(format, Types.StringType.get(), Literal.of(""), "VARCHAR ''");
        testInitialDefaultValue(format, Types.StringType.get(), Literal.of("攻殻機動隊"), "VARCHAR '攻殻機動隊'");
        testInitialDefaultValue(format, Types.StringType.get(), Literal.of("😂"), "VARCHAR '😂'");
        testInitialDefaultValue(format, Types.StringType.get(), Literal.of("a'singlequote"), "VARCHAR 'a''singlequote'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testUuidWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "UUID", "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'", "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'");
        testWriteDefaultValue(format, "UUID", "NULL", "CAST(NULL AS UUID)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testUuidInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.UUIDType.get(), Literal.of("406caec7-68b9-4778-81b2-a12ece70c8b1"), "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVarbinaryWriteDefault(IcebergFileFormat format)
    {
        testWriteDefaultValue(format, "VARBINARY", "X'65683F'", "X'65683F'");
        testWriteDefaultValue(format, "VARBINARY", "NULL", "CAST(NULL AS VARBINARY)");
    }

    @ParameterizedTest
    @EnumSource(IcebergFileFormat.class)
    void testVarbinaryInitialDefault(IcebergFileFormat format)
    {
        testInitialDefaultValue(format, Types.BinaryType.get(), Literal.of(ByteBuffer.wrap("eh?".getBytes(UTF_8))), "X'65683F'");
    }

    @Test
    void testUnsupportedDefaultColumnValueWriteDefault()
    {
        assertQueryFails(
                "CREATE TABLE test_unsupported_default_column_value(x int DEFAULT 1) WITH (format_version=2)",
                "Default column values are not supported for Iceberg table format version < 3");

        try (TestTable table = newTrinoTable("test_unsupported_default_column_value", "(x int)  WITH (format_version=2)")) {
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN y int DEFAULT 1", "Default column values are not supported for Iceberg table format version < 3");
            assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN x SET DEFAULT 123", "Default column values are not supported for Iceberg table format version < 3");

            loadTable(table.getName()).updateSchema()
                    .updateColumnDefault("x", Literal.of(123))
                    .commit();
            assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN x DROP DEFAULT", "Default column values are not supported for Iceberg table format version < 3");
        }
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(
                tableName,
                getHiveMetastore(getQueryRunner()),
                getFileSystemFactory(getQueryRunner()),
                "iceberg",
                "tpch");
    }

    private void testWriteDefaultValue(IcebergFileFormat format, @Language("SQL") String type, @Language("SQL") String defaultValue, @Language("SQL") String expectedValue)
    {
        try (TestTable table = newTrinoTable("test_default_value", "(id int, data %s DEFAULT %s) WITH (format='%s')".formatted(type, defaultValue, format))) {
            assertUpdate("INSERT INTO " + table.getName() + "(id) VALUES 1", 1);

            assertThat(query("SELECT data FROM " + table.getName()))
                    .as("%s type expected %s", type, defaultValue)
                    .matches("VALUES " + expectedValue);
        }
    }

    private void testInitialDefaultValue(IcebergFileFormat format, Type type, Literal<?> defaultValue, @Language("SQL") String expectedValue)
    {
        try (TestTable table = newTrinoTable("test_initial_default", "WITH (format='" + format + "') AS SELECT 1 id")) {
            BaseTable icebergTable = loadTable(table.getName());
            icebergTable.updateSchema()
                    .addColumn("data", type, defaultValue)
                    .addColumn("part", type, defaultValue)
                    .commit();
            icebergTable.updateSpec()
                    .addField("part")
                    .commit();

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, " + expectedValue + ", " + expectedValue + ")");
        }
    }
}
