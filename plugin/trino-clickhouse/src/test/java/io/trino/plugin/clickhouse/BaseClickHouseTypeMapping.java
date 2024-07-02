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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.UuidType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAndTrinoInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_DEFAULT_SCALE;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_ROUNDING_MODE;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseClickHouseTypeMapping
        extends AbstractTestQueryFramework
{
    private final ZoneId jvmZone = ZoneId.systemDefault();

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    protected TestingClickHouseServer clickhouseServer;

    @BeforeAll
    public void setUp()
    {
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(vilnius, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(vilnius, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        LocalDate timeGapInKathmandu = LocalDate.of(1986, 1, 1);
        checkIsGap(kathmandu, timeGapInKathmandu.atStartOfDay());
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in ClickHouse and Trino
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in ClickHouse and Trino
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"))

                .addRoundTrip("Nullable(tinyint)", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_tinyint"));

        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-129", TINYINT, "TINYINT '127'")
                .addRoundTrip("tinyint", "128", TINYINT, "TINYINT '-128'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_tinyint"));
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in ClickHouse and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in ClickHouse and Trino
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"))

                .addRoundTrip("Nullable(smallint)", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_smallint"));

        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32769", SMALLINT, "SMALLINT '32767'")
                .addRoundTrip("smallint", "32768", SMALLINT, "SMALLINT '-32768'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in ClickHouse and Trino
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in ClickHouse and Trino
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"))

                .addRoundTrip("Nullable(integer)", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_int"));

        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483649", INTEGER, "INTEGER '2147483647'")
                .addRoundTrip("integer", "2147483648", INTEGER, "INTEGER '-2147483648'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_integer"));
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in ClickHouse and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in ClickHouse and Trino
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"))

                .addRoundTrip("Nullable(bigint)", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_bigint"));

        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775809", BIGINT, "BIGINT '9223372036854775807'")
                .addRoundTrip("bigint", "9223372036854775808", BIGINT, "BIGINT '-9223372036854775808'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_bigint"));
    }

    @Test
    public void testUint8()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("UInt8", "0", SMALLINT, "SMALLINT '0'") // min value in ClickHouse
                .addRoundTrip("UInt8", "255", SMALLINT, "SMALLINT '255'") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt8)", "NULL", SMALLINT, "CAST(null AS SMALLINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_uint8"));

        SqlDataTypeTest.create()
                .addRoundTrip("UInt8", "0", SMALLINT, "SMALLINT '0'") // min value in ClickHouse
                .addRoundTrip("UInt8", "255", SMALLINT, "SMALLINT '255'") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt8)", "NULL", SMALLINT, "CAST(null AS SMALLINT)")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_uint8"));
    }

    @Test
    public void testUnsupportedUint8()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("UInt8", "-1", SMALLINT, "SMALLINT '255'")
                .addRoundTrip("UInt8", "256", SMALLINT, "SMALLINT '0'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_uint8"));

        // Prevent writing incorrect results in the connector
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_uint8", "(value UInt8) ENGINE=Log")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (-1)", table.getName()),
                    "Value must be between 0 and 255 in ClickHouse: -1");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (256)", table.getName()),
                    "Value must be between 0 and 255 in ClickHouse: 256");
        }
    }

    @Test
    public void testUint16()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("UInt16", "0", INTEGER, "0") // min value in ClickHouse
                .addRoundTrip("UInt16", "65535", INTEGER, "65535") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt16)", "NULL", INTEGER, "CAST(null AS INTEGER)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_uint16"));

        SqlDataTypeTest.create()
                .addRoundTrip("UInt16", "0", INTEGER, "0") // min value in ClickHouse
                .addRoundTrip("UInt16", "65535", INTEGER, "65535") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt16)", "NULL", INTEGER, "CAST(null AS INTEGER)")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_uint16"));
    }

    @Test
    public void testUnsupportedUint16()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("UInt16", "-1", INTEGER, "65535")
                .addRoundTrip("UInt16", "65536", INTEGER, "0")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_uint16"));

        // Prevent writing incorrect results in the connector
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_uint16", "(value UInt16) ENGINE=Log")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (-1)", table.getName()),
                    "Value must be between 0 and 65535 in ClickHouse: -1");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (65536)", table.getName()),
                    "Value must be between 0 and 65535 in ClickHouse: 65536");
        }
    }

    @Test
    public void testUint32()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("UInt32", "0", BIGINT, "BIGINT '0'") // min value in ClickHouse
                .addRoundTrip("UInt32", "4294967295", BIGINT, "BIGINT '4294967295'") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt32)", "NULL", BIGINT, "CAST(null AS BIGINT)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_uint32"));

        SqlDataTypeTest.create()
                .addRoundTrip("UInt32", "BIGINT '0'", BIGINT, "BIGINT '0'") // min value in ClickHouse
                .addRoundTrip("UInt32", "BIGINT '4294967295'", BIGINT, "BIGINT '4294967295'") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt32)", "NULL", BIGINT, "CAST(null AS BIGINT)")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_uint32"));
    }

    @Test
    public void testUnsupportedUint32()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("UInt32", "-1", BIGINT, "BIGINT '4294967295'")
                .addRoundTrip("UInt32", "4294967296", BIGINT, "BIGINT '0'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_uint32"));

        // Prevent writing incorrect results in the connector
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_uint32", "(value UInt32) ENGINE=Log")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (CAST('-1' AS BIGINT))", table.getName()),
                    "Value must be between 0 and 4294967295 in ClickHouse: -1");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (CAST('4294967296' AS BIGINT))", table.getName()),
                    "Value must be between 0 and 4294967295 in ClickHouse: 4294967296");
        }
    }

    @Test
    public void testUint64()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("UInt64", "0", createDecimalType(20), "CAST('0' AS decimal(20, 0))") // min value in ClickHouse
                .addRoundTrip("UInt64", "18446744073709551615", createDecimalType(20), "CAST('18446744073709551615' AS decimal(20, 0))") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt64)", "NULL", createDecimalType(20), "CAST(null AS decimal(20, 0))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_uint64"));

        SqlDataTypeTest.create()
                .addRoundTrip("UInt64", "CAST('0' AS decimal(20, 0))", createDecimalType(20), "CAST('0' AS decimal(20, 0))") // min value in ClickHouse
                .addRoundTrip("UInt64", "CAST('18446744073709551615' AS decimal(20, 0))", createDecimalType(20), "CAST('18446744073709551615' AS decimal(20, 0))") // max value in ClickHouse
                .addRoundTrip("Nullable(UInt64)", "NULL", createDecimalType(20), "CAST(null AS decimal(20, 0))")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_uint64"));
    }

    @Test
    public void testUnsupportedUint64()
    {
        // ClickHouse stores incorrect results when the values are out of supported range. This test should be fixed when ClickHouse changes the behavior.
        SqlDataTypeTest.create()
                .addRoundTrip("UInt64", "-1", createDecimalType(20), "CAST('18446744073709551615' AS decimal(20, 0))")
                .addRoundTrip("UInt64", "18446744073709551616", createDecimalType(20), "CAST('0' AS decimal(20, 0))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_unsupported_uint64"));

        // Prevent writing incorrect results in the connector
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_uint64", "(value UInt64) ENGINE=Log")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (CAST('-1' AS decimal(20, 0)))", table.getName()),
                    "Value must be between 0 and 18446744073709551615 in ClickHouse: -1");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (CAST('18446744073709551616' AS decimal(20, 0)))", table.getName()),
                    "Value must be between 0 and 18446744073709551615 in ClickHouse: 18446744073709551616");
        }
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "12.5", REAL, "REAL '12.5'")
                .addRoundTrip("real", "nan()", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("real", "-infinity()", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("real", "+infinity()", REAL, "CAST(+infinity() AS REAL)")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "12.5", REAL, "REAL '12.5'")
                .addRoundTrip("real", "nan", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("real", "-inf", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("real", "+inf", REAL, "CAST(+infinity() AS REAL)")
                .addRoundTrip("Nullable(real)", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "3.1415926835", DOUBLE, "DOUBLE '3.1415926835'")
                .addRoundTrip("double", "1.79769E308", DOUBLE, "DOUBLE '1.79769E308'")

                // https://github.com/ClickHouse/ClickHouse/issues/60146
                // .addRoundTrip("double", "2.225E-307", DOUBLE, "DOUBLE '2.225E-307'")

                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_double"))

                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")

                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(double)", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.trino_test_nullable_double"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('123456789.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST('12345678901234567890.31' AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 38)", "CAST('0.27182818284590452353602874713526624977' AS decimal(38, 38))", createDecimalType(38, 38), "CAST('0.27182818284590452353602874713526624977' AS decimal(38, 38))")
                .addRoundTrip("decimal(38, 38)", "CAST('-0.27182818284590452353602874713526624977' AS decimal(38, 38))", createDecimalType(38, 38), "CAST('-0.27182818284590452353602874713526624977' AS decimal(38, 38))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));

        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(decimal(3, 1))", "NULL", createDecimalType(3, 1), "CAST(NULL AS decimal(3,1))")
                .addRoundTrip("Nullable(decimal(3, 1))", "NULL", createDecimalType(3, 1), "CAST(NULL AS decimal(3,1))")
                .addRoundTrip("Nullable(decimal(30, 5))", "NULL", createDecimalType(30, 5), "CAST(NULL AS decimal(30,5))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_nullable_decimal"));

        SqlDataTypeTest.create()
                .addRoundTrip(format("Decimal(%d, 5)", Decimals.MAX_PRECISION + 1), "1.1", createDecimalType(Decimals.MAX_PRECISION, 5), format("CAST(1.1 AS DECIMAL(%d, 5))", Decimals.MAX_PRECISION))
                .execute(getQueryRunner(), sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 5), clickhouseCreateAndInsert("tpch.test_unspecified_decimal"));
    }

    @Test
    public void testDecimalExceedingPrecisionIsIgnored()
    {
        testUnsupportedDataTypeIsIgnored("decimal(50,0)", "'12345678901234567890123456789012345678901234567890'");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxConvertedToVarchar()
    {
        testUnsupportedDataTypeConvertedToVarchar(
                getSession(),
                "Nullable(Decimal(50,0))",
                "12345678901234567890123456789012345678901234567890",
                "'12345678901234567890123456789012345678901234567890'");
    }

    protected Session sessionWithDecimalMappingAllowOverflow(RoundingMode roundingMode, int scale)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("clickhouse", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("clickhouse", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    protected Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    public void testUnsupportedDataTypeIsIgnored(String dataType, String dataValue)
    {
        JdbcSqlExecutor jse = new JdbcSqlExecutor(clickhouseServer.getJdbcUrl());
        try (TestTable table = new TestTable(
                jse,
                "tpch.test_unsupported_decimal",
                format("(i Int32, ut %s) ENGINE=Log", dataType),
                ImmutableList.of("1, " + dataValue))) {
            assertQuery(format("SELECT * FROM %s", table.getName()), "VALUES 1");
            assertQuery(format("DESC %s", table.getName()), "VALUES ('i', 'integer','', '')"); // no 'unsupported_column'

            assertUpdate(format("INSERT INTO %s VALUES 3", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 1, 3");
        }
    }

    private void testUnsupportedDataTypeConvertedToVarchar(Session session, String dataTypeName, String databaseValue, String trinoValue)
    {
        JdbcSqlExecutor jse = new JdbcSqlExecutor(clickhouseServer.getJdbcUrl());
        try (TestTable table = new TestTable(
                jse,
                "tpch.unsupported_type",
                format("(key Int32, unsupported_column %s) Engine=Log", dataTypeName),
                ImmutableList.of("1, NULL", "2, " + databaseValue))) {
            Session convertToVarchar = Session.builder(session)
                    .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                    .build();
            assertQuery(
                    convertToVarchar,
                    "SELECT * FROM " + table.getName(),
                    format("VALUES (1, NULL), (2, %s)", trinoValue));
            assertQuery(
                    convertToVarchar,
                    format("SELECT key FROM %s WHERE unsupported_column = %s", table.getName(), trinoValue),
                    "VALUES 2");
            assertQuery(
                    convertToVarchar,
                    "DESC " + table.getName(),
                    "VALUES " +
                            "('key', 'integer', '', ''), " +
                            "('unsupported_column', 'varchar', '', '')");
            assertUpdate(
                    convertToVarchar,
                    format("INSERT INTO %s (key, unsupported_column) VALUES (3, NULL)", table.getName()),
                    1);
            assertQueryFails(
                    convertToVarchar,
                    format("INSERT INTO %s (key, unsupported_column) VALUES (4, %s)", table.getName(), trinoValue),
                    ".*Underlying type that is mapped to VARCHAR is not supported for INSERT.*");
            assertUpdate(
                    convertToVarchar,
                    format("INSERT INTO %s (key) VALUES 5", table.getName()),
                    1);
            assertQuery(
                    convertToVarchar,
                    "SELECT * FROM " + table.getName(),
                    format("VALUES (1, NULL), (2, %s), (3, NULL), (5, NULL)", trinoValue));
        }
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithExceedingIntegerValues()
    {
        JdbcSqlExecutor jse = new JdbcSqlExecutor(clickhouseServer.getJdbcUrl());

        try (TestTable testTable = new TestTable(jse, "tpch.test_exceeding_max_decimal",
                "(d_col decimal(65,25)) Engine=Log",
                asList("1234567890123456789012345678901234567890.123456789", "-1234567890123456789012345678901234567890.123456789"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES ('1234567890123456789012345678901234567890.1234567890000000000000000'), ('-1234567890123456789012345678901234567890.1234567890000000000000000')");
        }
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithNonExceedingIntegerValues()
    {
        JdbcSqlExecutor jse = new JdbcSqlExecutor(clickhouseServer.getJdbcUrl());

        try (TestTable testTable = new TestTable(
                jse,
                "tpch.test_exceeding_max_decimal",
                "(d_col decimal(60,20)) Engine=Log",
                asList("123456789012345678901234567890.123456789012345", "-123456789012345678901234567890.123456789012345"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (123456789012345678901234567890), (-123456789012345678901234567890)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'decimal(38,8)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (123456789012345678901234567890.12345679), (-123456789012345678901234567890.12345679)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 22),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'decimal(38,20)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 20),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 9),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES ('123456789012345678901234567890.12345678901234500000'), ('-123456789012345678901234567890.12345678901234500000')");
        }
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithSupportedValues()
    {
        testDecimalExceedingPrecisionMaxWithSupportedValues(40, 8);
        testDecimalExceedingPrecisionMaxWithSupportedValues(50, 10);
    }

    private void testDecimalExceedingPrecisionMaxWithSupportedValues(int typePrecision, int typeScale)
    {
        JdbcSqlExecutor jse = new JdbcSqlExecutor(clickhouseServer.getJdbcUrl());
        try (TestTable testTable = new TestTable(
                jse,
                "tpch.test_exceeding_max_decimal",
                format("(d_col decimal(%d,%d)) Engine=Log", typePrecision, typeScale),
                asList("12.01", "-12.01", "123", "-123", "1.12345678", "-1.12345678"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12), (-12), (123), (-123), (1), (-1)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 3),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'decimal(38,3)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.123), (-1.123)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col', 'decimal(38,8)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 9),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
        }
    }

    protected String omitDatabasePrefix(String tableName)
    {
        String[] components = tableName.split("\\.");
        return components[components.length - 1];
    }

    @Test
    public void testClickHouseChar()
    {
        // ClickHouse char is String, which is arbitrary bytes
        textAsBinaryRoundTripTest("char(255)")
                // plain
                .addRoundTrip("char(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("char(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // nullable
                .addRoundTrip("Nullable(char(10))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(char(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("Nullable(char(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("Nullable(char(255))", "''", VARBINARY, "X''")
                // low-cardinality
                .addRoundTrip("LowCardinality(char(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(char(255))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(char(5))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(char(32))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(char(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(char(77))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(char(10)))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("LowCardinality(Nullable(char(10)))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(Nullable(char(1)))", "'😂'", VARBINARY, "to_utf8('😂')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_char"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("char(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("char(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(char(10))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(char(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("Nullable(char(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                // low-cardinality
                .addRoundTrip("LowCardinality(char(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(char(255))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(char(5))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(char(32))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(char(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(char(77))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(char(10)))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(char(10)))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(char(1)))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_char"));
    }

    @Test
    public void testClickHouseFixedString()
    {
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("FixedString(10)", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("FixedString(10)", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .addRoundTrip("FixedString(10)", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello\0\0\0\0\0')")
                .addRoundTrip("FixedString(10)", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'00000000000000000000'")
                // nullable
                .addRoundTrip("Nullable(FixedString(10))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(FixedString(10))", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("Nullable(FixedString(10))", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .addRoundTrip("Nullable(FixedString(10))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'00000000000000000000'")
                // low-cardinality
                .addRoundTrip("LowCardinality(FixedString(10))", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("LowCardinality(FixedString(10))", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .addRoundTrip("LowCardinality(FixedString(10))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'00000000000000000000'")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'c12345678b'", VARBINARY, "to_utf8('c12345678b')")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'c123'", VARBINARY, "to_utf8('c123\0\0\0\0\0\0')")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'00000000000000000000'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_fixed_string"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("FixedString(10)", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("FixedString(10)", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .addRoundTrip("FixedString(10)", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARCHAR, "CAST('hello\0\0\0\0\0' as varchar)")
                .addRoundTrip("FixedString(10)", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARCHAR, "CAST('\0\0\0\0\0\0\0\0\0\0' as varchar)")
                // nullable
                .addRoundTrip("Nullable(FixedString(10))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARCHAR, "CAST('hello\0\0\0\0\0' as varchar)")
                .addRoundTrip("Nullable(FixedString(10))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARCHAR, "CAST('\0\0\0\0\0\0\0\0\0\0' as varchar)")
                // low-cardinality
                .addRoundTrip("LowCardinality(FixedString(10))", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("LowCardinality(FixedString(10))", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .addRoundTrip("LowCardinality(FixedString(10))", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARCHAR, "CAST('hello\0\0\0\0\0' as varchar)")
                .addRoundTrip("LowCardinality(FixedString(10))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARCHAR, "CAST('\0\0\0\0\0\0\0\0\0\0' as varchar)")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'c12345678b'", VARCHAR, "CAST('c12345678b' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'c123'", VARCHAR, "CAST('c123\0\0\0\0\0\0' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'\\x68\\x65\\x6C\\x6C\\x6F'", VARCHAR, "CAST('hello\0\0\0\0\0' as varchar)")
                .addRoundTrip("LowCardinality(Nullable(FixedString(10)))", "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARCHAR, "CAST('\0\0\0\0\0\0\0\0\0\0' as varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_fixed_string"));
    }

    @Test
    public void testTrinoChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("char(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("char(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("char(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_char"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("char(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("char(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("char(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("char(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("char(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_char"));
    }

    @Test
    public void testClickHouseVarchar()
    {
        // ClickHouse varchar is String, which is arbitrary bytes
        textAsBinaryRoundTripTest("varchar(255)")
                // plain
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varchar(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("varchar(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("varchar(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // nullable
                .addRoundTrip("Nullable(varchar(30))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(varchar(30))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("Nullable(varchar(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("Nullable(varchar(255))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("Nullable(varchar(5))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("Nullable(varchar(32))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("Nullable(varchar(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("Nullable(varchar(77))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality
                .addRoundTrip("LowCardinality(varchar(30))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("LowCardinality(varchar(10))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(varchar(255))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(varchar(5))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(varchar(32))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(varchar(1))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(varchar(77))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(varchar(30)))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("LowCardinality(Nullable(varchar(30)))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("LowCardinality(Nullable(varchar(10)))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(Nullable(varchar(255)))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(Nullable(varchar(5)))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(Nullable(varchar(32)))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(Nullable(varchar(1)))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(Nullable(varchar(77)))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("varchar(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(varchar(30))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(varchar(30))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("Nullable(varchar(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("Nullable(varchar(255))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("Nullable(varchar(5))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Nullable(varchar(32))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Nullable(varchar(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("Nullable(varchar(77))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality
                .addRoundTrip("LowCardinality(varchar(30))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(10))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(255))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(5))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(32))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(1))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(varchar(77))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(varchar(30)))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(30)))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(10)))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(255)))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(5)))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(32)))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(1)))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(varchar(77)))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_varchar"));
    }

    private static SqlDataTypeTest textAsBinaryRoundTripTest(String inputType)
    {
        String nullInputType = format("Nullable(%s)", inputType);
        String lowCardInputType = format("LowCardinality(%s)", inputType);
        String nullLowCardInputType = format("LowCardinality(Nullable(%s))", inputType);

        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "''", VARBINARY, "X''")
                .addRoundTrip(inputType, "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(inputType, "'\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(inputType, "'\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(inputType, "'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(inputType, "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'000000000000'")
                .addRoundTrip(nullInputType, "''", VARBINARY, "X''")
                .addRoundTrip(nullInputType, "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(nullInputType, "'\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(nullInputType, "'\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(nullInputType, "'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(nullInputType, "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'000000000000'")
                .addRoundTrip(lowCardInputType, "''", VARBINARY, "X''")
                .addRoundTrip(lowCardInputType, "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(lowCardInputType, "'\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(lowCardInputType, "'\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(lowCardInputType, "'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(lowCardInputType, "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'000000000000'")
                .addRoundTrip(nullLowCardInputType, "''", VARBINARY, "X''")
                .addRoundTrip(nullLowCardInputType, "'\\x68\\x65\\x6C\\x6C\\x6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(nullLowCardInputType, "'\\x50\\x69\\xC4\\x99\\x6B\\x6E\\x61\\x20\\xC5\\x82\\xC4\\x85\\x6B\\x61\\x20\\x77\\x20\\xE6\\x9D\\xB1\\xE4\\xBA\\xAC\\xE9\\x83\\xBD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(nullLowCardInputType, "'\\x42\\x61\\x67\\x20\\x66\\x75\\x6C\\x6C\\x20\\x6F\\x66\\x20\\xF0\\x9F\\x92\\xB0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(nullLowCardInputType, "'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x0D\\xF9\\x36\\x7A\\xA7\\x00\\x00\\x00'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip(nullLowCardInputType, "'\\x00\\x00\\x00\\x00\\x00\\x00'", VARBINARY, "X'000000000000'");
    }

    @Test
    public void testClickHouseString()
    {
        // TODO add more test cases
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("String", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("String", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("String", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("String", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("String", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("String", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("String", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // nullable
                .addRoundTrip("Nullable(String)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("Nullable(String)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("Nullable(String)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("Nullable(String)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("Nullable(String)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("Nullable(String)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("Nullable(String)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("Nullable(String)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality
                .addRoundTrip("LowCardinality(String)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("LowCardinality(String)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(String)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(String)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(String)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(String)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(String)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(String))", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("LowCardinality(Nullable(String))", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                // plain
                .addRoundTrip("String", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("String", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("String", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("String", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("String", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("String", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("String", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // nullable
                .addRoundTrip("Nullable(String)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("Nullable(String)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("Nullable(String)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("Nullable(String)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("Nullable(String)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Nullable(String)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Nullable(String)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("Nullable(String)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality
                .addRoundTrip("LowCardinality(String)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(String)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // low-cardinality nullable
                .addRoundTrip("LowCardinality(Nullable(String))", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("LowCardinality(Nullable(String))", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), clickhouseCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testTrinoVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(30)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varchar(10)", "'text_a'", VARBINARY, "to_utf8('text_a')")
                .addRoundTrip("varchar(255)", "'text_b'", VARBINARY, "to_utf8('text_b')")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARBINARY, "to_utf8('攻殻機動隊')")
                .addRoundTrip("varchar(1)", "'😂'", VARBINARY, "to_utf8('😂')")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARBINARY, "to_utf8('Ну, погоди!')")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_varchar"));

        // Set map_string_as_varchar session property as true
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(30)", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("varchar(30)", "'Piękna łąka w 東京都'", VARCHAR, "CAST('Piękna łąka w 東京都' AS varchar)")
                .addRoundTrip("varchar(10)", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar(255)", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar(1)", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), mapStringAsVarcharSession(), trinoCreateAsSelect(mapStringAsVarcharSession(), "test_varchar"));
    }

    @Test
    public void testTrinoVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "X''")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"));
    }

    @Test
    public void testDate()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();
            SqlDataTypeTest.create()
                    .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                    .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                    .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                    .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));

            // Null
            SqlDataTypeTest.create()
                    .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
            SqlDataTypeTest.create()
                    .addRoundTrip("Nullable(date)", "NULL", DATE, "CAST(NULL AS DATE)")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"));
        }
    }

    @Test
    public void testClickHouseDateMinMaxValues()
    {
        testClickHouseDateMinMaxValues("1970-01-01");
        testClickHouseDateMinMaxValues("2149-06-06");
    }

    private void testClickHouseDateMinMaxValues(String date)
    {
        SqlDataTypeTest dateTests = SqlDataTypeTest.create()
                .addRoundTrip("date", format("DATE '%s'", date), DATE, format("DATE '%s'", date));

        for (ZoneId timeZoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId.getId()))
                    .build();
            dateTests
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
        }
    }

    @Test
    public void testUnsupportedDate()
    {
        testUnsupportedDate("1969-12-31");
        testUnsupportedDate("2149-06-07");
    }

    private void testUnsupportedDate(String unsupportedDate)
    {
        String minSupportedDate = "1970-01-01";
        String maxSupportedDate = "2149-06-06";

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_unsupported_date", "(dt date)")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (DATE '%s')", table.getName(), unsupportedDate),
                    format("Date must be between %s and %s in ClickHouse: %s", minSupportedDate, maxSupportedDate, unsupportedDate));
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_date", "(dt date) ENGINE=Log")) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES ('%s')", table.getName(), unsupportedDate));
            assertQuery(format("SELECT dt <> DATE '%s' FROM %s", unsupportedDate, table.getName()), "SELECT true"); // Inserting an unsupported date in ClickHouse will turn it into another date
        }
    }

    @Test
    public void testTimestamp()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            SqlDataTypeTest.create()
                    .addRoundTrip("timestamp(0)", "timestamp '1986-01-01 00:13:07'", createTimestampType(0), "TIMESTAMP '1986-01-01 00:13:07'") // time gap in Kathmandu
                    .addRoundTrip("timestamp(0)", "timestamp '2018-03-25 03:17:17'", createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'") // time gap in Vilnius
                    .addRoundTrip("timestamp(0)", "timestamp '2018-10-28 01:33:17'", createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'") // time doubled in JVM zone
                    .addRoundTrip("timestamp(0)", "timestamp '2018-10-28 03:33:33'", createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'") // time double in Vilnius
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));

            timestampTest("timestamp")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_timestamp"));
            timestampTest("datetime")
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_datetime"));
        }
    }

    private SqlDataTypeTest timestampTest(String inputType)
    {
        return unsupportedTimestampBecomeUnexpectedValueTest(inputType)
                .addRoundTrip(inputType, "'1986-01-01 00:13:07'", createTimestampType(0), "TIMESTAMP '1986-01-01 00:13:07'") // time gap in Kathmandu
                .addRoundTrip(inputType, "'2018-03-25 03:17:17'", createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'") // time gap in Vilnius
                .addRoundTrip(inputType, "'2018-10-28 01:33:17'", createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'") // time doubled in JVM zone
                .addRoundTrip(inputType, "'2018-10-28 03:33:33'", createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'") // time double in Vilnius
                .addRoundTrip(format("Nullable(%s)", inputType), "NULL", createTimestampType(0), "CAST(NULL AS TIMESTAMP(0))");
    }

    protected SqlDataTypeTest unsupportedTimestampBecomeUnexpectedValueTest(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "'1969-12-31 23:59:59'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'");
    }

    @Test
    public void testClickHouseDateTimeMinMaxValues()
    {
        testClickHouseDateTimeMinMaxValues("1970-01-01 00:00:00"); // min value in ClickHouse
        testClickHouseDateTimeMinMaxValues("2106-02-07 06:28:15"); // max value in ClickHouse
    }

    private void testClickHouseDateTimeMinMaxValues(String timestamp)
    {
        SqlDataTypeTest dateTests1 = SqlDataTypeTest.create()
                .addRoundTrip("timestamp(0)", format("timestamp '%s'", timestamp), createTimestampType(0), format("TIMESTAMP '%s'", timestamp));
        SqlDataTypeTest dateTests2 = SqlDataTypeTest.create()
                .addRoundTrip("timestamp", format("'%s'", timestamp), createTimestampType(0), format("TIMESTAMP '%s'", timestamp));
        SqlDataTypeTest dateTests3 = SqlDataTypeTest.create()
                .addRoundTrip("datetime", format("'%s'", timestamp), createTimestampType(0), format("TIMESTAMP '%s'", timestamp));

        for (ZoneId timeZoneId : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId.getId()))
                    .build();
            dateTests1
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                    .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
            dateTests2.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_timestamp"));
            dateTests3.execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.test_datetime"));
        }
    }

    @Test
    public void testUnsupportedTimestamp()
    {
        testUnsupportedTimestamp("1969-12-31 23:59:59"); // min - 1 second
        testUnsupportedTimestamp("2106-02-07 06:28:16"); // max + 1 second
    }

    public void testUnsupportedTimestamp(String unsupportedTimestamp)
    {
        String minSupportedTimestamp = "1970-01-01 00:00:00";
        String maxSupportedTimestamp = "2106-02-07 06:28:15";

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_unsupported_timestamp", "(dt timestamp(0))")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (TIMESTAMP '%s')", table.getName(), unsupportedTimestamp),
                    format("Timestamp must be between %s and %s in ClickHouse: %s", minSupportedTimestamp, maxSupportedTimestamp, unsupportedTimestamp));
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_unsupported_timestamp", "(dt datetime) ENGINE=Log")) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES ('%s')", table.getName(), unsupportedTimestamp));
            assertQuery(format("SELECT dt <> TIMESTAMP '%s' FROM %s", unsupportedTimestamp, table.getName()), "SELECT true"); // Inserting an unsupported datetime in ClickHouse will turn it into another datetime
        }
    }

    @Test
    public void testClickHouseDateTimeWithTimeZone()
    {
        for (ZoneId sessionZone : timezones()) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .build();

            dateTimeWithTimeZoneTest(clickhouseDateTimeInputTypeFactory("datetime"))
                    .execute(getQueryRunner(), session, clickhouseCreateAndInsert("tpch.datetime_tz"));
        }
    }

    private SqlDataTypeTest dateTimeWithTimeZoneTest(Function<ZoneId, String> inputTypeFactory)
    {
        ZoneId utc = ZoneId.of("UTC");
        SqlDataTypeTest tests = SqlDataTypeTest.create()
                .addRoundTrip(format("Nullable(%s)", inputTypeFactory.apply(utc)), "NULL", TIMESTAMP_TZ_SECONDS, "CAST(NULL AS TIMESTAMP(0) WITH TIME ZONE)")

                // Since ClickHouse datetime(timezone) does not support values before epoch, we do not test this here.

                // epoch
                .addRoundTrip(inputTypeFactory.apply(utc), "0", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 00:00:00 Z'")
                .addRoundTrip(inputTypeFactory.apply(utc), "'1970-01-01 00:00:00'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 00:00:00 Z'")
                .addRoundTrip(inputTypeFactory.apply(kathmandu), "'1970-01-01 00:00:00'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 05:30:00 +05:30'")

                // after epoch
                .addRoundTrip(inputTypeFactory.apply(utc), "'2019-03-18 10:01:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2019-03-18 10:01:17 Z'")
                .addRoundTrip(inputTypeFactory.apply(kathmandu), "'2019-03-18 10:01:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2019-03-18 10:01:17 +05:45'")
                .addRoundTrip(inputTypeFactory.apply(ZoneId.of("GMT")), "'2019-03-18 10:01:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2019-03-18 10:01:17 Z'")
                .addRoundTrip(inputTypeFactory.apply(ZoneId.of("UTC+00:00")), "'2019-03-18 10:01:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2019-03-18 10:01:17 Z'")

                // time doubled in JVM zone
                .addRoundTrip(inputTypeFactory.apply(utc), "'2018-10-28 01:33:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 01:33:17 Z'")
                .addRoundTrip(inputTypeFactory.apply(jvmZone), "'2018-10-28 01:33:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 01:33:17 -05:00'")
                .addRoundTrip(inputTypeFactory.apply(kathmandu), "'2018-10-28 01:33:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 01:33:17 +05:45'")

                // time doubled in Vilnius
                .addRoundTrip(inputTypeFactory.apply(utc), "'2018-10-28 03:33:33'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 03:33:33 Z'")
                .addRoundTrip(inputTypeFactory.apply(vilnius), "'2018-10-28 03:33:33'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 03:33:33 +03:00'")
                .addRoundTrip(inputTypeFactory.apply(kathmandu), "'2018-10-28 03:33:33'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-10-28 03:33:33 +05:45'")

                // time gap in JVM zone
                .addRoundTrip(inputTypeFactory.apply(utc), "'1970-01-01 00:13:42'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 00:13:42 Z'")
                // TODO: Check the range of DateTime(timezone) values written from Trino to ClickHouse to prevent ClickHouse from storing incorrect results.
                //       e.g. 1970-01-01 00:13:42 will become 1970-01-01 05:30:00
                // .addRoundTrip(inputTypeFactory.apply(kathmandu), "'1970-01-01 00:13:42'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1970-01-01 00:13:42 +05:30'")
                .addRoundTrip(inputTypeFactory.apply(utc), "'2018-04-01 02:13:55'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-04-01 02:13:55 Z'")
                .addRoundTrip(inputTypeFactory.apply(kathmandu), "'2018-04-01 02:13:55'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-04-01 02:13:55 +05:45'")

                // time gap in Vilnius
                .addRoundTrip(inputTypeFactory.apply(kathmandu), "'2018-03-25 03:17:17'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '2018-03-25 03:17:17 +05:45'")

                // time gap in Kathmandu
                .addRoundTrip(inputTypeFactory.apply(vilnius), "'1986-01-01 00:13:07'", TIMESTAMP_TZ_SECONDS, "TIMESTAMP '1986-01-01 00:13:07 +03:00'");

        return tests;
    }

    private List<ZoneId> timezones()
    {
        return ImmutableList.of(
                UTC,
                jvmZone,
                // using two non-JVM zones so that we don't need to worry what ClickHouse system zone is
                vilnius,
                kathmandu,
                TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId());
    }

    @Test
    public void testEnum()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Enum('hello' = 1, 'world' = 2)", "'hello'", createUnboundedVarcharType(), "VARCHAR 'hello'")
                .addRoundTrip("Enum('hello' = 1, 'world' = 2)", "'world'", createUnboundedVarcharType(), "VARCHAR 'world'")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_enum"));
    }

    @Test
    public void testUuid()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Nullable(UUID)", "NULL", UuidType.UUID, "CAST(NULL AS UUID)")
                .addRoundTrip("Nullable(UUID)", "'114514ea-0601-1981-1142-e9b55b0abd6d'", UuidType.UUID, "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS UUID)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("default.ck_test_uuid"));

        SqlDataTypeTest.create()
                .addRoundTrip("CAST(NULL AS UUID)", "cast(NULL as UUID)")
                .addRoundTrip("UUID '114514ea-0601-1981-1142-e9b55b0abd6d'", "CAST('114514ea-0601-1981-1142-e9b55b0abd6d' AS UUID)")
                .execute(getQueryRunner(), trinoCreateAsSelect("default.ck_test_uuid"))
                .execute(getQueryRunner(), trinoCreateAndInsert("default.ck_test_uuid"));
    }

    @Test
    public void testIp()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("IPv4", "'0.0.0.0'", IPADDRESS, "IPADDRESS '0.0.0.0'")
                .addRoundTrip("IPv4", "'116.253.40.133'", IPADDRESS, "IPADDRESS '116.253.40.133'")
                .addRoundTrip("IPv4", "'255.255.255.255'", IPADDRESS, "IPADDRESS '255.255.255.255'")
                .addRoundTrip("IPv6", "'::'", IPADDRESS, "IPADDRESS '::'")
                .addRoundTrip("IPv6", "'2001:44c8:129:2632:33:0:252:2'", IPADDRESS, "IPADDRESS '2001:44c8:129:2632:33:0:252:2'")
                .addRoundTrip("IPv6", "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", IPADDRESS, "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'")
                .addRoundTrip("Nullable(IPv4)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .addRoundTrip("Nullable(IPv6)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_ip"));

        SqlDataTypeTest.create()
                .addRoundTrip("IPv4", "IPADDRESS '0.0.0.0'", IPADDRESS, "IPADDRESS '0.0.0.0'")
                .addRoundTrip("IPv4", "IPADDRESS '116.253.40.133'", IPADDRESS, "IPADDRESS '116.253.40.133'")
                .addRoundTrip("IPv4", "IPADDRESS '255.255.255.255'", IPADDRESS, "IPADDRESS '255.255.255.255'")
                .addRoundTrip("IPv6", "IPADDRESS '::'", IPADDRESS, "IPADDRESS '::'")
                .addRoundTrip("IPv6", "IPADDRESS '2001:44c8:129:2632:33:0:252:2'", IPADDRESS, "IPADDRESS '2001:44c8:129:2632:33:0:252:2'")
                .addRoundTrip("IPv6", "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", IPADDRESS, "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'")
                .addRoundTrip("Nullable(IPv4)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .addRoundTrip("Nullable(IPv6)", "NULL", IPADDRESS, "CAST(NULL AS IPADDRESS)")
                .execute(getQueryRunner(), clickhouseCreateAndTrinoInsert("tpch.test_ip"));
    }

    protected static Session mapStringAsVarcharSession()
    {
        return testSessionBuilder()
                .setCatalog("clickhouse")
                .setSchema(TPCH_SCHEMA)
                .setCatalogSessionProperty("clickhouse", "map_string_as_varchar", "true")
                .build();
    }

    protected DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    protected DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    protected DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    protected DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    protected DataSetup clickhouseCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new ClickHouseSqlExecutor(onRemoteDatabase()), tableNamePrefix);
    }

    protected DataSetup clickhouseCreateAndTrinoInsert(String tableNamePrefix)
    {
        return new CreateAndTrinoInsertDataSetup(new ClickHouseSqlExecutor(onRemoteDatabase()), new TrinoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    protected SqlExecutor onRemoteDatabase()
    {
        return clickhouseServer::execute;
    }

    private static Function<ZoneId, String> clickhouseDateTimeInputTypeFactory(String inputTypePrefix)
    {
        return zone -> format("%s('%s')", inputTypePrefix, zone);
    }
}
