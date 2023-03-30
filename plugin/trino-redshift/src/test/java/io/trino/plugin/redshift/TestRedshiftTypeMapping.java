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
package io.trino.plugin.redshift;

import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.io.BaseEncoding.base16;
import static io.trino.plugin.redshift.RedshiftClient.REDSHIFT_MAX_VARCHAR;
import static io.trino.plugin.redshift.RedshiftQueryRunner.JDBC_PASSWORD;
import static io.trino.plugin.redshift.RedshiftQueryRunner.JDBC_URL;
import static io.trino.plugin.redshift.RedshiftQueryRunner.JDBC_USER;
import static io.trino.plugin.redshift.RedshiftQueryRunner.TEST_SCHEMA;
import static io.trino.plugin.redshift.RedshiftQueryRunner.createRedshiftQueryRunner;
import static io.trino.plugin.redshift.RedshiftQueryRunner.executeInRedshift;
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
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRedshiftTypeMapping
        extends AbstractTestQueryFramework
{
    private static final ZoneId testZone = TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId();

    private final ZoneId jvmZone = ZoneId.systemDefault();
    private final LocalDateTime timeGapInJvmZone = LocalDate.EPOCH.atStartOfDay();
    private final LocalDateTime timeDoubledInJvmZone = LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_789_000);

    // using two non-JVM zones so that we don't need to worry what the backend's system zone is

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    private final LocalDateTime timeGapInVilnius = LocalDateTime.of(2018, 3, 25, 3, 17, 17);
    private final LocalDateTime timeDoubledInVilnius = LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_333_000);

    // Size of offset changed since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");
    private final LocalDateTime timeGapInKathmandu = LocalDateTime.of(1986, 1, 1, 0, 13, 7);

    private final LocalDate dayOfMidnightGapInJvmZone = LocalDate.EPOCH;
    private final LocalDate dayOfMidnightGapInVilnius = LocalDate.of(1983, 4, 1);
    private final LocalDate dayAfterMidnightSetBackInVilnius = LocalDate.of(1983, 10, 1);

    @BeforeClass
    public void checkRanges()
    {
        // Timestamps
        checkIsGap(jvmZone, timeGapInJvmZone);
        checkIsDoubled(jvmZone, timeDoubledInJvmZone);
        checkIsGap(vilnius, timeGapInVilnius);
        checkIsDoubled(vilnius, timeDoubledInVilnius);
        checkIsGap(kathmandu, timeGapInKathmandu);

        // Times
        checkIsGap(jvmZone, LocalTime.of(0, 0, 0).atDate(LocalDate.EPOCH));

        // Dates
        checkIsGap(jvmZone, dayOfMidnightGapInJvmZone.atStartOfDay());
        checkIsGap(vilnius, dayOfMidnightGapInVilnius.atStartOfDay());
        checkIsDoubled(vilnius, dayAfterMidnightSetBackInVilnius.atStartOfDay().minusNanos(1));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRedshiftQueryRunner(Map.of(), Map.of(), List.of());
    }

    @Test
    public void testBasicTypes()
    {
        // Assume that if these types work at all, they have standard semantics.
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("double", "123.45", DOUBLE, "DOUBLE '123.45'")
                .addRoundTrip("real", "123.45", REAL, "REAL '123.45'")
                // If we map tinyint to smallint:
                .addRoundTrip("tinyint", "5", SMALLINT, "SMALLINT '5'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(65535)", "'varchar max'", createVarcharType(65535), "CAST('varchar max' AS varchar(65535))")
                .addRoundTrip("varchar(40)", "'攻殻機動隊'", createVarcharType(40), "CAST('攻殻機動隊' AS varchar(40))")
                .addRoundTrip("varchar(8)", "'隊'", createVarcharType(8), "CAST('隊' AS varchar(8))")
                .addRoundTrip("varchar(16)", "'😂'", createVarcharType(16), "CAST('😂' AS varchar(16))")
                .addRoundTrip("varchar(88)", "'Ну, погоди!'", createVarcharType(88), "CAST('Ну, погоди!' AS varchar(88))")
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(4096)", "'char max'", createVarcharType(4096), "CAST('char max' AS varchar(4096))")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_varchar"))
                .execute(getQueryRunner(), redshiftCreateAndInsert("jdbc_test_varchar"));
    }

    @Test
    public void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("char(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .addRoundTrip("char(4096)", "'char max'", createCharType(4096), "CAST('char max' AS char(4096))")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_char"))
                .execute(getQueryRunner(), redshiftCreateAndInsert("jdbc_test_char"));

        // Test with types larger than Redshift's char(max)
        SqlDataTypeTest.create()
                .addRoundTrip("char(65535)", "'varchar max'", createVarcharType(65535), format("CAST('varchar max%s' AS varchar(65535))", " ".repeat(65535 - "varchar max".length())))
                .addRoundTrip("char(4136)", "'攻殻機動隊'", createVarcharType(4136), format("CAST('%s' AS varchar(4136))", padVarchar(4136).apply("攻殻機動隊")))
                .addRoundTrip("char(4104)", "'隊'", createVarcharType(4104), format("CAST('%s' AS varchar(4104))", padVarchar(4104).apply("隊")))
                .addRoundTrip("char(4112)", "'😂'", createVarcharType(4112), format("CAST('%s' AS varchar(4112))", padVarchar(4112).apply("😂")))
                .addRoundTrip("varchar(88)", "'Ну, погоди!'", createVarcharType(88), "CAST('Ну, погоди!' AS varchar(88))")
                .addRoundTrip("char(4106)", "'text_a'", createVarcharType(4106), format("CAST('%s' AS varchar(4106))", padVarchar(4106).apply("text_a")))
                .addRoundTrip("char(4351)", "'text_b'", createVarcharType(4351), format("CAST('%s' AS varchar(4351))", padVarchar(4351).apply("text_b")))
                .addRoundTrip("char(8192)", "'char max'", createVarcharType(8192), format("CAST('%s' AS varchar(8192))", padVarchar(8192).apply("char max")))
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_large_char"));
    }

    /**
     * Test handling of data outside Redshift's normal bounds.
     *
     * <p>Redshift sometimes returns unbounded {@code VARCHAR} data, apparently
     * when it returns directly from a Postgres function.
     */
    @Test
    public void testPostgresText()
    {
        try (TestView view1 = new TestView("postgres_text_view", "SELECT lpad('x', 1)");
                TestView view2 = new TestView("pg_catalog_view", "SELECT relname FROM pg_class")) {
            // Test data and type from a function
            assertThat(query(format("SELECT * FROM %s", view1.name)))
                    .matches("VALUES CAST('x' AS varchar)");

            // Test the type of an internal table
            assertThat(query(format("SELECT * FROM %s LIMIT 1", view2.name)))
                    .hasOutputTypes(List.of(createUnboundedVarcharType()));
        }
    }

    // Make sure that Redshift still maps NCHAR and NVARCHAR to CHAR and VARCHAR.
    @Test
    public void checkNCharAndNVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("nvarchar(65535)", "'varchar max'", createVarcharType(65535), "CAST('varchar max' AS varchar(65535))")
                .addRoundTrip("nvarchar(40)", "'攻殻機動隊'", createVarcharType(40), "CAST('攻殻機動隊' AS varchar(40))")
                .addRoundTrip("nvarchar(8)", "'隊'", createVarcharType(8), "CAST('隊' AS varchar(8))")
                .addRoundTrip("nvarchar(16)", "'😂'", createVarcharType(16), "CAST('😂' AS varchar(16))")
                .addRoundTrip("nvarchar(88)", "'Ну, погоди!'", createVarcharType(88), "CAST('Ну, погоди!' AS varchar(88))")
                .addRoundTrip("nvarchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("nvarchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("nvarchar(4096)", "'char max'", createVarcharType(4096), "CAST('char max' AS varchar(4096))")
                .execute(getQueryRunner(), redshiftCreateAndInsert("jdbc_test_nvarchar"));

        SqlDataTypeTest.create()
                .addRoundTrip("nchar(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("nchar(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .addRoundTrip("nchar(4096)", "'char max'", createCharType(4096), "CAST('char max' AS char(4096))")
                .execute(getQueryRunner(), redshiftCreateAndInsert("jdbc_test_nchar"));
    }

    @Test
    public void testUnicodeChar() // Redshift doesn't allow multibyte chars in CHAR
    {
        try (TestTable table = testTable("test_multibyte_char", "(c char(32))")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES ('\u968A')", table.getName()),
                    "^Value for Redshift CHAR must be ASCII, but found '\u968A'$");
        }

        assertCreateFails(
                "test_multibyte_char_ctas",
                "AS SELECT CAST('\u968A' AS char(32)) c",
                "^Value for Redshift CHAR must be ASCII, but found '\u968A'$");
    }

    // Make sure Redshift really doesn't allow multibyte characters in CHAR
    @Test
    public void checkUnicodeCharInRedshift()
    {
        try (TestTable table = testTable("check_multibyte_char", "(c char(32))")) {
            assertThatThrownBy(() -> getRedshiftExecutor()
                    .execute(format("INSERT INTO %s VALUES ('\u968a')", table.getName())))
                    .cause()
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("CHAR string contains invalid ASCII character");
        }
    }

    @Test
    public void testOversizedCharacterTypes()
    {
        // Test that character types too large for Redshift map to the maximum size
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'unbounded'", createVarcharType(65535), "CAST('unbounded' AS varchar(65535))")
                .addRoundTrip(format("varchar(%s)", REDSHIFT_MAX_VARCHAR + 1), "'oversized varchar'", createVarcharType(65535), "CAST('oversized varchar' AS varchar(65535))")
                .addRoundTrip(format("char(%s)", REDSHIFT_MAX_VARCHAR + 1), "'oversized char'", createVarcharType(65535), format("CAST('%s' AS varchar(65535))", padVarchar(65535).apply("oversized char")))
                .execute(getQueryRunner(), trinoCreateAsSelect("oversized_character_types"));
    }

    @Test
    public void testVarbinary()
    {
        // Redshift's VARBYTE is mapped to Trino VARBINARY. Redshift does not have VARBINARY type.
        SqlDataTypeTest.create()
                // varbyte
                .addRoundTrip("varbyte", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbyte", "to_varbyte('', 'hex')", VARBINARY, "X''")
                .addRoundTrip("varbyte", utf8VarbyteLiteral("hello"), VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbyte", utf8VarbyteLiteral("Piękna łąka w 東京都"), VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbyte", utf8VarbyteLiteral("Bag full of 💰"), VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbyte", "to_varbyte('0001020304050607080DF9367AA7000000', 'hex')", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbyte", "to_varbyte('000000000000', 'hex')", VARBINARY, "X'000000000000'")
                .addRoundTrip("varbyte(1)", "to_varbyte('00', 'hex')", VARBINARY, "X'00'") // minimum length
                .addRoundTrip("varbyte(1024000)", "to_varbyte('00', 'hex')", VARBINARY, "X'00'") // maximum length
                // varbinary
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", utf8VarbyteLiteral("Bag full of 💰"), VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "to_varbyte('0001020304050607080DF9367AA7000000', 'hex')", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary(1)", "to_varbyte('00', 'hex')", VARBINARY, "X'00'") // minimum length
                .addRoundTrip("varbinary(1024000)", "to_varbyte('00', 'hex')", VARBINARY, "X'00'") // maximum length
                // binary varying
                .addRoundTrip("binary varying", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("binary varying", utf8VarbyteLiteral("Bag full of 💰"), VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("binary varying", "to_varbyte('0001020304050607080DF9367AA7000000', 'hex')", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("binary varying(1)", "to_varbyte('00', 'hex')", VARBINARY, "X'00'") // minimum length
                .addRoundTrip("binary varying(1024000)", "to_varbyte('00', 'hex')", VARBINARY, "X'00'") // maximum length
                .execute(getQueryRunner(), redshiftCreateAndInsert("test_varbinary"));

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

    private static String utf8VarbyteLiteral(String string)
    {
        return format("to_varbyte('%s', 'hex')", base16().encode(string.getBytes(UTF_8)));
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
                .addRoundTrip("decimal(31, 0)", "CAST('2718281828459045235360287471352' AS decimal(31, 0))", createDecimalType(31, 0), "CAST('2718281828459045235360287471352' AS decimal(31, 0))")
                .addRoundTrip("decimal(31, 0)", "CAST('-2718281828459045235360287471352' AS decimal(31, 0))", createDecimalType(31, 0), "CAST('-2718281828459045235360287471352' AS decimal(31, 0))")
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(31, 0)", "NULL", createDecimalType(31, 0), "CAST(NULL AS decimal(31, 0))")
                .execute(getQueryRunner(), redshiftCreateAndInsert("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
    }

    @Test
    public void testRedshiftDecimalCutoff()
    {
        String columns = "(d19 decimal(19, 0), d18 decimal(19, 18), d0 decimal(19, 19))";
        try (TestTable table = testTable("test_decimal_range", columns)) {
            assertQueryFails(
                    format("INSERT INTO %s (d19) VALUES (DECIMAL'9991999999999999999')", table.getName()),
                    "^Value out of range for Redshift DECIMAL\\(19, 0\\)$");
            assertQueryFails(
                    format("INSERT INTO %s (d18) VALUES (DECIMAL'9.991999999999999999')", table.getName()),
                    "^Value out of range for Redshift DECIMAL\\(19, 18\\)$");
            assertQueryFails(
                    format("INSERT INTO %s (d0) VALUES (DECIMAL'.9991999999999999999')", table.getName()),
                    "^Value out of range for Redshift DECIMAL\\(19, 19\\)$");
        }
    }

    @Test
    public void testRedshiftDecimalScaleLimit()
    {
        assertCreateFails(
                "test_overlarge_decimal_scale",
                "(d DECIMAL(38, 38))",
                "^ERROR: DECIMAL scale 38 must be between 0 and 37$");
    }

    @Test
    public void testUnsupportedTrinoDataTypes()
    {
        assertCreateFails(
                "test_unsupported_type",
                "(col json)",
                "Unsupported column type: json");
    }

    @Test(dataProvider = "datetime_test_parameters")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();
        SqlDataTypeTest.create()
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'") // first day of AD
                .addRoundTrip("date", "DATE '1500-01-01'", DATE, "DATE '1500-01-01'") // sometime before julian->gregorian switch
                .addRoundTrip("date", "DATE '1600-01-01'", DATE, "DATE '1600-01-01'") // long ago but after julian->gregorian switch
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'") // after epoch
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer in northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter in northern hemisphere (possible DST in southern hemisphere)
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'") // day of midnight gap in JVM
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'") // day of midnight gap in Vilnius
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'") // day after midnight setback in Vilnius
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"))
                .execute(getQueryRunner(), session, redshiftCreateAndInsert("test_date"));

        // some time BC
        SqlDataTypeTest.create()
                .addRoundTrip("date", "DATE '-0100-01-01'", DATE, "DATE '-0100-01-01'")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));
        SqlDataTypeTest.create()
                .addRoundTrip("date", "DATE '0101-01-01 BC'", DATE, "DATE '-0100-01-01'")
                .execute(getQueryRunner(), session, redshiftCreateAndInsert("test_date"));
    }

    @Test(dataProvider = "datetime_test_parameters")
    public void testTime(ZoneId sessionZone)
    {
        // Redshift gets bizarre errors if you try to insert after
        // specifying precision for a time column.
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();
        timeTypeTests("time(6)")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "time_from_trino"));
        timeTypeTests("time")
                .execute(getQueryRunner(), session, redshiftCreateAndInsert("time_from_jdbc"));
    }

    private static SqlDataTypeTest timeTypeTests(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "TIME '00:00:00.000000'", createTimeType(6), "TIME '00:00:00.000000'") // gap in JVM zone on Epoch day
                .addRoundTrip(inputType, "TIME '00:13:42.000000'", createTimeType(6), "TIME '00:13:42.000000'") // gap in JVM zone on Epoch day
                .addRoundTrip(inputType, "TIME '01:33:17.000000'", createTimeType(6), "TIME '01:33:17.000000'")
                .addRoundTrip(inputType, "TIME '03:17:17.000000'", createTimeType(6), "TIME '03:17:17.000000'")
                .addRoundTrip(inputType, "TIME '10:01:17.100000'", createTimeType(6), "TIME '10:01:17.100000'")
                .addRoundTrip(inputType, "TIME '13:18:03.000000'", createTimeType(6), "TIME '13:18:03.000000'")
                .addRoundTrip(inputType, "TIME '14:18:03.000000'", createTimeType(6), "TIME '14:18:03.000000'")
                .addRoundTrip(inputType, "TIME '15:18:03.000000'", createTimeType(6), "TIME '15:18:03.000000'")
                .addRoundTrip(inputType, "TIME '16:18:03.123456'", createTimeType(6), "TIME '16:18:03.123456'")
                .addRoundTrip(inputType, "TIME '19:01:17.000000'", createTimeType(6), "TIME '19:01:17.000000'")
                .addRoundTrip(inputType, "TIME '20:01:17.000000'", createTimeType(6), "TIME '20:01:17.000000'")
                .addRoundTrip(inputType, "TIME '21:01:17.000001'", createTimeType(6), "TIME '21:01:17.000001'")
                .addRoundTrip(inputType, "TIME '22:59:59.000000'", createTimeType(6), "TIME '22:59:59.000000'")
                .addRoundTrip(inputType, "TIME '23:59:59.000000'", createTimeType(6), "TIME '23:59:59.000000'")
                .addRoundTrip(inputType, "TIME '23:59:59.999999'", createTimeType(6), "TIME '23:59:59.999999'");
    }

    @Test(dataProvider = "datetime_test_parameters")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();
        // Redshift doesn't allow timestamp precision to be specified
        timestampTypeTests("timestamp(6)")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "timestamp_from_trino"));
        timestampTypeTests("timestamp")
                .execute(getQueryRunner(), session, redshiftCreateAndInsert("timestamp_from_jdbc"));

        // some time BC
        SqlDataTypeTest.create()
                .addRoundTrip("timestamp(6)", "TIMESTAMP '-0100-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '-0100-01-01 00:00:00.000000'")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));
        SqlDataTypeTest.create()
                .addRoundTrip("timestamp", "TIMESTAMP '0101-01-01 00:00:00 BC'", createTimestampType(6), "TIMESTAMP '-0100-01-01 00:00:00.000000'")
                .execute(getQueryRunner(), session, redshiftCreateAndInsert("test_date"));
    }

    private static SqlDataTypeTest timestampTypeTests(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "TIMESTAMP '0001-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '0001-01-01 00:00:00.000000'") // first day of AD
                .addRoundTrip(inputType, "TIMESTAMP '1500-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1500-01-01 00:00:00.000000'") // sometime before julian->gregorian switch
                .addRoundTrip(inputType, "TIMESTAMP '1600-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1600-01-01 00:00:00.000000'") // long ago but after julian->gregorian switch
                .addRoundTrip(inputType, "TIMESTAMP '1958-01-01 13:18:03.123456'", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123456'") // before epoch
                .addRoundTrip(inputType, "TIMESTAMP '2019-03-18 10:09:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:09:17.987654'") // after epoch
                .addRoundTrip(inputType, "TIMESTAMP '2018-10-28 01:33:17.456789'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456789'") // time doubled in JVM
                .addRoundTrip(inputType, "TIMESTAMP '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'") // time doubled in Vilnius
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'") // time gap in JVM
                .addRoundTrip(inputType, "TIMESTAMP '2018-03-25 03:17:17.000000'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'") // time gap in Vilnius
                .addRoundTrip(inputType, "TIMESTAMP '1986-01-01 00:13:07.000000'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'") // time gap in Kathmandu
                // Full time precision
                .addRoundTrip(inputType, "TIMESTAMP '1969-12-31 23:59:59.999999'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999999'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.999999'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.999999'");
    }

    @Test(dataProvider = "datetime_test_parameters")
    public void testTimestampWithTimeZone(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '2022-09-27 12:34:56 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2022-09-27 12:34:56.000000 UTC'")
                .addRoundTrip("timestamp(1) with time zone", "TIMESTAMP '2022-09-27 12:34:56.1 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2022-09-27 12:34:56.100000 UTC'")
                .addRoundTrip("timestamp(2) with time zone", "TIMESTAMP '2022-09-27 12:34:56.12 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2022-09-27 12:34:56.120000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2022-09-27 12:34:56.123 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2022-09-27 12:34:56.123000 UTC'")
                .addRoundTrip("timestamp(4) with time zone", "TIMESTAMP '2022-09-27 12:34:56.1234 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2022-09-27 12:34:56.123400 UTC'")
                .addRoundTrip("timestamp(5) with time zone", "TIMESTAMP '2022-09-27 12:34:56.12345 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2022-09-27 12:34:56.123450 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2022-09-27 12:34:56.123456 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2022-09-27 12:34:56.123456 UTC'")

                // short timestamp with time zone
                // .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '-4712-01-01 00:00:00 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '-4712-01-01 00:00:00.000000 UTC'") // min value in Redshift
                // .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '0001-01-01 00:00:00 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '0001-01-01 00:00:00.000000 UTC'") // first day of AD
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1582-10-04 23:59:59.999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-04 23:59:59.999000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1582-10-05 00:00:00.000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-05 00:00:00.000000 UTC'") // begin julian->gregorian switch
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1582-10-14 23:59:59.999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-14 23:59:59.999000 UTC'") // end julian->gregorian switch
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1582-10-15 00:00:00.000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-15 00:00:00.000000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.1 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.100000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.9 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.900000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.123 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.999000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.123 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1986-01-01 00:13:07 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1986-01-01 00:13:07.000000 UTC'") // time gap in Kathmandu
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-10-28 01:33:17.456 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-10-28 01:33:17.456000 UTC'") // time doubled in JVM
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-10-28 03:33:33.333 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-10-28 03:33:33.333000 UTC'") // time doubled in Vilnius
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-03-25 03:17:17.000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-03-25 03:17:17.000000 UTC'") // time gap in Vilnius
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2020-09-27 12:34:56.1 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.100000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2020-09-27 12:34:56.9 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.900000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2020-09-27 12:34:56.123 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2020-09-27 12:34:56.999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.999000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2020-09-27 12:34:56.123 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '73326-09-11 20:14:45.247 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '73326-09-11 20:14:45.247000 UTC'") // max value in Trino
                .addRoundTrip("timestamp(3) with time zone", "NULL", TIMESTAMP_TZ_MICROS, "CAST(NULL AS timestamp(6) with time zone)")

                // long timestamp with time zone
                // .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '0001-01-01 00:00:00 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '0001-01-01 00:00:00.000000 UTC'") // first day of AD
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1582-10-04 23:59:59.999999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-04 23:59:59.999999 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1582-10-05 00:00:00.000000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-05 00:00:00.000000 UTC'") // begin julian->gregorian switch
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1582-10-14 23:59:59.999999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-14 23:59:59.999999 UTC'") // end julian->gregorian switch
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1582-10-15 00:00:00.000000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-15 00:00:00.000000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.1 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.100000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.9 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.900000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.123 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.999000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1986-01-01 00:13:07.000000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1986-01-01 00:13:07.000000 UTC'") // time gap in Kathmandu (long timestamp_tz)
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2018-10-28 01:33:17.456789 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-10-28 01:33:17.456789 UTC'") // time doubled in JVM
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2018-10-28 03:33:33.333333 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-10-28 03:33:33.333333 UTC'") // time doubled in Vilnius
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2018-03-25 03:17:17.000000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-03-25 03:17:17.000000 UTC'") // time gap in Vilnius
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2020-09-27 12:34:56.1 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.100000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2020-09-27 12:34:56.9 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.900000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2020-09-27 12:34:56.123 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2020-09-27 12:34:56.999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.999000 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '2020-09-27 12:34:56.123456 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.123456 UTC'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '73326-09-11 20:14:45.247999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '73326-09-11 20:14:45.247999 UTC'") // max value in Trino
                .addRoundTrip("timestamp(6) with time zone", "NULL", TIMESTAMP_TZ_MICROS, "CAST(NULL AS timestamp(6) with time zone)")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(getSession(), "test_timestamp_tz"));

        redshiftTimestampWithTimeZoneTests("timestamptz")
                .execute(getQueryRunner(), session, redshiftCreateAndInsert("test_timestamp_tz"));
        redshiftTimestampWithTimeZoneTests("timestamp with time zone")
                .execute(getQueryRunner(), session, redshiftCreateAndInsert("test_timestamp_tz"));
    }

    private static SqlDataTypeTest redshiftTimestampWithTimeZoneTests(String inputType)
    {
        return SqlDataTypeTest.create()
                // .addRoundTrip(inputType, "TIMESTAMP '4713-01-01 00:00:00 BC' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '-4712-01-01 00:00:00.000000 UTC'") // min value in Redshift
                // .addRoundTrip(inputType, "TIMESTAMP '0001-01-01 00:00:00' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '0001-01-01 00:00:00.000000 UTC'") // first day of AD
                .addRoundTrip(inputType, "TIMESTAMP '1582-10-04 23:59:59.999999' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-04 23:59:59.999999 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1582-10-05 00:00:00.000000' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-05 00:00:00.000000 UTC'") // begin julian->gregorian switch
                .addRoundTrip(inputType, "TIMESTAMP '1582-10-14 23:59:59.999999' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-14 23:59:59.999999 UTC'") // end julian->gregorian switch
                .addRoundTrip(inputType, "TIMESTAMP '1582-10-15 00:00:00.000000' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1582-10-15 00:00:00.000000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.1' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.100000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.9' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.900000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.123' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.123000' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.999' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.999000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1970-01-01 00:00:00.123456' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '1986-01-01 00:13:07.000000' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1986-01-01 00:13:07.000000 UTC'") // time gap in Kathmandu
                .addRoundTrip(inputType, "TIMESTAMP '2018-10-28 01:33:17.456789' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-10-28 01:33:17.456789 UTC'") // time doubled in JVM
                .addRoundTrip(inputType, "TIMESTAMP '2018-10-28 03:33:33.333333' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-10-28 03:33:33.333333 UTC'") // time doubled in Vilnius
                .addRoundTrip(inputType, "TIMESTAMP '2018-03-25 03:17:17.000000' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2018-03-25 03:17:17.000000 UTC'") // time gap in Vilnius
                .addRoundTrip(inputType, "TIMESTAMP '2020-09-27 12:34:56.1' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.100000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '2020-09-27 12:34:56.9' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.900000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '2020-09-27 12:34:56.123' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '2020-09-27 12:34:56.123000' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '2020-09-27 12:34:56.999' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.999000 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '2020-09-27 12:34:56.123456' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '2020-09-27 12:34:56.123456 UTC'")
                .addRoundTrip(inputType, "TIMESTAMP '73326-09-11 20:14:45.247999' AT TIME ZONE 'UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '73326-09-11 20:14:45.247999 UTC'"); // max value in Trino
    }

    @Test
    public void testTimestampWithTimeZoneCoercion()
    {
        SqlDataTypeTest.create()
                // short timestamp with time zone
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.12341 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'") // round down
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.123499 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'") // round up, end result rounds down
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.1235 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.124000 UTC'") // round up
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.111222333444 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.111000 UTC'") // max precision
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.9999995 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:01.000000 UTC'") // round up to next second
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 23:59:59.9999995 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-02 00:00:00.000000 UTC'") // round up to next day
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1969-12-31 23:59:59.9999995 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'") // negative epoch
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1969-12-31 23:59:59.999499999999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1969-12-31 23:59:59.999000 UTC'") // negative epoch
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1969-12-31 23:59:59.9994 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1969-12-31 23:59:59.999000 UTC'") // negative epoch

                // long timestamp with time zone
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.1234561 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'") // round down
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.123456499 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'") // nanoc round up, end result rounds down
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.1234565 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.123457 UTC'") // round up
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.111222333444 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.111222 UTC'") // max precision
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.9999995 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:01.000000 UTC'") // round up to next second
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 23:59:59.9999995 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-02 00:00:00.000000 UTC'") // round up to next day
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1969-12-31 23:59:59.9999995 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'") // negative epoch
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1969-12-31 23:59:59.999999499999 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1969-12-31 23:59:59.999999 UTC'") // negative epoch
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1969-12-31 23:59:59.9999994 UTC'", TIMESTAMP_TZ_MICROS, "TIMESTAMP '1969-12-31 23:59:59.999999 UTC'") // negative epoch
                .execute(getQueryRunner(), trinoCreateAsSelect(getSession(), "test_timestamp_tz"));
    }

    @Test
    public void testTimestampWithTimeZoneOverflow()
    {
        // The min timestamp with time zone value in Trino is smaller than Redshift
        try (TestTable table = new TestTable(getTrinoExecutor(), "timestamp_tz_min", "(ts timestamp(3) with time zone)")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (TIMESTAMP '-69387-04-22 03:45:14.752 UTC')", table.getName()),
                    "\\QMinimum timestamp with time zone in Redshift is -4712-01-01 00:00:00.000000: -69387-04-22 03:45:14.752000");
        }
        try (TestTable table = new TestTable(getTrinoExecutor(), "timestamp_tz_min", "(ts timestamp(6) with time zone)")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (TIMESTAMP '-69387-04-22 03:45:14.752000 UTC')", table.getName()),
                    "\\QMinimum timestamp with time zone in Redshift is -4712-01-01 00:00:00.000000: -69387-04-22 03:45:14.752000");
        }

        // The max timestamp with time zone value in Redshift is larger than Trino
        try (TestTable table = new TestTable(getRedshiftExecutor(), TEST_SCHEMA + ".timestamp_tz_max", "(ts timestamptz)", ImmutableList.of("TIMESTAMP '294276-12-31 23:59:59' AT TIME ZONE 'UTC'"))) {
            assertThatThrownBy(() -> query("SELECT * FROM " + table.getName()))
                    .hasMessage("Millis overflow: 9224318015999000");
        }
    }

    @DataProvider(name = "datetime_test_parameters")
    public Object[][] dataProviderForDatetimeTests()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                {vilnius},
                {kathmandu},
                {testZone},
        };
    }

    @Test
    public void testUnsupportedDateTimeTypes()
    {
        assertCreateFails(
                "test_time_with_time_zone",
                "(value TIME WITH TIME ZONE)",
                "Unsupported column type: (?i)time.* with time zone");
    }

    @Test
    public void testDateLimits()
    {
        // We can't test the exact date limits because Redshift doesn't say
        // what they are, so we test one date on either side.
        try (TestTable table = testTable("test_date_limits", "(d date)")) {
            // First day of smallest year that Redshift supports (based on its documentation)
            assertUpdate(format("INSERT INTO %s VALUES (DATE '-4712-01-01')", table.getName()), 1);
            // Small date observed to not work
            assertThatThrownBy(() -> computeActual(format("INSERT INTO %s VALUES (DATE '-4713-06-01')", table.getName())))
                    .hasStackTraceContaining("ERROR: date out of range: \"4714-06-01 BC\"");

            // Last day of the largest year that Redshift supports (based on in its documentation)
            assertUpdate(format("INSERT INTO %s VALUES (DATE '294275-12-31')", table.getName()), 1);
            // Large date observed to not work
            assertThatThrownBy(() -> computeActual(format("INSERT INTO %s VALUES (DATE '5875000-01-01')", table.getName())))
                    .hasStackTraceContaining("ERROR: date out of range: \"5875000-01-01 AD\"");
        }
    }

    @Test
    public void testLimitedTimePrecision()
    {
        Map<Integer, List<TestCase>> testCasesByPrecision = groupTestCasesByInput(
                "TIME '\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,12})?'",
                input -> input.length() - "TIME '00:00:00'".length() - (input.contains(".") ? 1 : 0),
                List.of(
                        // No rounding
                        new TestCase("TIME '00:00:00'", "TIME '00:00:00'"),
                        new TestCase("TIME '00:00:00.000000'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '00:00:00.123456'", "TIME '00:00:00.123456'"),
                        new TestCase("TIME '12:34:56'", "TIME '12:34:56'"),
                        new TestCase("TIME '12:34:56.123456'", "TIME '12:34:56.123456'"),
                        new TestCase("TIME '23:59:59'", "TIME '23:59:59'"),
                        new TestCase("TIME '23:59:59.9'", "TIME '23:59:59.9'"),
                        new TestCase("TIME '23:59:59.999'", "TIME '23:59:59.999'"),
                        new TestCase("TIME '23:59:59.999999'", "TIME '23:59:59.999999'"),
                        // round down
                        new TestCase("TIME '00:00:00.0000001'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '00:00:00.000000000001'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '12:34:56.1234561'", "TIME '12:34:56.123456'"),
                        // round down, maximal value
                        new TestCase("TIME '00:00:00.0000004'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '00:00:00.000000449'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '00:00:00.000000444449'", "TIME '00:00:00.000000'"),
                        // round up, minimal value
                        new TestCase("TIME '00:00:00.0000005'", "TIME '00:00:00.000001'"),
                        new TestCase("TIME '00:00:00.000000500'", "TIME '00:00:00.000001'"),
                        new TestCase("TIME '00:00:00.000000500000'", "TIME '00:00:00.000001'"),
                        // round up, maximal value
                        new TestCase("TIME '00:00:00.0000009'", "TIME '00:00:00.000001'"),
                        new TestCase("TIME '00:00:00.000000999'", "TIME '00:00:00.000001'"),
                        new TestCase("TIME '00:00:00.000000999999'", "TIME '00:00:00.000001'"),
                        // round up to next day, minimal value
                        new TestCase("TIME '23:59:59.9999995'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '23:59:59.999999500'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '23:59:59.999999500000'", "TIME '00:00:00.000000'"),
                        // round up to next day, maximal value
                        new TestCase("TIME '23:59:59.9999999'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '23:59:59.999999999'", "TIME '00:00:00.000000'"),
                        new TestCase("TIME '23:59:59.999999999999'", "TIME '00:00:00.000000'"),
                        // don't round to next day (round down near upper bound)
                        new TestCase("TIME '23:59:59.9999994'", "TIME '23:59:59.999999'"),
                        new TestCase("TIME '23:59:59.999999499'", "TIME '23:59:59.999999'"),
                        new TestCase("TIME '23:59:59.999999499999'", "TIME '23:59:59.999999'")));

        for (Entry<Integer, List<TestCase>> entry : testCasesByPrecision.entrySet()) {
            String tableName = format("test_time_precision_%d_%s", entry.getKey(), randomNameSuffix());
            runTestCases(tableName, entry.getValue());
        }
    }

    @Test
    public void testLimitedTimestampPrecision()
    {
        Map<Integer, List<TestCase>> testCasesByPrecision = groupTestCasesByInput(
                "TIMESTAMP '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d{1,12})?'",
                input -> input.length() - "TIMESTAMP '0000-00-00 00:00:00'".length() - (input.contains(".") ? 1 : 0),
                // No rounding
                new TestCase("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00'"),
                new TestCase("TIMESTAMP '2020-11-03 12:34:56'", "TIMESTAMP '2020-11-03 12:34:56'"),
                new TestCase("TIMESTAMP '1969-12-31 00:00:00.000000'", "TIMESTAMP '1969-12-31 00:00:00.000000'"),

                new TestCase("TIMESTAMP '1970-01-01 00:00:00.123456'", "TIMESTAMP '1970-01-01 00:00:00.123456'"),
                new TestCase("TIMESTAMP '2020-11-03 12:34:56.123456'", "TIMESTAMP '2020-11-03 12:34:56.123456'"),
                new TestCase("TIMESTAMP '1969-12-31 23:59:59'", "TIMESTAMP '1969-12-31 23:59:59'"),

                new TestCase("TIMESTAMP '1970-01-01 23:59:59.9'", "TIMESTAMP '1970-01-01 23:59:59.9'"),
                new TestCase("TIMESTAMP '2020-11-03 23:59:59.999'", "TIMESTAMP '2020-11-03 23:59:59.999'"),
                new TestCase("TIMESTAMP '1969-12-31 23:59:59.999999'", "TIMESTAMP '1969-12-31 23:59:59.999999'"),
                // round down
                new TestCase("TIMESTAMP '1969-12-31 00:00:00.0000001'", "TIMESTAMP '1969-12-31 00:00:00.000000'"),
                new TestCase("TIMESTAMP '1970-01-01 00:00:00.000000000001'", "TIMESTAMP '1970-01-01 00:00:00.000000'"),
                new TestCase("TIMESTAMP '2020-11-03 12:34:56.1234561'", "TIMESTAMP '2020-11-03 12:34:56.123456'"),
                // round down, maximal value
                new TestCase("TIMESTAMP '2020-11-03 00:00:00.0000004'", "TIMESTAMP '2020-11-03 00:00:00.000000'"),
                new TestCase("TIMESTAMP '1969-12-31 00:00:00.000000449'", "TIMESTAMP '1969-12-31 00:00:00.000000'"),
                new TestCase("TIMESTAMP '1970-01-01 00:00:00.000000444449'", "TIMESTAMP '1970-01-01 00:00:00.000000'"),
                // round up, minimal value
                new TestCase("TIMESTAMP '1970-01-01 00:00:00.0000005'", "TIMESTAMP '1970-01-01 00:00:00.000001'"),
                new TestCase("TIMESTAMP '2020-11-03 00:00:00.000000500'", "TIMESTAMP '2020-11-03 00:00:00.000001'"),
                new TestCase("TIMESTAMP '1969-12-31 00:00:00.000000500000'", "TIMESTAMP '1969-12-31 00:00:00.000001'"),
                // round up, maximal value
                new TestCase("TIMESTAMP '1969-12-31 00:00:00.0000009'", "TIMESTAMP '1969-12-31 00:00:00.000001'"),
                new TestCase("TIMESTAMP '1970-01-01 00:00:00.000000999'", "TIMESTAMP '1970-01-01 00:00:00.000001'"),
                new TestCase("TIMESTAMP '2020-11-03 00:00:00.000000999999'", "TIMESTAMP '2020-11-03 00:00:00.000001'"),
                // round up to next year, minimal value
                new TestCase("TIMESTAMP '2020-12-31 23:59:59.9999995'", "TIMESTAMP '2021-01-01 00:00:00.000000'"),
                new TestCase("TIMESTAMP '1969-12-31 23:59:59.999999500'", "TIMESTAMP '1970-01-01 00:00:00.000000'"),
                new TestCase("TIMESTAMP '1970-01-01 23:59:59.999999500000'", "TIMESTAMP '1970-01-02 00:00:00.000000'"),
                // round up to next day/year, maximal value
                new TestCase("TIMESTAMP '1970-01-01 23:59:59.9999999'", "TIMESTAMP '1970-01-02 00:00:00.000000'"),
                new TestCase("TIMESTAMP '2020-12-31 23:59:59.999999999'", "TIMESTAMP '2021-01-01 00:00:00.000000'"),
                new TestCase("TIMESTAMP '1969-12-31 23:59:59.999999999999'", "TIMESTAMP '1970-01-01 00:00:00.000000'"),
                // don't round to next year (round down near upper bound)
                new TestCase("TIMESTAMP '1969-12-31 23:59:59.9999994'", "TIMESTAMP '1969-12-31 23:59:59.999999'"),
                new TestCase("TIMESTAMP '1970-01-01 23:59:59.999999499'", "TIMESTAMP '1970-01-01 23:59:59.999999'"),
                new TestCase("TIMESTAMP '2020-12-31 23:59:59.999999499999'", "TIMESTAMP '2020-12-31 23:59:59.999999'"));

        for (Entry<Integer, List<TestCase>> entry : testCasesByPrecision.entrySet()) {
            String tableName = format("test_timestamp_precision_%d_%s", entry.getKey(), randomNameSuffix());
            runTestCases(tableName, entry.getValue());
        }
    }

    private static Map<Integer, List<TestCase>> groupTestCasesByInput(String inputRegex, Function<String, Integer> classifier, TestCase... testCases)
    {
        return groupTestCasesByInput(inputRegex, classifier, Arrays.asList(testCases));
    }

    private static Map<Integer, List<TestCase>> groupTestCasesByInput(String inputRegex, Function<String, Integer> classifier, List<TestCase> testCases)
    {
        return testCases.stream()
                .peek(test -> {
                    if (!test.input().matches(inputRegex)) {
                        throw new RuntimeException("Bad test case input format: " + test.input());
                    }
                })
                .collect(groupingBy(classifier.compose(TestCase::input)));
    }

    private void runTestCases(String tableName, List<TestCase> testCases)
    {
        // Must use CTAS instead of TestTable because if the table is created before the insert,
        // the type mapping will treat it as TIME(6) no matter what it was created as.
        getTrinoExecutor().execute(format(
                "CREATE TABLE %s AS SELECT * FROM (VALUES %s) AS t (id, value)",
                tableName,
                testCases.stream()
                        .map(testCase -> format("(%d, %s)", testCase.id(), testCase.input()))
                        .collect(joining("), (", "(", ")"))));
        try {
            assertQuery(
                    format("SELECT value FROM %s ORDER BY id", tableName),
                    testCases.stream()
                            .map(TestCase::expected)
                            .collect(joining("), (", "VALUES (", ")")));
        }
        finally {
            getTrinoExecutor().execute("DROP TABLE " + tableName);
        }
    }

    @Test
    public static void checkIllegalRedshiftTimePrecision()
    {
        assertRedshiftCreateFails(
                "check_redshift_time_precision_error",
                "(t TIME(6))",
                "ERROR: time column does not support precision.");
    }

    @Test
    public static void checkIllegalRedshiftTimestampPrecision()
    {
        assertRedshiftCreateFails(
                "check_redshift_timestamp_precision_error",
                "(t TIMESTAMP(6))",
                "ERROR: timestamp column does not support precision.");
    }

    /**
     * Assert that a {@code CREATE TABLE} statement made from Redshift fails,
     * and drop the table if it doesn't fail.
     */
    private static void assertRedshiftCreateFails(String tableNamePrefix, String tableBody, String message)
    {
        String tableName = tableNamePrefix + "_" + randomNameSuffix();
        try {
            assertThatThrownBy(() -> getRedshiftExecutor()
                    .execute(format("CREATE TABLE %s %s", tableName, tableBody)))
                    .cause()
                    .as("Redshift create fails for %s %s", tableName, tableBody)
                    .isInstanceOf(SQLException.class)
                    .hasMessage(message);
        }
        catch (AssertionError failure) {
            // If the table was created, clean it up because the tests run on a shared Redshift instance
            try {
                getRedshiftExecutor().execute("DROP TABLE IF EXISTS " + tableName);
            }
            catch (Throwable e) {
                failure.addSuppressed(e);
            }
            throw failure;
        }
    }

    /**
     * Assert that a {@code CREATE TABLE} statement fails, and drop the table
     * if it doesn't fail.
     */
    private void assertCreateFails(String tableNamePrefix, String tableBody, String expectedMessageRegExp)
    {
        String tableName = tableNamePrefix + "_" + randomNameSuffix();
        try {
            assertQueryFails(format("CREATE TABLE %s %s", tableName, tableBody), expectedMessageRegExp);
        }
        catch (AssertionError failure) {
            // If the table was created, clean it up because the tests run on a shared Redshift instance
            try {
                getRedshiftExecutor().execute("DROP TABLE " + tableName);
            }
            catch (Throwable e) {
                failure.addSuppressed(e);
            }
            throw failure;
        }
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getQueryRunner().getDefaultSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private static DataSetup redshiftCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getRedshiftExecutor(), TEST_SCHEMA + "." + tableNamePrefix);
    }

    /**
     * Create a table in the test schema using the JDBC.
     *
     * <p>Creating a test table normally doesn't use the correct schema.
     */
    private static TestTable testTable(String namePrefix, String body)
    {
        return new TestTable(getRedshiftExecutor(), TEST_SCHEMA + "." + namePrefix, body);
    }

    private SqlExecutor getTrinoExecutor()
    {
        return new TrinoSqlExecutor(getQueryRunner());
    }

    private static SqlExecutor getRedshiftExecutor()
    {
        Properties properties = new Properties();
        properties.setProperty("user", JDBC_USER);
        properties.setProperty("password", JDBC_PASSWORD);
        return new JdbcSqlExecutor(JDBC_URL, properties);
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(
                zone.getRules().getValidOffsets(dateTime).isEmpty(),
                "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(
                zone.getRules().getValidOffsets(dateTime).size() == 2,
                "Expected %s to be doubled in %s", dateTime, zone);
    }

    private static Function<String, String> padVarchar(int length)
    {
        // Add the same padding as RedshiftClient.writeCharAsVarchar, but start from String, not Slice
        return (input) -> input + " ".repeat(length - Utf8.encodedLength(input));
    }

    /**
     * A pair of input and expected output from a test.
     * Each instance has a unique ID.
     */
    private static class TestCase
    {
        private static final AtomicInteger LAST_ID = new AtomicInteger();

        private final int id;
        private final String input;
        private final String expected;

        private TestCase(String input, String expected)
        {
            this.id = LAST_ID.incrementAndGet();
            this.input = input;
            this.expected = expected;
        }

        public int id()
        {
            return this.id;
        }

        public String input()
        {
            return this.input;
        }

        public String expected()
        {
            return this.expected;
        }
    }

    private static class TestView
            implements AutoCloseable
    {
        final String name;

        TestView(String namePrefix, String definition)
        {
            name = requireNonNull(namePrefix) + "_" + randomNameSuffix();
            executeInRedshift(format("CREATE VIEW %s.%s AS %s", TEST_SCHEMA, name, definition));
        }

        @Override
        public void close()
        {
            executeInRedshift(format("DROP VIEW IF EXISTS %s.%s", TEST_SCHEMA, name));
        }
    }
}
