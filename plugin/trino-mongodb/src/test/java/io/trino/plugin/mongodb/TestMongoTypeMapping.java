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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClients;
import io.trino.Session;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.ZoneId;

import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoQueryRunner;
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
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoTypeMapping
        extends AbstractTestQueryFramework
{
    private MongoServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new MongoServer());
        return createMongoQueryRunner(server, ImmutableMap.of(), ImmutableList.of());
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));

        SqlDataTypeTest.create()
                .addRoundTrip("true", "true")
                .addRoundTrip("false", "false")
                .execute(getQueryRunner(), mongoCreateAndInsert(getSession(), "tpch", "test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'")
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'")
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648")
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_int"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));

        // INTEGER values written by MongoDB are inferred as BIGINT in Trino
        SqlDataTypeTest.create()
                .addRoundTrip("-2147483648", "BIGINT '-2147483648'")
                .addRoundTrip("0", "BIGINT '0'")
                .addRoundTrip("2147483647", "BIGINT '2147483647'")
                .execute(getQueryRunner(), mongoCreateAndInsert(getSession(), "tpch", "test_int"));
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_bigint"));
    }

    @Test
    public void testMongoNumberLong()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("NumberLong(-9223372036854775808)", "BIGINT '-9223372036854775808'")
                .addRoundTrip("NumberLong(9223372036854775807)", "BIGINT '9223372036854775807'")
                .execute(getQueryRunner(), mongoCreateAndInsert(getSession(), "tpch", "test_number_long"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "nan()", DOUBLE, "nan()")
                .addRoundTrip("double", "+infinity()", DOUBLE, "+infinity()")
                .addRoundTrip("double", "-infinity()", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), trinoCreateAsSelect("trino_test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("trino_test_double"));
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
                // TODO: Fix NumberFormatException: Conversion to Decimal128 would require inexact rounding
//                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
//                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST(NULL AS decimal(3, 0))", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));

        SqlDataTypeTest.create()
                .addRoundTrip("NumberDecimal(\"2\")", "CAST('2' AS decimal(1, 0))")
                .addRoundTrip("NumberDecimal(\"2.3\")", "CAST('2.3' AS decimal(2, 1))")
                .addRoundTrip("NumberDecimal(\"-2.3\")", "CAST('-2.3' AS decimal(2, 1))")
                .addRoundTrip("NumberDecimal(\"1234567890123456789012345678901234\")", "CAST('1234567890123456789012345678901234' AS decimal(34, 0))") // 34 is the max precision in Decimal128
                .addRoundTrip("NumberDecimal(\"1234567890123456.789012345678901234\")", "CAST('1234567890123456.789012345678901234' AS decimal(34, 18))")
                .execute(getQueryRunner(), mongoCreateAndInsert(getSession(), "tpch", "test_decimal"));
    }

    @Test
    public void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char", "''", createCharType(1), "CAST('' AS char(1))")
                .addRoundTrip("char", "'a'", createCharType(1), "CAST('a' AS char(1))")
                .addRoundTrip("char(1)", "''", createCharType(1), "CAST('' AS char(1))")
                .addRoundTrip("char(1)", "'a'", createCharType(1), "CAST('a' AS char(1))")
                .addRoundTrip("char(8)", "'abc'", createCharType(8), "CAST('abc' AS char(8))")
                .addRoundTrip("char(8)", "'12345678'", createCharType(8), "CAST('12345678' AS char(8))")
                .addRoundTrip("char(255)", format("'%s'", "a".repeat(255)), createCharType(255), format("CAST('%s' AS char(255))", "a".repeat(255)))
                .addRoundTrip("char(1)", "'攻'", createCharType(1), "CAST('攻' AS char(1))")
                .addRoundTrip("char(5)", "'攻殻'", createCharType(5), "CAST('攻殻' AS char(5))")
                .addRoundTrip("char(5)", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS char(5))")
                .addRoundTrip("char", "NULL", createCharType(1), "CAST(NULL AS char(1))")
                .addRoundTrip("char(255)", "NULL", createCharType(255), "CAST(NULL AS char(255))")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_char"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS VARCHAR(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS VARCHAR(255))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS VARCHAR(65535))")
                .addRoundTrip("varchar(10485760)", "'text_f'", createVarcharType(10485760), "CAST('text_f' AS VARCHAR(10485760))")
                .addRoundTrip("varchar", "'unbounded'", VARCHAR, "VARCHAR 'unbounded'")
                .addRoundTrip("varchar(10)", "NULL", createVarcharType(10), "CAST(NULL AS VARCHAR(10))")
                .addRoundTrip("varchar", "NULL", VARCHAR, "CAST(NULL AS VARCHAR)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"));

        SqlDataTypeTest.create()
                .addRoundTrip("'unbounded'", "VARCHAR 'unbounded'")
                .addRoundTrip("''", "VARCHAR ''")
                .execute(getQueryRunner(), mongoCreateAndInsert(getSession(), "tpch", "test_varchar"));
    }

    @Test
    public void testVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "to_utf8('')")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varbinary"));
    }

    @Test
    public void testTime()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time", "NULL", createTimeType(3), "CAST(NULL AS TIME)")
                .addRoundTrip("time", "TIME '00:00:00'", createTimeType(3), "TIME '00:00:00.000'")
                .addRoundTrip("time", "TIME '23:59:59'", createTimeType(3), "TIME '23:59:59.000'")
                .addRoundTrip("time", "TIME '23:59:59.9'", createTimeType(3), "TIME '23:59:59.900'")
                .addRoundTrip("time", "TIME '23:59:59.99'", createTimeType(3), "TIME '23:59:59.990'")
                .addRoundTrip("time", "TIME '23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_time"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_time"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("date", "DATE '-5877641-06-23'", DATE, "DATE '-5877641-06-23'") // min value in Trino
                .addRoundTrip("date", "DATE '-0001-01-01'", DATE, "DATE '-0001-01-01'")
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "DATE '0012-12-12'", DATE, "DATE '0012-12-12'")
                .addRoundTrip("date", "DATE '1500-01-01'", DATE, "DATE '1500-01-01'")
                .addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'") // before julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-05'") // begin julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-14'") // end julian->gregorian switch
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'")
                .addRoundTrip("date", "DATE '19999-12-31'", DATE, "DATE '19999-12-31'")
                .addRoundTrip("date", "DATE '5881580-07-11'", DATE, "DATE '5881580-07-11'") // max value in Trino
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp '-290307-12-31 23:59:59.999'", "timestamp '-290307-12-31 23:59:59.999'") // min value
                .addRoundTrip("timestamp '1582-10-04 23:59:59.999'", "timestamp '1582-10-04 23:59:59.999'") // before julian->gregorian switch
                .addRoundTrip("timestamp '1582-10-05 00:00:00.000'", "timestamp '1582-10-05 00:00:00.000'") // begin julian->gregorian switch
                .addRoundTrip("timestamp '1582-10-14 23:59:59.999'", "timestamp '1582-10-14 23:59:59.999'") // end julian->gregorian switch
                .addRoundTrip("timestamp '1970-01-01 00:00:00.000'", "timestamp '1970-01-01 00:00:00.000'") // epoch
                .addRoundTrip("timestamp '1986-01-01 00:13:07.123'", "timestamp '1986-01-01 00:13:07.123'") // time gap in Kathmandu
                .addRoundTrip("timestamp '2018-03-25 03:17:17.123'", "timestamp '2018-03-25 03:17:17.123'") // time gap in Vilnius
                .addRoundTrip("timestamp '2018-10-28 01:33:17.456'", "timestamp '2018-10-28 01:33:17.456'") // time doubled in JVM zone
                .addRoundTrip("timestamp '2018-10-28 03:33:33.333'", "timestamp '2018-10-28 03:33:33.333'") // time double in Vilnius
                .addRoundTrip("timestamp '294247-01-10 04:00:54.775'", "timestamp '294247-01-10 04:00:54.775'") // max value

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestampWithTimeZoneMapping(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp '-69387-04-22 03:45:14.752 UTC'", "timestamp '-69387-04-22 03:45:14.752 UTC'") // min value
                .addRoundTrip("timestamp '1582-10-04 23:59:59.999 UTC'", "timestamp '1582-10-04 23:59:59.999 UTC'") // before julian->gregorian switch
                .addRoundTrip("timestamp '1582-10-05 00:00:00.000 UTC'", "timestamp '1582-10-05 00:00:00.000 UTC'") // begin julian->gregorian switch
                .addRoundTrip("timestamp '1582-10-14 23:59:59.999 UTC'", "timestamp '1582-10-14 23:59:59.999 UTC'") // end julian->gregorian switch
                .addRoundTrip("timestamp '1970-01-01 00:00:00.000 UTC'", "timestamp '1970-01-01 00:00:00.000 UTC'") // epoch
                .addRoundTrip("timestamp '1986-01-01 00:13:07.123 UTC'", "timestamp '1986-01-01 00:13:07.123 UTC'") // time gap in Kathmandu
                .addRoundTrip("timestamp '2018-03-25 03:17:17.123 UTC'", "timestamp '2018-03-25 03:17:17.123 UTC'") // time gap in Vilnius
                .addRoundTrip("timestamp '2018-10-28 01:33:17.456 UTC'", "timestamp '2018-10-28 01:33:17.456 UTC'") // time doubled in JVM zone
                .addRoundTrip("timestamp '2018-10-28 03:33:33.333 UTC'", "timestamp '2018-10-28 03:33:33.333 UTC'") // time double in Vilnius
                .addRoundTrip("timestamp '2022-10-01 22:30:00.000 -01:30'", "timestamp '2022-10-02 00:00:00.000 UTC'") // not UTC (-01:30)
                .addRoundTrip("timestamp '2022-10-02 01:30:00.000 +01:30'", "timestamp '2022-10-02 00:00:00.000 UTC'") // not UTC (+01:30)
                .addRoundTrip("timestamp '73326-09-11 20:14:45.247 UTC'", "timestamp '73326-09-11 20:14:45.247 UTC'") // max value

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp_with_time_zone"));
    }

    @Test
    public void testArray()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY(boolean)", "ARRAY[true, false]", new ArrayType(BOOLEAN), "ARRAY[true, false]")
                .addRoundTrip("ARRAY(bigint)", "ARRAY[123456789012]", new ArrayType(BIGINT), "ARRAY[123456789012]")
                .addRoundTrip("ARRAY(integer)", "ARRAY[1, 2, 1234567890]", new ArrayType(INTEGER), "ARRAY[1, 2, 1234567890]")
                .addRoundTrip("ARRAY(smallint)", "ARRAY[32456]", new ArrayType(SMALLINT), "ARRAY[SMALLINT '32456']")
                .addRoundTrip("ARRAY(double)", "ARRAY[123.45]", new ArrayType(DOUBLE), "ARRAY[DOUBLE '123.45']")
                .addRoundTrip("ARRAY(real)", "ARRAY[123.45]", new ArrayType(REAL), "ARRAY[REAL '123.45']")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_array"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_array"));
    }

    @Test
    public void testArrayNulls()
    {
        // Verify only SELECT instead of using SqlDataTypeTest because array comparison not supported for arrays with null elements
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_array_nulls", "(c1 ARRAY(boolean), c2 ARRAY(varchar), c3 ARRAY(varchar))", ImmutableList.of("(NULL, ARRAY[NULL], ARRAY['foo', NULL, 'bar', NULL])"))) {
            assertThat(query("SELECT c1 FROM " + table.getName())).matches("VALUES CAST(NULL AS ARRAY(boolean))");
            assertThat(query("SELECT c2 FROM " + table.getName())).matches("VALUES CAST(ARRAY[NULL] AS ARRAY(varchar))");
            assertThat(query("SELECT c3 FROM " + table.getName())).matches("VALUES CAST(ARRAY['foo', NULL, 'bar', NULL] AS ARRAY(varchar))");
        }
    }

    @Test
    public void testJson()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("json", "json '{\"id\":0,\"name\":\"user_0\"}'", JSON, "json '{\"id\":0,\"name\":\"user_0\"}'")
                .addRoundTrip("json", "json '{}'", JSON, "json '{}'")
                .addRoundTrip("json", "CAST(NULL AS json)", JSON, "CAST(NULL AS json)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_json"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_json"));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {ZoneId.systemDefault()},
                // no DST in 1970, but has DST in later years (e.g. 2018)
                {ZoneId.of("Europe/Vilnius")},
                // minutes offset change since 1970-01-01, no DST
                {ZoneId.of("Asia/Kathmandu")},
                {TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId()},
        };
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup mongoCreateAndInsert(Session session, String databaseName, String tableNamePrefix)
    {
        return new MongoCreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), MongoClients.create(server.getConnectionString()), databaseName, tableNamePrefix);
    }
}
