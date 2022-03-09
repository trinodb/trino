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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.plugin.oracle.OracleDataTypes.oracleTimestamp3TimeZoneDataType;
import static io.trino.plugin.oracle.OracleSessionProperties.NUMBER_DEFAULT_SCALE;
import static io.trino.plugin.oracle.OracleSessionProperties.NUMBER_ROUNDING_MODE;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.datatype.DataType.timestampDataType;
import static io.trino.testing.datatype.DataType.timestampWithTimeZoneDataType;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_EVEN;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;

public abstract class AbstractTestOracleTypeMapping
        extends AbstractTestQueryFramework
{
    protected static final int MAX_CHAR_ON_READ = 2000;
    protected static final int MAX_CHAR_ON_WRITE = 500;

    protected static final int MAX_VARCHAR2_ON_READ = 4000;
    protected static final int MAX_VARCHAR2_ON_WRITE = 1000;

    protected static final int MAX_NCHAR = 1000;
    protected static final int MAX_NVARCHAR2 = 2000;

    private static final String NO_SUPPORTED_COLUMNS = "Table '.*' has no supported columns \\(all \\d+ columns are not supported\\)";

    private final ZoneId jvmZone = ZoneId.systemDefault();
    private final LocalDateTime timeGapInJvmZone1 = LocalDateTime.of(1970, 1, 1, 0, 13, 42);
    private final LocalDateTime timeGapInJvmZone2 = LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000);
    private final LocalDateTime timeDoubledInJvmZone = LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000);

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    private final LocalDateTime timeGapInVilnius = LocalDateTime.of(2018, 3, 25, 3, 17, 17);
    private final LocalDateTime timeDoubledInVilnius = LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000);

    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");
    private final LocalDateTime timeGapInKathmandu = LocalDateTime.of(1986, 1, 1, 0, 13, 7);

    @BeforeClass
    public void setUp()
    {
        checkIsGap(jvmZone, timeGapInJvmZone1);
        checkIsGap(jvmZone, timeGapInJvmZone2);
        checkIsDoubled(jvmZone, timeDoubledInJvmZone);

        checkIsGap(vilnius, timeGapInVilnius);
        checkIsDoubled(vilnius, timeDoubledInVilnius);

        checkIsGap(kathmandu, timeGapInKathmandu);
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
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    /* Floating point types tests */

    @Test
    public void testFloatingPointMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "123.45", REAL, "REAL '123.45'")
                .addRoundTrip("real", "nan()", REAL, "CAST(nan() AS real)")
                .addRoundTrip("real", "+infinity()", REAL, "CAST(+infinity() AS real)")
                .addRoundTrip("real", "-infinity()", REAL, "CAST(-infinity() AS real)")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "double '1.0E100'")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "nan()", DOUBLE, "CAST(nan() AS double)")
                .addRoundTrip("double", "+infinity()", DOUBLE, "CAST(+infinity() AS double)")
                .addRoundTrip("double", "-infinity()", DOUBLE, "CAST(-infinity() AS double)")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .execute(getQueryRunner(), trinoCreateAsSelect("floats"))
                .execute(getQueryRunner(), trinoCreateAndInsert("floats"));
    }

    @Test
    public void testOracleFloatingPointMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("float", "1E100", DOUBLE, "double '1E100'")
                .addRoundTrip("float", "1.0", DOUBLE, "double '1.0'")
                .addRoundTrip("float", "123456.123456", DOUBLE, "double '123456.123456'")
                .addRoundTrip("float", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("float(126)", "1E100", DOUBLE, "double '1E100'")
                .addRoundTrip("float(126)", "1.0", DOUBLE, "double '1.0'")
                .addRoundTrip("float(126)", "1234567890123456789.0123456789", DOUBLE, "double '1234567890123456789.0123456789'")
                .addRoundTrip("float(126)", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("float(1)", "100000.0", DOUBLE, "double '100000.0'")
                .addRoundTrip("float(7)", "123000.0", DOUBLE, "double '123000.0'")
                .execute(getQueryRunner(), oracleCreateAndInsert("oracle_float"));
    }

    @Test
    public void testFloatingPointReadMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("binary_float", "123.45", REAL, "REAL '123.45'")
                .addRoundTrip("binary_float", "'nan'", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("binary_float", "'infinity'", REAL, "CAST(+infinity() AS REAL)")
                .addRoundTrip("binary_float", "'-infinity'", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("binary_float", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("binary_double", "1.0E100", DOUBLE, "double '1.0E100'")
                .addRoundTrip("binary_double", "'nan'", DOUBLE, "CAST(nan() AS double)")
                .addRoundTrip("binary_double", "'infinity'", DOUBLE, "CAST(+infinity() AS double)")
                .addRoundTrip("binary_double", "'-infinity'", DOUBLE, "CAST(-infinity() AS double)")
                .execute(getQueryRunner(), oracleCreateAndInsert("read_floats"));
    }

    /* varchar tests */

    @Test
    public void testVarcharMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'string 010'", createVarcharType(10), "CAST('string 010' AS VARCHAR(10))")
                .addRoundTrip("varchar(20)", "'string 20'", createVarcharType(20), "CAST('string 20' AS VARCHAR(20))")
                .addRoundTrip(format("varchar(%d)", MAX_VARCHAR2_ON_WRITE), "'string max size'",
                        createVarcharType(MAX_VARCHAR2_ON_WRITE), format("CAST('string max size' AS VARCHAR(%d))", MAX_VARCHAR2_ON_WRITE))
                .addRoundTrip("varchar(5)", "NULL", createVarcharType(5), "CAST(NULL AS VARCHAR(5))")
                .execute(getQueryRunner(), trinoCreateAsSelect("varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("varchar"));
    }

    @Test
    public void testVarcharReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar2(5 char)", "NULL", createVarcharType(5), "CAST(NULL AS VARCHAR(5))")
                .addRoundTrip("varchar2(10 char)", "'string 010'", createVarcharType(10), "CAST('string 010' AS VARCHAR(10))")
                .addRoundTrip("varchar2(20 char)", "'string 20'", createVarcharType(20), "CAST('string 20' AS VARCHAR(20))")
                .addRoundTrip(format("varchar2(%d char)", MAX_VARCHAR2_ON_WRITE), "'string max size'",
                        createVarcharType(MAX_VARCHAR2_ON_WRITE), format("CAST('string max size' AS VARCHAR(%d))", MAX_VARCHAR2_ON_WRITE))
                .addRoundTrip("varchar2(5 byte)", "NULL", createVarcharType(5), "CAST(NULL AS VARCHAR(5))")
                .addRoundTrip("varchar2(10 byte)", "'string 010'", createVarcharType(10), "CAST('string 010' AS VARCHAR(10))")
                .addRoundTrip("varchar2(20 byte)", "'string 20'", createVarcharType(20), "CAST('string 20' AS VARCHAR(20))")
                .addRoundTrip(format("varchar2(%d byte)", MAX_VARCHAR2_ON_READ), "'string max size'",
                        createVarcharType(MAX_VARCHAR2_ON_READ), format("CAST('string max size' AS VARCHAR(%d))", MAX_VARCHAR2_ON_READ))
                .addRoundTrip("nvarchar2(5)", "NULL", createVarcharType(5), "CAST(NULL AS VARCHAR(5))")
                .addRoundTrip("nvarchar2(10)", "'string 010'", createVarcharType(10), "CAST('string 010' AS VARCHAR(10))")
                .addRoundTrip("nvarchar2(20)", "'string 20'", createVarcharType(20), "CAST('string 20' AS VARCHAR(20))")
                .addRoundTrip(format("nvarchar2(%d)", MAX_NVARCHAR2), "'string max size'",
                        createVarcharType(MAX_NVARCHAR2), format("CAST('string max size' AS VARCHAR(%d))", MAX_NVARCHAR2))
                .execute(getQueryRunner(), oracleCreateAndInsert("read_varchar"));
    }

    /*
    The unicode tests assume the following Oracle database parameters:
     - NLS_NCHAR_CHARACTERSET = AL16UTF16
     - NLS_CHARACTERSET = AL32UTF8
     */
    @Test
    public void testVarcharUnicodeMapping()
    {
        // the number of Unicode code points in 攻殻機動隊 is 5, and in 😂 is 1.
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("varchar(13)", "'攻殻機動隊'", createVarcharType(13), "CAST('攻殻機動隊' AS varchar(13))")
                .addRoundTrip(format("varchar(%d)", MAX_VARCHAR2_ON_WRITE), "'攻殻機動隊'",
                        createVarcharType(MAX_VARCHAR2_ON_WRITE), format("CAST('攻殻機動隊' AS varchar(%d))", MAX_VARCHAR2_ON_WRITE))
                .addRoundTrip("varchar(1)", "'😂'", createVarcharType(1), "CAST('😂' AS varchar(1))")
                .addRoundTrip("varchar(6)", "'😂'", createVarcharType(6), "CAST('😂' AS varchar(6))")
                .execute(getQueryRunner(), trinoCreateAsSelect("varchar_unicode"))
                .execute(getQueryRunner(), trinoCreateAndInsert("varchar_unicode"));
    }

    @Test
    public void testVarcharUnicodeReadMapping()
    {
        SqlDataTypeTest.create()
                // the number of Unicode code points in 攻殻機動隊 is 5, and in 😂 is 1.
                .addRoundTrip("varchar2(5 char)", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("varchar2(13 char)", "'攻殻機動隊'", createVarcharType(13), "CAST('攻殻機動隊' AS varchar(13))")
                .addRoundTrip(format("varchar2(%d char)", MAX_VARCHAR2_ON_READ), "'攻殻機動隊'",
                        createVarcharType(MAX_VARCHAR2_ON_READ), format("CAST('攻殻機動隊' AS varchar(%d))", MAX_VARCHAR2_ON_READ))
                .addRoundTrip("varchar2(1 char)", "'😂'", createVarcharType(1), "CAST('😂' AS varchar(1))")
                .addRoundTrip("varchar2(6 char)", "'😂'", createVarcharType(6), "CAST('😂' AS varchar(6))")
                // the number of bytes using charset UTF-8 in 攻殻機動隊 is 15, and in '😂' is 4.
                .addRoundTrip("varchar2(15 byte)", "'攻殻機動隊'", createVarcharType(15), "CAST('攻殻機動隊' AS varchar(15))")
                .addRoundTrip("varchar2(23 byte)", "'攻殻機動隊'", createVarcharType(23), "CAST('攻殻機動隊' AS varchar(23))")
                .addRoundTrip(format("varchar2(%d byte)", MAX_VARCHAR2_ON_READ), "'攻殻機動隊'",
                        createVarcharType(MAX_VARCHAR2_ON_READ), format("CAST('攻殻機動隊' AS varchar(%d))", MAX_VARCHAR2_ON_READ))
                .addRoundTrip("varchar2(4 byte)", "'😂'", createVarcharType(4), "CAST('😂' AS varchar(4))")
                .addRoundTrip("varchar2(9 byte)", "'😂'", createVarcharType(9), "CAST('😂' AS varchar(9))")
                // the length of string in 攻殻機動隊 is 5, and in 😂 is 2.
                .addRoundTrip("nvarchar2(5)", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("nvarchar2(13)", "'攻殻機動隊'", createVarcharType(13), "CAST('攻殻機動隊' AS varchar(13))")
                .addRoundTrip(format("nvarchar2(%d)", MAX_NVARCHAR2), "'攻殻機動隊'",
                        createVarcharType(MAX_NVARCHAR2), format("CAST('攻殻機動隊' AS varchar(%d))", MAX_NVARCHAR2))
                .addRoundTrip("nvarchar2(2)", "'😂'", createVarcharType(2), "CAST('😂' AS varchar(2))")
                .addRoundTrip("nvarchar2(7)", "'😂'", createVarcharType(7), "CAST('😂' AS varchar(7))")
                .execute(getQueryRunner(), oracleCreateAndInsert("read_varchar_unicode"));
    }

    @Test
    public void testUnboundedVarcharMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'攻殻機動隊'", createUnboundedVarcharType(), "VARCHAR '攻殻機動隊'")
                .addRoundTrip("varchar", "'😂'", createUnboundedVarcharType(), "VARCHAR '😂'")
                .addRoundTrip("varchar", "'clob'", createUnboundedVarcharType(), "VARCHAR 'clob'")
                .addRoundTrip("varchar", "NULL", createUnboundedVarcharType(), "CAST(NULL AS varchar)")
                .addRoundTrip(format("varchar(%d)", MAX_VARCHAR2_ON_WRITE + 1), "'攻殻機動隊'", createUnboundedVarcharType(), "VARCHAR '攻殻機動隊'")
                .addRoundTrip(format("varchar(%d)", MAX_VARCHAR2_ON_WRITE + 1), "'😂'", createUnboundedVarcharType(), "VARCHAR '😂'")
                .addRoundTrip(format("varchar(%d)", MAX_VARCHAR2_ON_WRITE + 1), "'clob'", createUnboundedVarcharType(), "VARCHAR 'clob'")
                .addRoundTrip(format("varchar(%d)", MAX_VARCHAR2_ON_WRITE + 1), "NULL", createUnboundedVarcharType(), "CAST(NULL AS varchar)")
                .addRoundTrip(format("char(%d)", MAX_CHAR_ON_WRITE + 1), "'攻殻機動隊'", createUnboundedVarcharType(), "VARCHAR '攻殻機動隊'")
                .addRoundTrip(format("char(%d)", MAX_CHAR_ON_WRITE + 1), "'😂'", createUnboundedVarcharType(), "VARCHAR '😂'")
                .addRoundTrip(format("char(%d)", MAX_CHAR_ON_WRITE + 1), "'clob'", createUnboundedVarcharType(), "VARCHAR 'clob'")
                .addRoundTrip(format("char(%d)", MAX_CHAR_ON_WRITE + 1), "NULL", createUnboundedVarcharType(), "CAST(NULL AS varchar)")
                .execute(getQueryRunner(), trinoCreateAsSelect("unbounded"))
                .execute(getQueryRunner(), trinoCreateAndInsert("unbounded"));
    }

    @Test
    public void testUnboundedVarcharReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("clob", "'攻殻機動隊'", createUnboundedVarcharType(), "VARCHAR '攻殻機動隊'")
                .addRoundTrip("clob", "'😂'", createUnboundedVarcharType(), "VARCHAR '😂'")
                .addRoundTrip("clob", "'clob'", createUnboundedVarcharType(), "VARCHAR 'clob'")
                .addRoundTrip("clob", "NULL", createUnboundedVarcharType(), "CAST(NULL AS VARCHAR)")
                .addRoundTrip("clob", "empty_clob()", createUnboundedVarcharType(), "VARCHAR ''")
                .addRoundTrip("nclob", "'攻殻機動隊'", createUnboundedVarcharType(), "VARCHAR '攻殻機動隊'")
                .addRoundTrip("nclob", "'😂'", createUnboundedVarcharType(), "VARCHAR '😂'")
                .addRoundTrip("nclob", "'clob'", createUnboundedVarcharType(), "VARCHAR 'clob'")
                .addRoundTrip("nclob", "NULL", createUnboundedVarcharType(), "CAST(NULL AS VARCHAR)")
                .addRoundTrip("nclob", "empty_clob()", createUnboundedVarcharType(), "VARCHAR ''")
                .execute(getQueryRunner(), oracleCreateAndInsert("read_unbounded"));
        // The tests on empty strings are read-only because Oracle treats empty
        // strings as NULL. The empty clob is generated by an Oracle function.
    }

    /* char tests */

    @Test
    public void testCharMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'string 010'", createCharType(10), "CAST('string 010' AS CHAR(10))")
                .addRoundTrip("char(20)", "'string 20'", createCharType(20), "CAST('string 20' AS CHAR(20))")
                .addRoundTrip(format("char(%d)", MAX_CHAR_ON_WRITE), "'string max size'",
                        createCharType(MAX_CHAR_ON_WRITE), format("CAST('string max size' AS CHAR(%d))", MAX_CHAR_ON_WRITE))
                .addRoundTrip("char(5)", "NULL", createCharType(5), "CAST(NULL AS CHAR(5))")
                .execute(getQueryRunner(), trinoCreateAsSelect("char"))
                .execute(getQueryRunner(), trinoCreateAndInsert("char"));
    }

    @Test
    public void testCharReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(5 char)", "NULL", createCharType(5), "CAST(NULL AS CHAR(5))")
                .addRoundTrip("char(10 char)", "'string 010'", createCharType(10), "CAST('string 010' AS CHAR(10))")
                .addRoundTrip("char(20 char)", "'string 20'", createCharType(20), "CAST('string 20' AS CHAR(20))")
                .addRoundTrip(format("char(%d char)", MAX_CHAR_ON_READ), "'string max size'",
                        createCharType(MAX_CHAR_ON_READ), format("CAST('string max size' AS CHAR(%d))", MAX_CHAR_ON_READ))

                .addRoundTrip("char(5 byte)", "NULL", createCharType(5), "CAST(NULL AS CHAR(5))")
                .addRoundTrip("char(10 byte)", "'string 010'", createCharType(10), "CAST('string 010' AS CHAR(10))")
                .addRoundTrip("char(20 byte)", "'string 20'", createCharType(20), "CAST('string 20' AS CHAR(20))")
                .addRoundTrip(format("char(%d byte)", MAX_CHAR_ON_READ), "'string max size'",
                        createCharType(MAX_CHAR_ON_READ), format("CAST('string max size' AS CHAR(%d))", MAX_CHAR_ON_READ))

                .addRoundTrip("nchar(5)", "NULL", createCharType(5), "CAST(NULL AS CHAR(5))")
                .addRoundTrip("nchar(10)", "'string 010'", createCharType(10), "CAST('string 010' AS CHAR(10))")
                .addRoundTrip("nchar(20)", "'string 20'", createCharType(20), "CAST('string 20' AS CHAR(20))")
                .addRoundTrip(format("nchar(%d)", MAX_NCHAR), "'string max size'",
                        createCharType(MAX_NCHAR), format("CAST('string max size' AS CHAR(%d))", MAX_NCHAR))
                .execute(getQueryRunner(), oracleCreateAndInsert("read_char"));
    }

    @Test
    public void testCharUnicodeMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(5)", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS char(5))")
                .addRoundTrip("char(13)", "'攻殻機動隊'", createCharType(13), "CAST('攻殻機動隊' AS char(13))")
                .addRoundTrip(format("char(%d)", MAX_CHAR_ON_WRITE), "'攻殻機動隊'",
                        createCharType(MAX_CHAR_ON_WRITE), format("CAST('攻殻機動隊' AS char(%d))", MAX_CHAR_ON_WRITE))
                .addRoundTrip("char(1)", "'😂'", createCharType(1), "CAST('😂' AS char(1))")
                .addRoundTrip("char(6)", "'😂'", createCharType(6), "CAST('😂' AS char(6))")
                .execute(getQueryRunner(), trinoCreateAsSelect("char_unicode"));
    }

    @Test
    public void testCharUnicodeReadMapping()
    {
        SqlDataTypeTest.create()
                // the number of Unicode code points in 攻殻機動隊 is 5, and in 😂 is 1.
                .addRoundTrip("char(5 char)", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS CHAR(5))")
                .addRoundTrip("char(13 char)", "'攻殻機動隊'", createCharType(13), "CAST('攻殻機動隊' AS CHAR(13))")
                .addRoundTrip(format("char(%d char)", MAX_CHAR_ON_READ), "'攻殻機動隊'",
                        createCharType(MAX_CHAR_ON_READ), format("CAST('攻殻機動隊' AS char(%d))", MAX_CHAR_ON_READ))
                .addRoundTrip("char(1 char)", "'😂'", createCharType(1), "CAST('😂' AS CHAR(1))")
                .addRoundTrip("char(6 char)", "'😂'", createCharType(6), "CAST('😂' AS CHAR(6))")
                // the number of bytes using charset UTF-8 in 攻殻機動隊 is 15, and in 😂 is 4.
                .addRoundTrip("char(15 byte)", "'攻殻機動隊'", createCharType(15), "CAST('攻殻機動隊' AS CHAR(15))")
                .addRoundTrip("char(23 byte)", "'攻殻機動隊'", createCharType(23), "CAST('攻殻機動隊' AS CHAR(23))")
                .addRoundTrip(format("char(%d byte)", MAX_CHAR_ON_READ), "'攻殻機動隊'",
                        createCharType(MAX_CHAR_ON_READ), format("CAST('攻殻機動隊' AS CHAR(%d))", MAX_CHAR_ON_READ))
                .addRoundTrip("char(4 byte)", "'😂'", createCharType(4), "CAST('😂' AS CHAR(4))")
                .addRoundTrip("char(9 byte)", "'😂'", createCharType(9), "CAST('😂' AS CHAR(9))")
                // the length of string in 攻殻機動隊 is 5, and in 😂 is 2.
                .addRoundTrip("nchar(5)", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS CHAR(5))")
                .addRoundTrip("nchar(13)", "'攻殻機動隊'", createCharType(13), "CAST('攻殻機動隊' AS CHAR(13))")
                .addRoundTrip(format("nchar(%d)", MAX_NCHAR), "'攻殻機動隊'",
                        createCharType(MAX_NCHAR), format("CAST('攻殻機動隊' AS CHAR(%d))", MAX_NCHAR))
                .addRoundTrip("nchar(2)", "'😂'", createCharType(2), "CAST('😂' AS CHAR(2))")
                .addRoundTrip("nchar(7)", "'😂'", createCharType(7), "CAST('😂' AS CHAR(7))")
                .execute(getQueryRunner(), oracleCreateAndInsert("read_char_unicode"));
    }

    /* Decimal tests */

    @Test
    public void testDecimalMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "CAST(193 AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(193 AS DECIMAL(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST(19 AS DECIMAL(3, 0)) ", createDecimalType(3, 0), "CAST(19 AS DECIMAL(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST(-193 AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(-193 AS DECIMAL(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST(10.0 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(10.0 AS DECIMAL(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST(10.1 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST(-10.1 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(-10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST(2 AS DECIMAL(4, 2))", createDecimalType(4, 2), "CAST(2 AS DECIMAL(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST(2.3 AS DECIMAL(4, 2))", createDecimalType(4, 2), "CAST(2.3 AS DECIMAL(4, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST(2 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(2 AS DECIMAL(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST(2.3 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(2.3 AS DECIMAL(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST(123456789.3 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(123456789.3 AS DECIMAL(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST(12345678901234567890.31 AS DECIMAL(24, 4))", createDecimalType(24, 4), "CAST(12345678901234567890.31 AS DECIMAL(24, 4))")
                .addRoundTrip("decimal(30, 5)", "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))", createDecimalType(30, 5), "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))", createDecimalType(30, 5), "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS DECIMAL(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS DECIMAL(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS DECIMAL(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS DECIMAL(38, 0))")
                .addRoundTrip("decimal(38, 38)", "CAST(.10000200003000040000500006000070000888 AS DECIMAL(38, 38))", createDecimalType(38, 38), "CAST(.10000200003000040000500006000070000888 AS DECIMAL(38, 38))")
                .addRoundTrip("decimal(38, 38)", "CAST(-.27182818284590452353602874713526624977 AS DECIMAL(38, 38))", createDecimalType(38, 38), "CAST(-.27182818284590452353602874713526624977 AS DECIMAL(38, 38))")
                .addRoundTrip("decimal(10, 3)", "CAST(NULL AS DECIMAL(10, 3))", createDecimalType(10, 3), "CAST(NULL AS DECIMAL(10, 3))")
                .execute(getQueryRunner(), trinoCreateAsSelect("decimals"))
                .execute(getQueryRunner(), trinoCreateAndInsert("decimals"));
    }

    @Test
    public void testIntegerMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "0", createDecimalType(3, 0), "CAST(0 AS DECIMAL(3, 0))")
                .addRoundTrip("smallint", "0", createDecimalType(5, 0), "CAST(0 AS DECIMAL(5, 0))")
                .addRoundTrip("integer", "0", createDecimalType(10, 0), "CAST(0 AS DECIMAL(10, 0))")
                .addRoundTrip("bigint", "0", createDecimalType(19, 0), "CAST(0 AS DECIMAL(19, 0))")
                .execute(getQueryRunner(), trinoCreateAsSelect("integers"))
                .execute(getQueryRunner(), trinoCreateAndInsert("integers"));
    }

    @Test
    public void testDecimalReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "193", createDecimalType(3, 0), "CAST(193 AS DECIMAL(3, 0))")
                .addRoundTrip("decimal(3, 0)", "19", createDecimalType(3, 0), "CAST(19 AS DECIMAL(3, 0))")
                .addRoundTrip("decimal(3, 0)", "-193", createDecimalType(3, 0), "CAST(-193 AS DECIMAL(3, 0))")
                .addRoundTrip("decimal(3, 1)", "10.0", createDecimalType(3, 1), "CAST(10.0 AS DECIMAL(3, 1))")
                .addRoundTrip("decimal(3, 1)", "10.1", createDecimalType(3, 1), "CAST(10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("decimal(3, 1)", "-10.1", createDecimalType(3, 1), "CAST(-10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("decimal(4, 2)", "2", createDecimalType(4, 2), "CAST(2 AS DECIMAL(4, 2))")
                .addRoundTrip("decimal(4, 2)", "2.3", createDecimalType(4, 2), "CAST(2.3 AS DECIMAL(4, 2))")
                .addRoundTrip("decimal(24, 2)", "2", createDecimalType(24, 2), "CAST(2 AS DECIMAL(24, 2))")
                .addRoundTrip("decimal(24, 2)", "2.3", createDecimalType(24, 2), "CAST(2.3 AS DECIMAL(24, 2))")
                .addRoundTrip("decimal(24, 2)", "123456789.3", createDecimalType(24, 2), "CAST(123456789.3 AS DECIMAL(24, 2))")
                .addRoundTrip("decimal(24, 4)", "12345678901234567890.31", createDecimalType(24, 4), "CAST(12345678901234567890.31 AS DECIMAL(24, 4))")
                .addRoundTrip("decimal(30, 5)", "3141592653589793238462643.38327", createDecimalType(30, 5), "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("decimal(30, 5)", "-3141592653589793238462643.38327", createDecimalType(30, 5), "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("decimal(38, 0)", "27182818284590452353602874713526624977", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS DECIMAL(38, 0))")
                .addRoundTrip("decimal(38, 0)", "-27182818284590452353602874713526624977", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS DECIMAL(38, 0))")
                .addRoundTrip("decimal(38, 38)", ".10000200003000040000500006000070000888", createDecimalType(38, 38), "CAST(.10000200003000040000500006000070000888 AS DECIMAL(38, 38))")
                .addRoundTrip("decimal(38, 38)", "-.27182818284590452353602874713526624977", createDecimalType(38, 38), "CAST(-.27182818284590452353602874713526624977 AS DECIMAL(38, 38))")
                .addRoundTrip("decimal(10, 3)", "NULL", createDecimalType(10, 3), "CAST(NULL AS DECIMAL(10, 3))")
                .execute(getQueryRunner(), oracleCreateAndInsert("read_decimals"));
    }

    @Test
    public void testNumberWithoutScaleReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("number(1)", "1", createDecimalType(1, 0), "CAST (1 AS DECIMAL(1, 0))")
                .addRoundTrip("number(2)", "99", createDecimalType(2, 0), "CAST (99 AS DECIMAL(2, 0))")
                .addRoundTrip("number(38)", "99999999999999999999999999999999999999", createDecimalType(38, 0), "CAST ('99999999999999999999999999999999999999' AS DECIMAL(38, 0))") // max
                .addRoundTrip("number(38)", "-99999999999999999999999999999999999999", createDecimalType(38, 0), "CAST ('-99999999999999999999999999999999999999' AS DECIMAL(38, 0))") // min
                .execute(getQueryRunner(), oracleCreateAndInsert("number_without_scale"));
    }

    @Test
    public void testNumberWithoutPrecisionAndScaleReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("number", "1", createDecimalType(38, 9), "CAST(1 AS DECIMAL(38, 9))")
                .addRoundTrip("number", "99", createDecimalType(38, 9), "CAST(99 AS DECIMAL(38, 9))")
                .addRoundTrip("number", "9999999999999999999999999999.999999999", createDecimalType(38, 9), "CAST('9999999999999999999999999999.999999999' AS DECIMAL(38, 9))") // max
                .addRoundTrip("number", "-9999999999999999999999999999.999999999", createDecimalType(38, 9), "CAST('-9999999999999999999999999999.999999999' AS DECIMAL(38, 9))") // min
                .execute(getQueryRunner(), number(9), oracleCreateAndInsert("no_prec_and_scale"));
    }

    @Test
    public void testRoundingOfUnspecifiedNumber()
    {
        try (TestTable table = oracleTable("rounding", "col NUMBER", "(0.123456789)")) {
            assertQuery(number(9), "SELECT * FROM " + table.getName(), "VALUES 0.123456789");
            assertQuery(number(HALF_EVEN, 6), "SELECT * FROM " + table.getName(), "VALUES 0.123457");
            assertQuery(number(HALF_EVEN, 3), "SELECT * FROM " + table.getName(), "VALUES 0.123");
            assertQueryFails(number(UNNECESSARY, 3), "SELECT * FROM " + table.getName(), "Rounding necessary");
        }

        try (TestTable table = oracleTable("rounding", "col NUMBER", "(123456789012345678901234567890.123456789)")) {
            assertQueryFails(number(9), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQuery(number(HALF_EVEN, 8), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.12345679");
            assertQuery(number(HALF_EVEN, 6), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.123457");
            assertQuery(number(HALF_EVEN, 3), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.123");
            assertQueryFails(number(UNNECESSARY, 3), "SELECT * FROM " + table.getName(), "Rounding necessary");
        }

        try (TestTable table = oracleTable("rounding", "col NUMBER", "(123456789012345678901234567890123456789)")) {
            assertQueryFails(number(0), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQueryFails(number(HALF_EVEN, 8), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQueryFails(number(HALF_EVEN, 0), "SELECT * FROM " + table.getName(), "Decimal overflow");
        }
    }

    @Test
    public void testNumberNegativeScaleReadMapping()
    {
        // TODO: Add similar tests for write mappings.
        // Those tests would require the table to be created in Oracle, but values inserted
        // by Trino, which is outside the capabilities of the current DataSetup classes.
        SqlDataTypeTest.create()
                .addRoundTrip("number(1, -1)", "20", createDecimalType(2, 0), "CAST(20 AS DECIMAL(2, 0))")
                .addRoundTrip("number(1, -1)", "35", createDecimalType(2, 0), "CAST(40 AS DECIMAL(2, 0))") // More useful as a test for write mappings.
                .addRoundTrip("number(2, -4)", "470000", createDecimalType(6, 0), "CAST(470000 AS DECIMAL(6, 0))")
                .addRoundTrip("number(2, -4)", "-80000", createDecimalType(6, 0), "CAST(-80000 AS DECIMAL(6, 0))")
                .addRoundTrip("number(8, -3)", "-8.8888888E+10", createDecimalType(11, 0), "CAST(-8.8888888E+10 AS DECIMAL(11, 0))")
                .addRoundTrip("number(8, -3)", "4050000", createDecimalType(11, 0), "CAST(4050000 AS DECIMAL(11, 0))")
                .addRoundTrip("number(14, -14)", "1.4000014000014E+27", createDecimalType(28, 0), "CAST(1.4000014000014E+27 AS DECIMAL(28, 0))")
                .addRoundTrip("number(14, -14)", "1E+21", createDecimalType(28, 0), "CAST(1E+21 AS DECIMAL(28, 0))")
                .addRoundTrip("number(5, -33)", "1.2345E+37", createDecimalType(38, 0), "CAST(1.2345E+37 AS DECIMAL(38, 0))")
                .addRoundTrip("number(5, -33)", "-1.2345E+37", createDecimalType(38, 0), "CAST(-1.2345E+37 AS DECIMAL(38, 0))")
                .addRoundTrip("number(1, -37)", "1E+37", createDecimalType(38, 0), "CAST(1E+37 AS DECIMAL(38, 0))")
                .addRoundTrip("number(1, -37)", "-1E+37", createDecimalType(38, 0), "CAST(-1E+37 AS DECIMAL(38, 0))")
                .addRoundTrip("number(37, -1)", "99999999999999999999999999999999999990", createDecimalType(38, 0), "CAST('99999999999999999999999999999999999990' AS DECIMAL(38, 0))") // max
                .addRoundTrip("number(37, -1)", "-99999999999999999999999999999999999990", createDecimalType(38, 0), "CAST('-99999999999999999999999999999999999990' AS DECIMAL(38, 0))") // min
                .execute(getQueryRunner(), oracleCreateAndInsert("number_negative_s"));
    }

    @Test
    public void testHighNumberScale()
    {
        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(38, 40)", "(0.0012345678901234567890123456789012345678)")) {
            assertQueryFails(number(UNNECESSARY), "SELECT * FROM " + table.getName(), NO_SUPPORTED_COLUMNS);
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0.00123456789012345678901234567890123457");
            assertQuery(numberConvertToVarchar(), "SELECT * FROM " + table.getName(), "VALUES '1.2345678901234567890123456789012345678E-03'");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(18, 40)", "(0.0000000000000000000000123456789012345678)")) {
            assertQueryFails(number(UNNECESSARY), "SELECT * FROM " + table.getName(), NO_SUPPORTED_COLUMNS);
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0.00000000000000000000001234567890123457");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(38, 80)", "(0.00000000000000000000000000000000000000000000012345678901234567890123456789012345678)")) {
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0");
            assertQuery(numberConvertToVarchar(), "SELECT * FROM " + table.getName(), "VALUES '1.2345678901234567890123456789012346E-46'");
        }
    }

    @Test
    public void testNumberWithHiveNegativeScaleReadMapping()
    {
        try (TestTable table = oracleTable("highNegativeScale", "col NUMBER(38, -60)", "(1234567890123456789012345678901234567000000000000000000000000000000000000000000000000000000000000)")) {
            assertQuery(numberConvertToVarchar(), "SELECT * FROM " + table.getName(), "VALUES '1.234567890123456789012345678901234567E96'");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(18, 60)", "(0.000000000000000000000000000000000000000000000123456789012345678)")) {
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0");
        }
    }

    private Session number(int scale)
    {
        return number(IGNORE, UNNECESSARY, Optional.of(scale));
    }

    private Session number(RoundingMode roundingMode)
    {
        return number(IGNORE, roundingMode, Optional.empty());
    }

    private Session number(RoundingMode roundingMode, int scale)
    {
        return number(IGNORE, roundingMode, Optional.of(scale));
    }

    private Session numberConvertToVarchar()
    {
        return number(CONVERT_TO_VARCHAR, UNNECESSARY, Optional.empty());
    }

    private Session number(UnsupportedTypeHandling unsupportedTypeHandlingStrategy, RoundingMode roundingMode, Optional<Integer> scale)
    {
        Session.SessionBuilder builder = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandlingStrategy.name())
                .setCatalogSessionProperty("oracle", NUMBER_ROUNDING_MODE, roundingMode.name());
        scale.ifPresent(value -> builder.setCatalogSessionProperty("oracle", NUMBER_DEFAULT_SCALE, value.toString()));
        return builder.build();
    }

    @Test
    public void testSpecialNumberFormats()
    {
        getOracleSqlExecutor().execute("CREATE TABLE test (num1 number)");
        getOracleSqlExecutor().execute("INSERT INTO test VALUES (12345678901234567890.12345678901234567890123456789012345678)");
        assertQuery(number(HALF_UP, 10), "SELECT * FROM test", "VALUES (12345678901234567890.1234567890)");
    }

    @Test
    public void testBooleanType()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "CAST(true AS DECIMAL(1, 0))", createDecimalType(1, 0), "CAST(true AS DECIMAL(1, 0))")
                .addRoundTrip("boolean", "CAST(false AS DECIMAL(1, 0))", createDecimalType(1, 0), "CAST(false AS DECIMAL(1, 0))")
                .execute(getQueryRunner(), trinoCreateAsSelect("boolean_types"))
                .execute(getQueryRunner(), trinoCreateAndInsert("boolean_types"));
    }

    @Test
    public void testVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "CAST(NULL AS varbinary)") // empty stored as NULL
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varbinary"));

        SqlDataTypeTest.create()
                .addRoundTrip("blob", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("blob", "empty_blob()", VARBINARY, "X''")
                .addRoundTrip("blob", "hextoraw('68656C6C6F')", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("blob", "hextoraw('5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD')", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("blob", "hextoraw('4261672066756C6C206F6620F09F92B0')", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("blob", "hextoraw('0001020304050607080DF9367AA7000000')", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("blob", "hextoraw('000000000000')", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), oracleCreateAndInsert("test_blob"));

        SqlDataTypeTest.create()
                .addRoundTrip("raw(2000)", "NULL", VARBINARY, "CAST(NULL AS varbinary)")
                .addRoundTrip("raw(2000)", "empty_blob()", VARBINARY, "CAST(NULL AS varbinary)") // empty stored as NULL
                .addRoundTrip("raw(2000)", "hextoraw('68656C6C6F')", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("raw(2000)", "hextoraw('5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD')", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("raw(2000)", "hextoraw('4261672066756C6C206F6620F09F92B0')", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("raw(2000)", "hextoraw('0001020304050607080DF9367AA7000000')", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("raw(2000)", "hextoraw('000000000000')", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), oracleCreateAndInsert("test_blob"));
    }

    @Test
    public void testDate()
    {
        // Note: these test cases are duplicates of those for PostgreSQL and MySQL.

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone =
                LocalDate.of(1970, 1, 1);

        verify(jvmZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeForwardAtMidnightInJvmZone
                        .atStartOfDay()).isEmpty());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone =
                LocalDate.of(1983, 4, 1);

        verify(someZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeForwardAtMidnightInSomeZone
                        .atStartOfDay()).isEmpty());

        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone =
                LocalDate.of(1983, 10, 1);

        verify(someZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeBackwardAtMidnightInSomeZone
                        .atStartOfDay().minusMinutes(1)).size() == 2);

        SqlDataTypeTest dateTests = SqlDataTypeTest.create()
                // min value in Oracle
                .addRoundTrip("DATE", "DATE '-4712-01-01'", TIMESTAMP_SECONDS, "TIMESTAMP '-4712-01-01 00:00:00'")
                .addRoundTrip("DATE", "DATE '-0001-01-01'", TIMESTAMP_SECONDS, "TIMESTAMP '-0001-01-01 00:00:00'")
                .addRoundTrip("DATE", "DATE '0001-01-01'", TIMESTAMP_SECONDS, "TIMESTAMP '0001-01-01 00:00:00'")
                // day before and after julian->gregorian calendar switch
                .addRoundTrip("DATE", "DATE '1582-10-04'", TIMESTAMP_SECONDS, "TIMESTAMP '1582-10-04 00:00:00'")
                .addRoundTrip("DATE", "DATE '1582-10-15'", TIMESTAMP_SECONDS, "TIMESTAMP '1582-10-15 00:00:00'")
                // before epoch
                .addRoundTrip("DATE", "DATE '1952-04-03'", TIMESTAMP_SECONDS, "TIMESTAMP '1952-04-03 00:00:00'")
                .addRoundTrip("DATE", "DATE '1970-01-01'", TIMESTAMP_SECONDS, "TIMESTAMP '1970-01-01 00:00:00'")
                .addRoundTrip("DATE", "DATE '1970-02-03'", TIMESTAMP_SECONDS, "TIMESTAMP '1970-02-03 00:00:00'")
                // summer on northern hemisphere (possible DST)
                .addRoundTrip("DATE", "DATE '2017-07-01'", TIMESTAMP_SECONDS, "TIMESTAMP '2017-07-01 00:00:00'")
                // winter on northern hemisphere
                // (possible DST on southern hemisphere)
                .addRoundTrip("DATE", "DATE '2017-01-01'", TIMESTAMP_SECONDS, "TIMESTAMP '2017-01-01 00:00:00'")
                .addRoundTrip("DATE", "DATE '1983-04-01'", TIMESTAMP_SECONDS, "TIMESTAMP '1983-04-01 00:00:00'")
                .addRoundTrip("DATE", "DATE '1983-10-01'", TIMESTAMP_SECONDS, "TIMESTAMP '1983-10-01 00:00:00'")
                // max value in Oracle
                .addRoundTrip("DATE", "DATE '9999-12-31'", TIMESTAMP_SECONDS, "TIMESTAMP '9999-12-31 00:00:00'");

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), ZoneId.systemDefault().getId(), ZoneId.of("Europe/Vilnius").getId())) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(getTimeZoneKey(timeZoneId))
                    .build();
            dateTests.execute(getQueryRunner(), session, oracleCreateAndInsert("test_date"));
            dateTests.execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"));
            dateTests.execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
        }
    }

    @Test
    public void testJulianGregorianDate()
    {
        // Oracle TO_DATE function returns +10 days during julian and gregorian calendar switch
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_julian_dt", "(ts date)")) {
            assertUpdate(format("INSERT INTO %s VALUES (DATE '1582-10-05')", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES TIMESTAMP '1582-10-15 00:00:00'");
        }
    }

    @Test
    public void testUnsupportedDate()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_unsupported_dt", "(ts date)")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (DATE '-4713-12-31')", table.getName()),
                    "\\QFailed to insert data: ORA-01841: (full) year must be between -4713 and +9999, and not be 0\n");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (DATE '0000-01-01')", table.getName()),
                    "\\QFailed to insert data: ORA-01841: (full) year must be between -4713 and +9999, and not be 0\n");
            // The error message sounds invalid date format in the connector, but it's no problem as the max year is 9999 in Oracle
            assertQueryFails(
                    format("INSERT INTO %s VALUES (DATE '10000-01-01')", table.getName()),
                    "\\QFailed to insert data: ORA-01861: literal does not match format string\n");
        }
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        // using two non-JVM zones so that we don't need to worry what Oracle system zone is
        SqlDataTypeTest tests = SqlDataTypeTest.create()
                // min value in Oracle
                .addRoundTrip("timestamp", "TIMESTAMP '-4712-01-01 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '-4712-01-01 00:00:00.000'")
                .addRoundTrip("timestamp", "TIMESTAMP '-0001-01-01 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '-0001-01-01 00:00:00.000'")
                // day before and after julian->gregorian calendar switch
                .addRoundTrip("timestamp", "TIMESTAMP '1582-10-04 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1582-10-04 00:00:00.000'")
                .addRoundTrip("timestamp", "TIMESTAMP '1582-10-15 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1582-10-15 00:00:00.000'")
                // before epoch
                .addRoundTrip("timestamp", "TIMESTAMP '1958-01-01 13:18:03.123'", TIMESTAMP_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("timestamp", "TIMESTAMP '2019-03-18 10:01:17.987'", TIMESTAMP_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987'")
                // epoch, epoch also is a gap in JVM zone
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("timestamp", timestampDataType(3).toLiteral(timeDoubledInJvmZone), TIMESTAMP_MILLIS, timestampDataType(3).toLiteral(timeDoubledInJvmZone))
                .addRoundTrip("timestamp", timestampDataType(3).toLiteral(timeDoubledInVilnius), TIMESTAMP_MILLIS, timestampDataType(3).toLiteral(timeDoubledInVilnius))
                .addRoundTrip("timestamp", timestampDataType(3).toLiteral(timeGapInJvmZone1), TIMESTAMP_MILLIS, timestampDataType(3).toLiteral(timeGapInJvmZone1))
                .addRoundTrip("timestamp", timestampDataType(3).toLiteral(timeGapInJvmZone2), TIMESTAMP_MILLIS, timestampDataType(3).toLiteral(timeGapInJvmZone2))
                .addRoundTrip("timestamp", timestampDataType(3).toLiteral(timeGapInVilnius), TIMESTAMP_MILLIS, timestampDataType(3).toLiteral(timeGapInVilnius))
                .addRoundTrip("timestamp", timestampDataType(3).toLiteral(timeGapInKathmandu), TIMESTAMP_MILLIS, timestampDataType(3).toLiteral(timeGapInKathmandu))
                // max value in Oracle
                .addRoundTrip("timestamp", "TIMESTAMP '9999-12-31 00:00:00.000'", TIMESTAMP_MILLIS, "TIMESTAMP '9999-12-31 00:00:00.000'");

        Session session = Session.builder(getSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();
        tests.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
        tests.execute(getQueryRunner(), session, oracleCreateAndInsert("test_timestamp"));
    }

    @Test
    public void testJulianGregorianTimestamp()
    {
        // Oracle TO_DATE function returns +10 days during julian and gregorian calendar switch
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_julian_ts", "(ts date)")) {
            assertUpdate(format("INSERT INTO %s VALUES (timestamp '1582-10-05')", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES TIMESTAMP '1582-10-15 00:00:00'");
        }
    }

    @Test
    public void testUnsupportedTimestamp()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_unsupported_ts", "(ts timestamp)")) {
            assertQueryFails(
                    format("INSERT INTO %s VALUES (TIMESTAMP '-4713-12-31 00:00:00.000')", table.getName()),
                    "\\QFailed to insert data: ORA-01841: (full) year must be between -4713 and +9999, and not be 0\n");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (TIMESTAMP '0000-01-01 00:00:00.000')", table.getName()),
                    "\\QFailed to insert data: ORA-01841: (full) year must be between -4713 and +9999, and not be 0\n");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (TIMESTAMP '10000-01-01 00:00:00.000')", table.getName()),
                    "\\QFailed to insert data: ORA-01862: the numeric value does not match the length of the format item\n");
        }
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                // using two non-JVM zones so that we don't need to worry what Oracle system zone is
                {vilnius},
                {kathmandu},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    @Test
    public void testTimestampWithTimeZoneFromTrino()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '1970-01-01 00:00:00.000 Z'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000 Z'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '1970-01-01 00:00:00.000 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000 Asia/Kathmandu'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '1970-01-01 00:00:00.000 +02:17'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000 +02:17'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '1970-01-01 00:00:00.000 -07:31'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000 -07:31'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '1958-01-01 13:18:03.123 Z'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123 Z'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '1958-01-01 13:18:03.123 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123 Asia/Kathmandu'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '1958-01-01 13:18:03.123 +02:17'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123 +02:17'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '1958-01-01 13:18:03.123 -07:31'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123 -07:31'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '2019-03-18 10:01:17.987 Z'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987 Z'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '2019-03-18 10:01:17.987 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987 Asia/Kathmandu'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '2019-03-18 10:01:17.987 +02:17'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987 +02:17'")
                .addRoundTrip("timestamp with time zone", "TIMESTAMP '2019-03-18 10:01:17.987 -07:31'",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987 -07:31'")
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInJvmZone.atZone(UTC)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInJvmZone.atZone(UTC)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInJvmZone.atZone(jvmZone)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInJvmZone.atZone(jvmZone)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInJvmZone.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInJvmZone.atZone(kathmandu)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInVilnius.atZone(UTC)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInVilnius.atZone(UTC)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInVilnius.atZone(vilnius)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInVilnius.atZone(vilnius)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInVilnius.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInVilnius.atZone(kathmandu)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone1.atZone(UTC)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone1.atZone(UTC)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone1.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone1.atZone(kathmandu)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone2.atZone(UTC)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone2.atZone(UTC)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone2.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone2.atZone(kathmandu)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeGapInVilnius.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInVilnius.atZone(kathmandu)))
                .addRoundTrip("timestamp with time zone", timestampWithTimeZoneDataType(3).toLiteral(timeGapInKathmandu.atZone(vilnius)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInKathmandu.atZone(vilnius)))
                .execute(getQueryRunner(), trinoCreateAsSelect("timestamp_tz"))
                .execute(getQueryRunner(), trinoCreateAndInsert("timestamp_tz"));
    }

    @Test
    public void testTimestampWithTimeZoneFromOracle()
    {
        // TODO: Fix Oracle TimestampWithTimeZone mappings to handle DST correctly (https://github.com/trinodb/trino/issues/7739)
        DataTypeTest.create()
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), timeDoubledInJvmZone.atZone(jvmZone))
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), timeDoubledInVilnius.atZone(vilnius))
                .execute(getQueryRunner(), oracleCreateAndInsert("timestamp_tz"));

        SqlDataTypeTest.create()
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '1970-01-01 00:00:00.000000000', 'UTC')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000 Z'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '1970-01-01 00:00:00.000000000', 'Asia/Kathmandu')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000 Asia/Kathmandu'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '1970-01-01 00:00:00.000000000', '+02:17')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000 +02:17'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '1970-01-01 00:00:00.000000000', '-07:31')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1970-01-01 00:00:00.000 -07:31'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '1958-01-01 13:18:03.123000000', 'UTC')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123 Z'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '1958-01-01 13:18:03.123000000', 'Asia/Kathmandu')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123 Asia/Kathmandu'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '1958-01-01 13:18:03.123000000', '+02:17')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123 +02:17'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '1958-01-01 13:18:03.123000000', '-07:31')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '1958-01-01 13:18:03.123 -07:31'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '2019-03-18 10:01:17.987000000', 'UTC')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987 Z'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '2019-03-18 10:01:17.987000000', 'Asia/Kathmandu')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987 Asia/Kathmandu'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '2019-03-18 10:01:17.987000000', '+02:17')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987 +02:17'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", "from_tz(TIMESTAMP '2019-03-18 10:01:17.987000000', '-07:31')",
                        TIMESTAMP_TZ_MILLIS, "TIMESTAMP '2019-03-18 10:01:17.987 -07:31'")
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeDoubledInJvmZone.atZone(UTC)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInJvmZone.atZone(UTC)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeDoubledInJvmZone.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInJvmZone.atZone(kathmandu)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeDoubledInVilnius.atZone(UTC)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInVilnius.atZone(UTC)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeDoubledInVilnius.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeDoubledInVilnius.atZone(kathmandu)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeGapInJvmZone1.atZone(UTC)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone1.atZone(UTC)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeGapInJvmZone1.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone1.atZone(kathmandu)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeGapInJvmZone2.atZone(UTC)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone2.atZone(UTC)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeGapInJvmZone2.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInJvmZone2.atZone(kathmandu)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeGapInVilnius.atZone(kathmandu)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInVilnius.atZone(kathmandu)))
                .addRoundTrip("TIMESTAMP(3) WITH TIME ZONE", oracleTimestamp3TimeZoneDataType().toLiteral(timeGapInKathmandu.atZone(vilnius)),
                        TIMESTAMP_TZ_MILLIS, timestampWithTimeZoneDataType(3).toLiteral(timeGapInKathmandu.atZone(vilnius)))
                .execute(getQueryRunner(), oracleCreateAndInsert("timestamp_tz"));
    }

    /* Unsupported type tests */

    @Test
    public void testUnsupportedBasicType()
    {
        testUnsupportedOracleType("BFILE"); // Never in mapping
    }

    @Test
    public void testUnsupportedNumberScale()
    {
        // Difference between precision and negative scale greater than 38
        testUnsupportedOracleType("number(20, -20)");
        testUnsupportedOracleType("number(38, -84)");
        // Scale larger than precision.
        testUnsupportedOracleType("NUMBER(2, 4)"); // Explicitly removed from mapping
    }

    /* Testing utilities */

    /**
     * Check that unsupported data types are ignored
     */
    private void testUnsupportedOracleType(String dataTypeName)
    {
        try (TestTable table = new TestTable(getOracleSqlExecutor(), "unsupported_type", format("(unsupported_type %s)", dataTypeName))) {
            assertQueryFails("SELECT * FROM " + table.getName(), NO_SUPPORTED_COLUMNS);
        }
    }

    private DataSetup oracleCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getOracleSqlExecutor(), tableNamePrefix);
    }

    protected abstract SqlExecutor getOracleSqlExecutor();

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

    private TestTable oracleTable(String tableName, String schema, String data)
    {
        return new TestTable(getOracleSqlExecutor(), tableName, format("(%s)", schema), ImmutableList.of(data));
    }
}
