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
package io.prestosql.plugin.memsql;

import io.prestosql.Session;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.datatype.CreateAndInsertDataSetup;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import io.prestosql.testing.sql.SqlExecutor;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static io.prestosql.plugin.memsql.MemSqlQueryRunner.createMemSqlQueryRunner;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.datatype.DataType.bigintDataType;
import static io.prestosql.testing.datatype.DataType.charDataType;
import static io.prestosql.testing.datatype.DataType.dataType;
import static io.prestosql.testing.datatype.DataType.decimalDataType;
import static io.prestosql.testing.datatype.DataType.doubleDataType;
import static io.prestosql.testing.datatype.DataType.integerDataType;
import static io.prestosql.testing.datatype.DataType.smallintDataType;
import static io.prestosql.testing.datatype.DataType.stringDataType;
import static io.prestosql.testing.datatype.DataType.tinyintDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static java.lang.String.format;

public class TestMemSqlTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String CHARACTER_SET_UTF8 = "CHARACTER SET utf8";

    protected TestingMemSqlServer memSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        memSqlServer = new TestingMemSqlServer();
        return createMemSqlQueryRunner(memSqlServer);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        memSqlServer.close();
    }

    @Test
    public void testBasicTypes()
    {
        DataTypeTest.create()
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(tinyintDataType(), (byte) 125)
                .addRoundTrip(doubleDataType(), 123.45d)
                // TODO (https://github.com/prestosql/presto/issues/5452) .addRoundTrip(realDataType(), 123.45f)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testFloat()
    {
        // TODO (https://github.com/prestosql/presto/issues/5452)
//        singlePrecisionFloatingPointTests(realDataType())
//                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_float"));
        singlePrecisionFloatingPointTests(memSqlFloatDataType())
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_float"));
    }

    private static DataTypeTest singlePrecisionFloatingPointTests(DataType<Float> floatType)
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MemSQL
        return DataTypeTest.create()
                .addRoundTrip(floatType, 3.14f)
                // .addRoundTrip(floatType, 3.1415927f) // Overeagerly rounded by MemSQL to 3.14159 TODO
                .addRoundTrip(floatType, null);
    }

    @Test
    public void testDouble()
    {
        doublePrecisionFloatingPointTests(doubleDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_double"));
        doublePrecisionFloatingPointTests(memSqlDoubleDataType())
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_double"));
    }

    private static DataTypeTest doublePrecisionFloatingPointTests(DataType<Double> doubleType)
    {
        // we are not testing Nan/-Infinity/+Infinity as those are not supported by MemSQL
        return DataTypeTest.create()
                .addRoundTrip(doubleType, 1.0e100d)
                .addRoundTrip(doubleType, null);
    }

    @Test
    public void testUnsignedTypes()
    {
        // TODO (https://github.com/prestosql/presto/issues/5453)
        throw new SkipException("TODO");
//        DataType<Short> memSqlUnsignedTinyInt = DataType.dataType("TINYINT UNSIGNED", SmallintType.SMALLINT, Objects::toString);
//        DataType<Integer> memSqlUnsignedSmallInt = DataType.dataType("SMALLINT UNSIGNED", IntegerType.INTEGER, Objects::toString);
//        DataType<Long> memSqlUnsignedInt = DataType.dataType("INT UNSIGNED", BigintType.BIGINT, Objects::toString);
//        DataType<Long> memSqlUnsignedInteger = DataType.dataType("INTEGER UNSIGNED", BigintType.BIGINT, Objects::toString);
//        DataType<BigDecimal> memSqlUnsignedBigint = DataType.dataType("BIGINT UNSIGNED", createDecimalType(20), Objects::toString);
//
//        DataTypeTest.create()
//                .addRoundTrip(memSqlUnsignedTinyInt, (short) 255)
//                .addRoundTrip(memSqlUnsignedSmallInt, 65_535)
//                .addRoundTrip(memSqlUnsignedInt, 4_294_967_295L)
//                .addRoundTrip(memSqlUnsignedInteger, 4_294_967_295L)
//                .addRoundTrip(memSqlUnsignedBigint, new BigDecimal("18446744073709551615"))
//                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_unsigned"));
    }

    @Test
    public void testMemsqlCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testPrestoCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), prestoCreateAsSelect("test_decimal"));
    }

    private DataTypeTest decimalTests()
    {
        return DataTypeTest.create()
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("193"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("19"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("-193"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.0"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(decimalDataType(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("-27182818284590452353602874713526624977"));
    }

    @Test
    public void testDecimalExceedingPrecisionMax()
    {
        testUnsupportedDataType("decimal(50,0)");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithExceedingIntegerValues()
    {
        // TODO determine whether we need decimal_rounding_mode session property
        throw new SkipException("TODO");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithNonExceedingIntegerValues()
    {
        // TODO determine whether we need decimal_rounding_mode session property
        throw new SkipException("TODO");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithSupportedValues()
    {
        // TODO determine whether we need decimal_rounding_mode session property
        throw new SkipException("TODO");
    }

    @Test
    public void testPrestoCreatedParameterizedChar()
    {
        memSqlCharTypeTest()
                .execute(getQueryRunner(), prestoCreateAsSelect("memsql_test_parameterized_char"));
    }

    @Test
    public void testMemSqlCreatedParameterizedChar()
    {
        memSqlCharTypeTest()
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_char"));
    }

    private DataTypeTest memSqlCharTypeTest()
    {
        return DataTypeTest.create()
                .addRoundTrip(charDataType("char", 1), "")
                .addRoundTrip(charDataType("char", 1), "a")
                .addRoundTrip(charDataType(1), "")
                .addRoundTrip(charDataType(1), "a")
                .addRoundTrip(charDataType(8), "abc")
                .addRoundTrip(charDataType(8), "12345678")
                .addRoundTrip(charDataType(255), "a".repeat(255));
    }

    @Test
    public void testMemSqlCreatedParameterizedCharUnicode()
    {
        DataTypeTest.create()
                .addRoundTrip(charDataType(1, CHARACTER_SET_UTF8), "\u653b")
                .addRoundTrip(charDataType(5, CHARACTER_SET_UTF8), "\u653b\u6bbb")
                .addRoundTrip(charDataType(5, CHARACTER_SET_UTF8), "\u653b\u6bbb\u6a5f\u52d5\u968a")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar"));
    }

    @Test
    public void testPrestoCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(varcharDataType(10), "text_a")
                .addRoundTrip(varcharDataType(255), "text_b")
                .addRoundTrip(varcharDataType(256), "text_c")
                // TODO https://github.com/prestosql/presto/issues/5454
//                .addRoundTrip(varcharDataType(65535), "text_d")
//                .addRoundTrip(varcharDataType(16777215), "text_e")
//                .addRoundTrip(varcharDataType(16777215), "text_f")
//                .addRoundTrip(varcharDataType(16777216), "text_g")
//                .addRoundTrip(varcharDataType(VarcharType.MAX_LENGTH), "text_h")
//                .addRoundTrip(varcharDataType(), "unbounded")
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_parameterized_varchar"));
    }

    @Test
    public void testMemSqlCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext", createVarcharType(255)), "a")
                .addRoundTrip(stringDataType("text", createVarcharType(65535)), "b")
                .addRoundTrip(stringDataType("mediumtext", createVarcharType(16777215)), "c")
                .addRoundTrip(stringDataType("longtext", createUnboundedVarcharType()), "d")
                .addRoundTrip(varcharDataType(32), "e")
                .addRoundTrip(varcharDataType(15000), "f")
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar"));
    }

    @Test
    public void testMemSqlCreatedParameterizedVarcharUnicode()
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext " + CHARACTER_SET_UTF8, createVarcharType(255)), sampleUnicodeText)
                .addRoundTrip(stringDataType("text " + CHARACTER_SET_UTF8, createVarcharType(65535)), sampleUnicodeText)
                .addRoundTrip(stringDataType("mediumtext " + CHARACTER_SET_UTF8, createVarcharType(16777215)), sampleUnicodeText)
                .addRoundTrip(stringDataType("longtext " + CHARACTER_SET_UTF8, createUnboundedVarcharType()), sampleUnicodeText)
                .addRoundTrip(varcharDataType(sampleUnicodeText.length(), CHARACTER_SET_UTF8), sampleUnicodeText)
                .addRoundTrip(varcharDataType(32, CHARACTER_SET_UTF8), sampleUnicodeText)
                .addRoundTrip(varcharDataType(20000, CHARACTER_SET_UTF8), sampleUnicodeText)
                .execute(getQueryRunner(), memSqlCreateAndInsert("tpch.memsql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testDate()
    {
        // TODO
        throw new SkipException("TODO");
    }

    @Test
    public void testDatetime()
    {
        // TODO (https://github.com/prestosql/presto/issues/5450) MemSQL datetime is not correctly read (see comment in StandardColumnMappings.timestampColumnMappingUsingSqlTimestamp)
        throw new SkipException("TODO");
    }

    @Test
    public void testTimestamp()
    {
        // TODO (https://github.com/prestosql/presto/issues/5450) MemSQL timestamp is not correctly read (see comment in StandardColumnMappings.timestampColumnMappingUsingSqlTimestamp)
        throw new SkipException("TODO");
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        SqlExecutor jdbcSqlExecutor = memSqlServer::execute;
        jdbcSqlExecutor.execute(format("CREATE TABLE tpch.test_unsupported_data_type(supported_column varchar(5), unsupported_column %s)", databaseDataType));
        try {
            assertQuery(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'tpch' AND TABLE_NAME = 'test_unsupported_data_type'",
                    "VALUES 'supported_column'"); // no 'unsupported_column'
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_unsupported_data_type");
        }
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return prestoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup prestoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup memSqlCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(memSqlServer::execute, tableNamePrefix);
    }

    private static DataType<Float> memSqlFloatDataType()
    {
        return dataType("float", RealType.REAL, Object::toString);
    }

    private static DataType<Double> memSqlDoubleDataType()
    {
        return dataType("double precision", DoubleType.DOUBLE, Object::toString);
    }
}
