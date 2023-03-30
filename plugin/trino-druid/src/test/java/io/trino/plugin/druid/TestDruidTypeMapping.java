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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDruidTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String DRUID_DOCKER_IMAGE = "apache/druid:0.18.0";
    protected TestingDruidServer druidServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.druidServer = new TestingDruidServer(DRUID_DOCKER_IMAGE);
        return DruidQueryRunner.createDruidQueryRunnerTpch(druidServer, ImmutableMap.of(), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        druidServer.close();
        druidServer = null;
    }

    @Test
    public void testLong()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                .addRoundTrip("col_0", "long", "NULL", BIGINT, "BIGINT '0'")
                .addRoundTrip("col_1", "long", "0", BIGINT, "BIGINT '0'")
                .addRoundTrip("col_2", "long", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in Trino
                .addRoundTrip("col_3", "long", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("col_4", "long", "9223372036854775807", BIGINT, "9223372036854775807") // max value in Trino
                .execute(getQueryRunner(), druidCreateAndInsert("test_bigint"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                .addRoundTrip("col_0", "double", "NULL", DOUBLE, "DOUBLE '0.0'")
                .addRoundTrip("col_1", "double", "0.0", DOUBLE, "DOUBLE '0.0'")
                .addRoundTrip("col_2", "double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("col_3", "double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("col_4", "double", "Invalid String", DOUBLE, "DOUBLE '0.0'")
                .execute(getQueryRunner(), druidCreateAndInsert("test_double"));
    }

    @Test
    public void testDoubleNaN()
    {
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "double", "NaN", DOUBLE, "nan()")
                        .execute(getQueryRunner(), druidCreateAndInsert("test_double_with_nan")))
                .hasMessageMatching(".*class java.lang.String cannot be cast to class java.lang.Number.*");
    }

    @Test
    public void testDoubleInfinity()
    {
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "double", "Infinity", DOUBLE, "+infinity()")
                        .execute(getQueryRunner(), druidCreateAndInsert("test_double_with_positive_infinity")))
                .hasMessageMatching(".*class java.lang.String cannot be cast to class java.lang.Number.*");
    }

    @Test
    public void testDoubleNegativeInfinity()
    {
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "double", "-Infinity", DOUBLE, "-infinity()")
                        .execute(getQueryRunner(), druidCreateAndInsert("test_double_with_negative_infinity")))
                .hasMessageMatching(".*class java.lang.String cannot be cast to class java.lang.Number.*");
    }

    @Test
    public void testFloat()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                .addRoundTrip("col_0", "float", "NULL", REAL, "REAL '0.0'")
                .addRoundTrip("col_1", "float", "0.0", REAL, "REAL '0.0'")
                .addRoundTrip("col_2", "float", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("col_3", "float", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("col_4", "float", "Invalid String", REAL, "REAL '0.0'")
                .execute(getQueryRunner(), druidCreateAndInsert("test_float"));
    }

    @Test
    public void testFloatNaN()
    {
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "float", "NaN", REAL, "CAST(nan() AS real)")
                        .execute(getQueryRunner(), druidCreateAndInsert("test_float_with_nan")))
                .hasMessageMatching(".*class java.lang.String cannot be cast to class java.lang.Number.*");
    }

    @Test
    public void testFloatInfinity()
    {
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "float", "Infinity", REAL, "+infinity()")
                        .execute(getQueryRunner(), druidCreateAndInsert("test_float_with_positive_infinity")))
                .hasMessageMatching(".*class java.lang.String cannot be cast to class java.lang.Number.*");
    }

    @Test
    public void testFloatNegativeInfinity()
    {
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "float", "-Infinity", REAL, "-infinity()")
                        .execute(getQueryRunner(), druidCreateAndInsert("test_float_with_negative_infinity")))
                .hasMessageMatching(".*class java.lang.String cannot be cast to class java.lang.Number.*");
    }

    @Test
    public void testVarchar()
    {
        //TODO Add test for unicode characters
        SqlDataTypeTest.create()
                .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                .addRoundTrip("col_0", "string", "null", createUnboundedVarcharType(), "CAST('null' AS varchar)")
                .addRoundTrip("col_1", "string", "NULL", createUnboundedVarcharType(), "CAST('NULL' AS varchar)")
                .addRoundTrip("col_2", "string", "text_a", createUnboundedVarcharType(), "CAST('text_a' AS varchar)")
                .addRoundTrip("col_3", "string", "text_b", createUnboundedVarcharType(), "CAST('text_b' AS varchar)")
                .execute(getQueryRunner(), druidCreateAndInsert("test_unbounded_varchar"));
    }

    @Test(dataProvider = "timestampValuesProvider")
    public void testTimestamp(String inputLiteral, String expectedLiteral)
    {
        SqlDataTypeTest.create()
                .addRoundTrip("__time", "timestamp", inputLiteral, TIMESTAMP_MILLIS, expectedLiteral)
                .addRoundTrip("col_0", "long", "0", BIGINT, "BIGINT '0'")
                .execute(getQueryRunner(), druidCreateAndInsert("test_timestamp"));
    }

    @DataProvider
    public Object[][] timestampValuesProvider()
    {
        return new Object[][] {
                //before epoch
                {"1958-01-01 13:18:03.123", "TIMESTAMP '1958-01-01 13:18:03.123'"},
                // after epoch
                {"2019-03-18 10:01:17.987", "TIMESTAMP '2019-03-18 10:01:17.987'"},
                // time doubled in JVM zone
                {"2018-10-28 01:33:17.456", "TIMESTAMP '2018-10-28 01:33:17.456'"},
                // time doubled in JVM zone
                {"2018-10-28 03:33:33.333", "TIMESTAMP '2018-10-28 03:33:33.333'"},
                // epoch
                {"1970-01-01 00:00:00.000", "TIMESTAMP '1970-01-01 00:00:00.000'"},
                // time gap in JVM zone
                {"1970-01-01 00:13:42.000", "TIMESTAMP '1970-01-01 00:13:42.000'"},
                {"2018-04-01 02:13:55.123", "TIMESTAMP '2018-04-01 02:13:55.123'"},
                // time gap in Vilnius
                {"2018-03-25 03:17:17.000", "TIMESTAMP '2018-03-25 03:17:17.000'"},
                // time gap in Kathmandu
                {"1986-01-01 00:13:07.000", "TIMESTAMP '1986-01-01 00:13:07.000'"},
                // test arbitrary time for all supported precisions
                {"1970-01-01 00:00:00", "TIMESTAMP '1970-01-01 00:00:00.000'"},
                {"1970-01-01 00:00:00.1", "TIMESTAMP '1970-01-01 00:00:00.100'"},
                {"1970-01-01 00:00:00.12", "TIMESTAMP '1970-01-01 00:00:00.120'"},
                {"1970-01-01 00:00:00.123", "TIMESTAMP '1970-01-01 00:00:00.123'"},
                {"1970-01-01 00:00:00.1239", "TIMESTAMP '1970-01-01 00:00:00.123'"},
                {"1970-01-01 00:00:00.12399", "TIMESTAMP '1970-01-01 00:00:00.123'"},
                {"1970-01-01 00:00:00.123999", "TIMESTAMP '1970-01-01 00:00:00.123'"},
                {"1970-01-01 00:00:00.1239999", "TIMESTAMP '1970-01-01 00:00:00.123'"},
                {"1970-01-01 00:00:00.12399999", "TIMESTAMP '1970-01-01 00:00:00.123'"},
                {"1970-01-01 00:00:00.123999999", "TIMESTAMP '1970-01-01 00:00:00.123'"},
                // before epoch with second fraction
                {"1969-12-31 23:59:59.1230000", "TIMESTAMP '1969-12-31 23:59:59.123'"}
        };
    }

    private DataSetup druidCreateAndInsert(String dataSourceName)
    {
        return new DruidCreateAndInsertDataSetup(druidServer, dataSourceName);
    }
}
