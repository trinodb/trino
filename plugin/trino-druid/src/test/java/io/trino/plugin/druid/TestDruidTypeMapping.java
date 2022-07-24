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
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
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
        //TODO Map Druid's Float data type to Trino's Real data type
        SqlDataTypeTest.create()
                .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                .addRoundTrip("col_0", "float", "NULL", DOUBLE, "DOUBLE '0.0'")
                .addRoundTrip("col_1", "float", "0.0", DOUBLE, "DOUBLE '0.0'")
                .addRoundTrip("col_2", "float", "3.14", DOUBLE, "DOUBLE '3.14'")
                .addRoundTrip("col_3", "float", "3.1415927", DOUBLE, "DOUBLE '3.1415927'")
                .addRoundTrip("col_4", "float", "Invalid String", DOUBLE, "DOUBLE '0.0'")
                .execute(getQueryRunner(), druidCreateAndInsert("test_float"));
    }

    @Test
    public void testFloatNaN()
    {
        //TODO Map Druid's Float data type to Trino's Real data type
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "float", "NaN", DOUBLE, "CAST(nan() AS double)")
                        .execute(getQueryRunner(), druidCreateAndInsert("test_float_with_nan")))
                .hasMessageMatching(".*class java.lang.String cannot be cast to class java.lang.Number.*");
    }

    @Test
    public void testFloatInfinity()
    {
        //TODO Map Druid's Float data type to Trino's Real data type
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "float", "Infinity", DOUBLE, "+infinity()")
                        .execute(getQueryRunner(), druidCreateAndInsert("test_float_with_positive_infinity")))
                .hasMessageMatching(".*class java.lang.String cannot be cast to class java.lang.Number.*");
    }

    @Test
    public void testFloatNegativeInfinity()
    {
        //TODO Map Druid's Float data type to Trino's Real data type
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("__time", "timestamp", "2020-01-01 00:00:00.000", TIMESTAMP_MILLIS, "TIMESTAMP '2020-01-01 00:00:00.000'")
                        .addRoundTrip("col_0", "double", "-Infinity", DOUBLE, "-infinity()")
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

    private DataSetup druidCreateAndInsert(String dataSourceName)
    {
        return new DruidCreateAndInsertDataSetup(druidServer, dataSourceName);
    }
}
