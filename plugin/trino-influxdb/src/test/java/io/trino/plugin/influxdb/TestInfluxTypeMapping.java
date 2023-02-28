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
package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import org.testng.annotations.Test;

import static io.trino.plugin.influxdb.InfluxQueryRunner.createInfluxQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestInfluxTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingInfluxServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingInfluxServer());
        return createInfluxQueryRunner(server, ImmutableList.of());
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-01T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-01 00:00:00' as timestamp(9))")
                .addRoundTrip("col_1", "boolean", "true", BOOLEAN, "true")
                .addRoundTrip("col_2", "boolean", "false", BOOLEAN, "false")
                .execute(getQueryRunner(), influxCreateAndInsert("test_boolean"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-01T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-01 00:00:00' as timestamp(9))")
                .addRoundTrip("col_1", "integer", "-128", BIGINT, "CAST('-128' as bigint)")
                .addRoundTrip("col_2", "integer", "5", BIGINT, "CAST('5' as bigint)")
                .addRoundTrip("col_3", "integer", "127", BIGINT, "CAST('127' as bigint)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_integer"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-02T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-02 00:00:00' as timestamp(9))")
                .addRoundTrip("col_1", "integer", "-32768", BIGINT, "CAST('-32768' as bigint)")
                .addRoundTrip("col_2", "integer", "32456", BIGINT, "CAST('32456' as bigint)")
                .addRoundTrip("col_3", "integer", "32767", BIGINT, "CAST('32767' as bigint)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_integer"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-03T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-03 00:00:00' as timestamp(9))")
                .addRoundTrip("col_1", "integer", "-2147483648", BIGINT, "CAST('-2147483648' as bigint)")
                .addRoundTrip("col_2", "integer", "1234567890", BIGINT, "CAST('1234567890' as bigint)")
                .addRoundTrip("col_3", "integer", "2147483647", BIGINT, "CAST('2147483647' as bigint)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_integer"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-04T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-04 00:00:00' as timestamp(9))")
                .addRoundTrip("col_1", "integer", "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip("col_2", "integer", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("col_3", "integer", "9223372036854775807", BIGINT, "9223372036854775807")
                .execute(getQueryRunner(), influxCreateAndInsert("test_integer"));
    }

    @Test
    public void testFloat()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-01T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-01 00:00:00' as timestamp(9))")
                .addRoundTrip("col_1", "float", "-193", DOUBLE, "CAST('-193' as double)")
                .addRoundTrip("col_2", "float", "-10.1", DOUBLE, "CAST('-10.1' as double)")
                .addRoundTrip("col_3", "float", "123456789.3", DOUBLE, "CAST('123456789.3' as double)")
                .addRoundTrip("col_4", "float", "12345678901234567890.31", DOUBLE, "CAST('12345678901234567890.31' as double)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_float"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-02T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-02 00:00:00' as timestamp(9))")
                .addRoundTrip("col_1", "float", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("col_2", "float", "123.456E10", DOUBLE, "123.456E10")
                .execute(getQueryRunner(), influxCreateAndInsert("test_float"));
    }

    @Test
    public void testString()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-01T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-01 00:00:00' as timestamp(9))")
                .addRoundTrip("col_1", "string", "text_a", VARCHAR, "CAST('text_a' as varchar)")
                .addRoundTrip("col_2", "string", "12345678", VARCHAR, "CAST('12345678' as varchar)")
                .addRoundTrip("col_3", "string", "攻殻機動隊", VARCHAR, "CAST('攻殻機動隊' as varchar)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_string"));
    }

    @Test
    public void testTimestamp()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-01T00:00:00Z", TIMESTAMP_NANOS, "CAST('2020-01-01 00:00:00' as timestamp(9))")
                .addRoundTrip("string", "required", VARCHAR, "CAST('required' as varchar)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_timestamp"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-02T00:00:00.000Z", TIMESTAMP_NANOS, "CAST('2020-01-02 00:00:00' as timestamp(9))")
                .addRoundTrip("string", "required", VARCHAR, "CAST('required' as varchar)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_timestamp"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-03T00:00:00.000000Z", TIMESTAMP_NANOS, "CAST('2020-01-03 00:00:00' as timestamp(9))")
                .addRoundTrip("string", "required", VARCHAR, "CAST('required' as varchar)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_timestamp"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-04T00:00:00.000000000Z", TIMESTAMP_NANOS, "CAST('2020-01-04 00:00:00' as timestamp(9))")
                .addRoundTrip("string", "required", VARCHAR, "CAST('required' as varchar)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_timestamp"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-01T00:00:00.123Z", TIMESTAMP_NANOS, "CAST('2020-01-01 00:00:00.123' as timestamp(9))")
                .addRoundTrip("string", "required", VARCHAR, "CAST('required' as varchar)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_timestamp"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-01T00:00:00.123456Z", TIMESTAMP_NANOS, "CAST('2020-01-01 00:00:00.123456' as timestamp(9))")
                .addRoundTrip("string", "required", VARCHAR, "CAST('required' as varchar)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_timestamp"));

        SqlDataTypeTest.create()
                .addRoundTrip("time", "timestamp", "2020-01-01T00:00:00.123456789Z", TIMESTAMP_NANOS, "CAST('2020-01-01 00:00:00.123456789' as timestamp(9))")
                .addRoundTrip("string", "required", VARCHAR, "CAST('required' as varchar)")
                .execute(getQueryRunner(), influxCreateAndInsert("test_timestamp"));
    }

    private DataSetup influxCreateAndInsert(String tableNamePrefix)
    {
        return new InfluxDataSetup(new InfluxSession(server.getEndpoint()), tableNamePrefix);
    }
}
