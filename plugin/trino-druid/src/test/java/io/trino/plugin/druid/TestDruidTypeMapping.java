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
import io.trino.plugin.druid.ingestion.IndexTaskBuilder;
import io.trino.plugin.druid.ingestion.TimestampSpec;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.druid.DruidQueryRunner.createDruidQueryRunnerTpch;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
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
        return createDruidQueryRunnerTpch(druidServer, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of());
    }

    @AfterAll
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

    @Test
    public void testTimestamp()
            throws Exception
    {
        int id = 1;
        List<TimestampCase> rows = ImmutableList.<TimestampCase>builder()
                // before epoch
                .add(new TimestampCase("1958-01-01 13:18:03.123", "TIMESTAMP '1958-01-01 13:18:03.123'", id++))
                // after epoch
                .add(new TimestampCase("2019-03-18 10:01:17.987", "TIMESTAMP '2019-03-18 10:01:17.987'", id++))
                // time doubled in JVM zone
                .add(new TimestampCase("2018-10-28 01:33:17.456", "TIMESTAMP '2018-10-28 01:33:17.456'", id++))
                // time doubled in JVM zone
                .add(new TimestampCase("2018-10-28 03:33:33.333", "TIMESTAMP '2018-10-28 03:33:33.333'", id++))
                // epoch
                .add(new TimestampCase("1970-01-01 00:00:00.000", "TIMESTAMP '1970-01-01 00:00:00.000'", id++))
                // time gap in JVM zone
                .add(new TimestampCase("1970-01-01 00:13:42.000", "TIMESTAMP '1970-01-01 00:13:42.000'", id++))
                .add(new TimestampCase("2018-04-01 02:13:55.123", "TIMESTAMP '2018-04-01 02:13:55.123'", id++))
                // time gap in Vilnius
                .add(new TimestampCase("2018-03-25 03:17:17.000", "TIMESTAMP '2018-03-25 03:17:17.000'", id++))
                // time gap in Kathmandu
                .add(new TimestampCase("1986-01-01 00:13:07.000", "TIMESTAMP '1986-01-01 00:13:07.000'", id++))
                // test arbitrary time for all supported precisions
                .add(new TimestampCase("1970-01-01 00:00:01", "TIMESTAMP '1970-01-01 00:00:01.000'", id++))
                .add(new TimestampCase("1970-01-01 00:00:02.1", "TIMESTAMP '1970-01-01 00:00:02.100'", id++))
                .add(new TimestampCase("1970-01-01 00:00:03.12", "TIMESTAMP '1970-01-01 00:00:03.120'", id++))
                .add(new TimestampCase("1970-01-01 00:00:04.123", "TIMESTAMP '1970-01-01 00:00:04.123'", id++))
                .add(new TimestampCase("1970-01-01 00:00:05.1239", "TIMESTAMP '1970-01-01 00:00:05.123'", id++))
                .add(new TimestampCase("1970-01-01 00:00:06.12399", "TIMESTAMP '1970-01-01 00:00:06.123'", id++))
                .add(new TimestampCase("1970-01-01 00:00:07.123999", "TIMESTAMP '1970-01-01 00:00:07.123'", id++))
                .add(new TimestampCase("1970-01-01 00:00:08.1239999", "TIMESTAMP '1970-01-01 00:00:08.123'", id++))
                .add(new TimestampCase("1970-01-01 00:00:09.12399999", "TIMESTAMP '1970-01-01 00:00:09.123'", id++))
                .add(new TimestampCase("1970-01-01 00:00:00.123999999", "TIMESTAMP '1970-01-01 00:00:00.123'", id++))
                // before epoch with second fraction
                .add(new TimestampCase("1969-12-31 23:59:59.1230000", "TIMESTAMP '1969-12-31 23:59:59.123'", id))
                .build();

        try (DruidTable testTable = new DruidTable("test_timestamp")) {
            String dataFilePath = format("%s/%s.tsv", druidServer.getHostWorkingDirectory(), testTable.getName());
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(dataFilePath, UTF_8))) {
                for (TimestampCase row : rows) {
                    writer.write("%s\t%s".formatted(row.inputLiteral, row.id));
                    writer.newLine();
                }
            }
            String dataSource = testTable.getName();
            IndexTaskBuilder builder = new IndexTaskBuilder();
            builder.setDatasource(dataSource);
            TimestampSpec timestampSpec = new TimestampSpec("dummy_druid_ts", "auto");
            builder.setTimestampSpec(timestampSpec);
            builder.addColumn("id", "long");
            druidServer.ingestData(testTable.getName(), Optional.empty(), builder.build(), dataFilePath);

            for (TimestampCase row : rows) {
                assertThat(query("SELECT __time FROM druid.druid." + testTable.getName() + " WHERE id = " + row.id))
                        .as("input: %s, expected: %s, id: %s", row.inputLiteral, row.expectedLiteral, row.id)
                        .matches("VALUES " + row.expectedLiteral);
                assertThat(query("SELECT id FROM druid.druid." + testTable.getName() + " WHERE __time = " + row.expectedLiteral))
                        .as("input: %s, expected: %s, id: %s", row.inputLiteral, row.expectedLiteral, row.id)
                        .matches("VALUES BIGINT '" + row.id + "'");
            }
        }
    }

    private record TimestampCase(String inputLiteral, String expectedLiteral, int id)
    {
        private TimestampCase
        {
            requireNonNull(inputLiteral, "inputLiteral is null");
            requireNonNull(expectedLiteral, "expectedLiteral is null");
        }
    }

    private DataSetup druidCreateAndInsert(String dataSourceName)
    {
        return new DruidCreateAndInsertDataSetup(druidServer, dataSourceName);
    }
}
