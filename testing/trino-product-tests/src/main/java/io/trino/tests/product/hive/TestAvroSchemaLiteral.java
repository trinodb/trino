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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.tempto.query.QueryExecutionException;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAvroSchemaLiteral
        extends HiveProductTest
{
    @Language("JSON")
    private static final String SCHEMA_LITERAL =
            """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col", "type":"int" }
              ]
            }
            """;

    @Test
    public void testHiveCreatedTable()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_schema_literal_hive");
        onHive().executeQuery(format("" +
                        "CREATE TABLE test_avro_schema_literal_hive " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                        "TBLPROPERTIES ('avro.schema.literal'='%s')",
                SCHEMA_LITERAL));
        onHive().executeQuery("INSERT INTO test_avro_schema_literal_hive VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_literal_hive")).containsExactlyInOrder(row("some text", 123042));
        assertThat(onTrino().executeQuery("SELECT * FROM test_avro_schema_literal_hive")).containsExactlyInOrder(row("some text", 123042));

        onHive().executeQuery("DROP TABLE test_avro_schema_literal_hive");
    }

    @Test
    public void testTrinoCreatedTable()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_avro_schema_literal_trino");
        onTrino().executeQuery(format("CREATE TABLE test_avro_schema_literal_trino (dummy_col VARCHAR) WITH (format='AVRO', avro_schema_literal='%s')", SCHEMA_LITERAL));
        onTrino().executeQuery("INSERT INTO test_avro_schema_literal_trino VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_literal_trino")).containsExactlyInOrder(row("some text", 123042));
        assertThat(onTrino().executeQuery("SELECT * FROM test_avro_schema_literal_trino")).containsExactlyInOrder(row("some text", 123042));

        onTrino().executeQuery("DROP TABLE test_avro_schema_literal_trino");
    }

    @Test(dataProvider =  "timestampPrecisionTestCases")
    public void testTimestampPrecisionMapping(TimestampPrecisionTestCase timestampPrecisionTestCase)
    {
        HiveTimestampPrecision sessionTimestampPrecision = timestampPrecisionTestCase.sessionTimestampPrecision();
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", timestampPrecisionTestCase.tableName()));
        onTrino().executeQuery(format("SET SESSION hive.timestamp_precision = '%s'", sessionTimestampPrecision.name()));

        String createTableSql = format("CREATE TABLE %s (%s %s) WITH (format='AVRO', avro_schema_literal='%s')",
                timestampPrecisionTestCase.tableName(),
                timestampPrecisionTestCase.trinoColumnName(),
                timestampPrecisionTestCase.trinoCreateType(),
                format("""
                        {
                          "namespace": "io.trino.test",
                          "name": "product_tests_avro_table_%s",
                          "type": "record",
                          "fields": [
                            { "name":"%s", "type":{"type":"long","logicalType":"%s"} }
                          ]
                        }""", timestampPrecisionTestCase.trinoColumnName(), timestampPrecisionTestCase.trinoColumnName(), timestampPrecisionTestCase.avroLogicalType()));
        if (timestampPrecisionTestCase.trinoReadType().isEmpty()) {
            assertThatThrownBy(() -> onTrino().executeQuery(createTableSql))
                    .isInstanceOf(QueryExecutionException.class)
                    .hasMessageContaining("Incorrect timestamp precision for %s; the configured precision is %s; column name: %s".formatted(timestampPrecisionTestCase.trinoCreateType().toLowerCase(Locale.ENGLISH), sessionTimestampPrecision.name(), timestampPrecisionTestCase.trinoColumnName()));
            return;
        }

        onTrino().executeQuery(createTableSql);
        assertThat(onTrino().executeQuery(format("SHOW COLUMNS FROM %s", timestampPrecisionTestCase.tableName())))
                .containsExactlyInOrder(row(timestampPrecisionTestCase.trinoColumnName(), timestampPrecisionTestCase.trinoReadType().get().toLowerCase(Locale.ENGLISH), "", ""));
    }

    @DataProvider
    public TimestampPrecisionTestCase[] timestampPrecisionTestCases()
    {
        ImmutableList.Builder<TimestampPrecisionTestCase> testCases = ImmutableList.builder();
        HiveTimestampPrecision[] sessionPrecisionValues = assertThat(HiveTimestampPrecision.values())
                .hasSize(3)
                .actual();
        for (HiveTimestampPrecision hiveTimestampPrecision : sessionPrecisionValues) {
            testCases.add(new TimestampPrecisionTestCase(
                    hiveTimestampPrecision,
                    "timestamp-millis",
                    "ts_millis",
                    "TIMESTAMP(3)",
                    hiveTimestampPrecision.equals(HiveTimestampPrecision.MILLISECONDS) ? Optional.of("TIMESTAMP(3)") : Optional.empty()));
            testCases.add(new TimestampPrecisionTestCase(
                    hiveTimestampPrecision,
                    "timestamp-micros",
                    "ts_micros",
                    "TIMESTAMP(6)",
                    hiveTimestampPrecision.equals(HiveTimestampPrecision.MICROSECONDS) ? Optional.of("BIGINT") : Optional.empty()));
            testCases.add(new TimestampPrecisionTestCase(
                    hiveTimestampPrecision,
                    "timestamp-nanos",
                    "ts_nanos",
                    "TIMESTAMP(9)",
                    hiveTimestampPrecision.equals(HiveTimestampPrecision.NANOSECONDS) ? Optional.of("BIGINT") : Optional.empty()));
        }
        return testCases.build().toArray(TimestampPrecisionTestCase[]::new);
    }

    public record TimestampPrecisionTestCase(
            HiveTimestampPrecision sessionTimestampPrecision,
            String avroLogicalType,
            String trinoColumnName,
            String trinoCreateType,
            Optional<String> trinoReadType)
    {
        public String tableName()
        {
            return format("test_avro_schema_literal_trino_%s_%s", trinoColumnName, sessionTimestampPrecision.name());
        }
    }
}
