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

import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Avro tables with schema URL.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestAvroSchemaUrl
{
    private static final String WAREHOUSE_PATH = "/user/hive/warehouse/TestAvroSchemaUrl";
    private static final String SCHEMAS_PATH = WAREHOUSE_PATH + "/schemas";
    private static final String DATA_PATH = WAREHOUSE_PATH + "/data";

    // Avro schema content embedded as strings
    private static final String ORIGINAL_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col", "type":"int" }
              ]
            }""";

    private static final String CAMEL_CASE_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"stringCol", "type":"string"},
                { "name":"intCol", "type":"int" }
              ]
            }""";

    private static final String CHANGE_COLUMN_TYPE_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col", "type":"long"}
              ]
            }""";

    @BeforeEach
    void setup(HiveStorageFormatsEnvironment env)
            throws IOException
    {
        HdfsClient hdfsClient = env.createHdfsClient();

        hdfsClient.createDirectory(SCHEMAS_PATH);
        hdfsClient.saveFile(SCHEMAS_PATH + "/original_schema.avsc", ORIGINAL_SCHEMA);
        hdfsClient.saveFile(SCHEMAS_PATH + "/camelCaseSchema.avsc", CAMEL_CASE_SCHEMA);
        saveResourceOnHdfs(hdfsClient, "column_with_long_type_definition_schema.avsc", SCHEMAS_PATH + "/column_with_long_type_definition_schema.avsc");

        hdfsClient.createDirectory(DATA_PATH);
    }

    @AfterEach
    void cleanup(HiveStorageFormatsEnvironment env)
    {
        try {
            env.createHdfsClient().delete(WAREHOUSE_PATH);
        }
        catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    static Stream<String> avroSchemaLocations()
    {
        return Stream.of(
                // hdfs://hadoop-master:9000 paths won't work in testcontainers setup
                // file:// paths also won't work as the schema file is not mounted
                // We test with HDFS paths that are available in our setup
                "hdfs:///user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc",
                "/user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc" // avro.schema.url can be path on HDFS (not URL)
        );
    }

    @ParameterizedTest
    @MethodSource("avroSchemaLocations")
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHiveCreatedTable(String schemaLocation, HiveStorageFormatsEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_avro_schema_url_hive");
        env.executeHiveUpdate(format("" +
                        "CREATE TABLE test_avro_schema_url_hive " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                        "TBLPROPERTIES ('avro.schema.url'='%s')",
                schemaLocation));
        env.executeHiveUpdate("INSERT INTO test_avro_schema_url_hive VALUES ('some text', 123042)");

        try {
            assertThat(env.executeHive("SELECT * FROM test_avro_schema_url_hive")).containsExactlyInOrder(row("some text", 123042));
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_avro_schema_url_hive")).containsExactlyInOrder(row("some text", 123042));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_avro_schema_url_hive");
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testAvroSchemaUrlInSerdeProperties(HiveStorageFormatsEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_avro_schema_url_in_serde_properties");

        String schemaLocationOnHdfs = SCHEMAS_PATH + "/test_avro_schema_url_in_serde_properties.avsc";
        env.createHdfsClient().saveFile(schemaLocationOnHdfs, ORIGINAL_SCHEMA);

        env.executeHiveUpdate(format("" +
                        "CREATE TABLE test_avro_schema_url_in_serde_properties " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                        "WITH SERDEPROPERTIES ('avro.schema.url'='%s')" +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' ",
                schemaLocationOnHdfs));

        try {
            assertThat(env.executeTrino("SHOW COLUMNS FROM hive.default.test_avro_schema_url_in_serde_properties"))
                    .containsExactlyInOrder(
                            row("string_col", "varchar", "", ""),
                            row("int_col", "integer", "", ""));

            assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE hive.default.test_avro_schema_url_in_serde_properties ADD COLUMN new_dummy_col varchar"))
                    .hasMessageContaining("ALTER TABLE not supported when Avro schema url is set");

            env.executeHiveUpdate("INSERT INTO test_avro_schema_url_in_serde_properties VALUES ('some text', 2147483635)");

            // Hive stores initial schema inferred from schema files in the Metastore DB.
            // We need to change the schema to test that current schema is used by Trino, not a snapshot saved during CREATE TABLE.
            env.createHdfsClient().saveFile(schemaLocationOnHdfs, CHANGE_COLUMN_TYPE_SCHEMA);

            assertThat(env.executeTrino("SHOW COLUMNS FROM hive.default.test_avro_schema_url_in_serde_properties"))
                    .containsExactlyInOrder(
                            row("string_col", "varchar", "", ""),
                            row("int_col", "bigint", "", ""));

            assertThat(env.executeTrino("SELECT * FROM hive.default.test_avro_schema_url_in_serde_properties"))
                    .containsExactlyInOrder(row("some text", 2147483635L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_avro_schema_url_in_serde_properties");
        }
    }

    @ParameterizedTest
    @MethodSource("avroSchemaLocations")
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testTrinoCreatedTable(String schemaLocation, HiveStorageFormatsEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_avro_schema_url_trino");
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.test_avro_schema_url_trino (dummy_col VARCHAR) WITH (format='AVRO', avro_schema_url='%s')", schemaLocation));

        try {
            env.executeTrinoUpdate("INSERT INTO hive.default.test_avro_schema_url_trino VALUES ('some text', 123042)");

            assertThat(env.executeHive("SELECT * FROM test_avro_schema_url_trino")).containsExactlyInOrder(row("some text", 123042));
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_avro_schema_url_trino")).containsExactlyInOrder(row("some text", 123042));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_avro_schema_url_trino");
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testTableWithLongColumnType(HiveStorageFormatsEnvironment env)
            throws IOException
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        saveResourceOnHdfs(hdfsClient, "column_with_long_type_definition_data.avro", DATA_PATH + "/column_with_long_type_definition_data.avro");

        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_avro_schema_url_long_column");
        env.executeTrinoUpdate(
                "CREATE TABLE test_avro_schema_url_long_column (dummy_col VARCHAR)" +
                        " WITH (format='AVRO', avro_schema_url='" + SCHEMAS_PATH + "/column_with_long_type_definition_schema.avsc')");
        env.executeHiveUpdate(
                "LOAD DATA INPATH '" + DATA_PATH + "/column_with_long_type_definition_data.avro' " +
                        "INTO TABLE test_avro_schema_url_long_column");

        try {
            assertThat(env.executeTrino("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_avro_schema_url_long_column'"))
                    .containsOnly(row("string_col"), row("long_record"));
            assertThat(env.executeTrino("" +
                    "SELECT " +
                    "  string_col, " +
                    "  long_record.record_field, " +
                    "  long_record.record_field422, " +
                    "  regexp_replace(json_format(CAST(long_record AS json)), '(?s)^.*(.{29})$', '... $1') " +
                    "FROM test_avro_schema_url_long_column"))
                    .containsOnly(row(
                            "string_col val",
                            "val",
                            "val422",
                            "... \",\"record_field499\":\"val499\"}"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS test_avro_schema_url_long_column");
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testPartitionedTableWithLongColumnType(HiveStorageFormatsEnvironment env)
            throws IOException
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        saveResourceOnHdfs(hdfsClient, "column_with_long_type_definition_data.avro", DATA_PATH + "/column_with_long_type_definition_data.avro");

        env.executeHiveUpdate("DROP TABLE IF EXISTS test_avro_schema_url_partitioned_long_column");
        env.executeHiveUpdate("" +
                "CREATE TABLE test_avro_schema_url_partitioned_long_column " +
                "PARTITIONED BY (pkey STRING) " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                "TBLPROPERTIES ('avro.schema.url'='" + SCHEMAS_PATH + "/column_with_long_type_definition_schema.avsc')");

        env.executeHiveUpdate(
                "LOAD DATA INPATH '" + DATA_PATH + "/column_with_long_type_definition_data.avro' " +
                        "INTO TABLE test_avro_schema_url_partitioned_long_column PARTITION(pkey='partition key value')");

        try {
            assertThat(env.executeTrino("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_avro_schema_url_partitioned_long_column'"))
                    .containsOnly(row("pkey"), row("string_col"), row("long_record"));
            assertThat(env.executeTrino("" +
                    "SELECT " +
                    "  pkey, " +
                    "  string_col, " +
                    "  long_record.record_field, " +
                    "  long_record.record_field422, " +
                    "  regexp_replace(json_format(CAST(long_record AS json)), '(?s)^.*(.{29})$', '... $1') " +
                    "FROM test_avro_schema_url_partitioned_long_column"))
                    .containsOnly(row(
                            "partition key value",
                            "string_col val",
                            "val",
                            "val422",
                            "... \",\"record_field499\":\"val499\"}"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_avro_schema_url_partitioned_long_column");
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHiveCreatedCamelCaseColumnTable(HiveStorageFormatsEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_camelCase_avro_schema_url_hive");
        env.executeHiveUpdate("" +
                "CREATE TABLE test_camelCase_avro_schema_url_hive " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                "TBLPROPERTIES ('avro.schema.url'='" + SCHEMAS_PATH + "/camelCaseSchema.avsc')");

        try {
            env.executeHiveUpdate("INSERT INTO test_camelCase_avro_schema_url_hive VALUES ('hi', 1)");
            env.executeTrinoUpdate("INSERT INTO hive.default.test_camelCase_avro_schema_url_hive VALUES ('bye', 2)");

            assertThat(env.executeHive("SELECT * FROM test_camelCase_avro_schema_url_hive")).containsOnly(row("hi", 1), row("bye", 2));
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_camelCase_avro_schema_url_hive")).containsOnly(row("hi", 1), row("bye", 2));
            assertThat(env.executeHive("SELECT intCol, stringCol FROM test_camelCase_avro_schema_url_hive")).containsOnly(row(1, "hi"), row(2, "bye"));
            assertThat(env.executeTrino("SELECT intCol, stringCol FROM hive.default.test_camelCase_avro_schema_url_hive")).containsOnly(row(1, "hi"), row(2, "bye"));
            assertThat(env.executeTrino("SELECT column_name FROM hive.information_schema.columns WHERE table_name = 'test_camelcase_avro_schema_url_hive'"))
                    .containsOnly(row("stringcol"), row("intcol"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_camelCase_avro_schema_url_hive");
        }
    }

    private void saveResourceOnHdfs(HdfsClient hdfsClient, String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = getResource(Path.of("io/trino/tests/product/hive/data/avro/", resource).toString()).openStream()) {
            byte[] content = inputStream.readAllBytes();
            hdfsClient.saveFile(location, content);
        }
    }
}
