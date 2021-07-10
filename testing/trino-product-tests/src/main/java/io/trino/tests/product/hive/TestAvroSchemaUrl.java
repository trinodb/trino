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

import com.google.inject.Inject;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryResult;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.AVRO;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.nio.file.Files.newInputStream;

public class TestAvroSchemaUrl
        extends HiveProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        // TODO move Avro schema files to classpath

        hdfsClient.createDirectory("/user/hive/warehouse/TestAvroSchemaUrl/schemas");
        saveResourceOnHdfs("avro/original_schema.avsc", "/user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc");
        saveResourceOnHdfs("avro/column_with_long_type_definition_schema.avsc", "/user/hive/warehouse/TestAvroSchemaUrl/schemas/column_with_long_type_definition_schema.avsc");

        hdfsClient.createDirectory("/user/hive/warehouse/TestAvroSchemaUrl/data");
        saveResourceOnHdfs("avro/column_with_long_type_definition_data.avro", "/user/hive/warehouse/TestAvroSchemaUrl/data/column_with_long_type_definition_data.avro");
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete("/user/hive/warehouse/TestAvroSchemaUrl");
    }

    private void saveResourceOnHdfs(String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = newInputStream(Paths.get("/docker/presto-product-tests", resource))) {
            hdfsClient.saveFile(location, inputStream);
        }
    }

    @DataProvider
    public Object[][] avroSchemaLocations()
    {
        return new Object[][] {
                {"file:///docker/presto-product-tests/avro/original_schema.avsc"}, // mounted in hadoop and presto containers
                {"hdfs://hadoop-master:9000/user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc"},
                {"hdfs:///user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc"},
                {"/user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc"}, // `avro.schema.url` can actually be path on HDFS (not URL)
        };
    }

    @Test(dataProvider = "avroSchemaLocations", groups = {AVRO, STORAGE_FORMATS})
    public void testHiveCreatedTable(String schemaLocation)
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_hive");
        onHive().executeQuery(format("" +
                        "CREATE TABLE test_avro_schema_url_hive " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                        "TBLPROPERTIES ('avro.schema.url'='%s')",
                schemaLocation));
        onHive().executeQuery("INSERT INTO test_avro_schema_url_hive VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_url_hive")).containsExactly(row("some text", 123042));
        assertThat(onTrino().executeQuery("SELECT * FROM test_avro_schema_url_hive")).containsExactly(row("some text", 123042));

        onHive().executeQuery("DROP TABLE test_avro_schema_url_hive");
    }

    @Test(groups = AVRO)
    public void testAvroSchemaUrlInSerdeProperties()
            throws IOException
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_in_serde_properties");

        String schemaLocationOnHdfs = "/user/hive/warehouse/TestAvroSchemaUrl/schemas/test_avro_schema_url_in_serde_properties.avsc";
        saveResourceOnHdfs("avro/original_schema.avsc", schemaLocationOnHdfs);
        onHive().executeQuery(format("" +
                        "CREATE TABLE test_avro_schema_url_in_serde_properties " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                        "WITH SERDEPROPERTIES ('avro.schema.url'='%s')" +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' ",
                schemaLocationOnHdfs));

        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM test_avro_schema_url_in_serde_properties"))
                .containsExactly(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "integer", "", ""));

        assertThat(() -> onTrino().executeQuery("ALTER TABLE test_avro_schema_url_in_serde_properties ADD COLUMN new_dummy_col varchar"))
                .failsWithMessage("ALTER TABLE not supported when Avro schema url is set");

        onHive().executeQuery("INSERT INTO test_avro_schema_url_in_serde_properties VALUES ('some text', 2147483635)");

        // Hive stores initial schema inferred from schema files in the Metastore DB.
        // We need to change the schema to test that current schema is used by Presto, not a snapshot saved during CREATE TABLE.
        saveResourceOnHdfs("avro/change_column_type_schema.avsc", schemaLocationOnHdfs);

        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM test_avro_schema_url_in_serde_properties"))
                .containsExactly(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "bigint", "", ""));

        assertThat(onTrino().executeQuery("SELECT * FROM test_avro_schema_url_in_serde_properties"))
                .containsExactly(row("some text", 2147483635L));

        onHive().executeQuery("DROP TABLE test_avro_schema_url_in_serde_properties");
    }

    @Test(dataProvider = "avroSchemaLocations", groups = {AVRO, STORAGE_FORMATS})
    public void testPrestoCreatedTable(String schemaLocation)
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_presto");
        onTrino().executeQuery(format("CREATE TABLE test_avro_schema_url_presto (dummy_col VARCHAR) WITH (format='AVRO', avro_schema_url='%s')", schemaLocation));
        onTrino().executeQuery("INSERT INTO test_avro_schema_url_presto VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_url_presto")).containsExactly(row("some text", 123042));
        assertThat(onTrino().executeQuery("SELECT * FROM test_avro_schema_url_presto")).containsExactly(row("some text", 123042));

        onTrino().executeQuery("DROP TABLE test_avro_schema_url_presto");
    }

    @Test(groups = {AVRO, STORAGE_FORMATS})
    public void testTableWithLongColumnType()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_long_column");
        onTrino().executeQuery(
                "CREATE TABLE test_avro_schema_url_long_column (dummy_col VARCHAR)" +
                        " WITH (format='AVRO', avro_schema_url='/user/hive/warehouse/TestAvroSchemaUrl/schemas/column_with_long_type_definition_schema.avsc')");
        onHive().executeQuery(
                "LOAD DATA INPATH '/user/hive/warehouse/TestAvroSchemaUrl/data/column_with_long_type_definition_data.avro' " +
                        "INTO TABLE test_avro_schema_url_long_column");

        assertThat(onTrino().executeQuery("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_avro_schema_url_long_column'"))
                .containsOnly(row("string_col"), row("long_record"));
        assertThat(onTrino().executeQuery("" +
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

        onTrino().executeQuery("DROP TABLE test_avro_schema_url_long_column");
    }

    @Test(groups = {AVRO, STORAGE_FORMATS})
    public void testPartitionedTableWithLongColumnType()
    {
        if (isOnHdp() && getHiveVersionMajor() < 3) {
            // HDP 2.6 won't allow to define a partitioned table with schema having a column with type definition over 2000 characters.
            // It is possible to create table with simpler schema and then alter the schema, but that results in different end state on CDH.
            // To retain proper test coverage on CDH, this test needs to be disabled on HDP.
            throw new SkipException("Skipping on HDP 2");
        }

        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_partitioned_long_column");
        onHive().executeQuery("" +
                "CREATE TABLE test_avro_schema_url_partitioned_long_column " +
                "PARTITIONED BY (pkey STRING) " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                "TBLPROPERTIES ('avro.schema.url'='/user/hive/warehouse/TestAvroSchemaUrl/schemas/column_with_long_type_definition_schema.avsc')");

        onHive().executeQuery(
                "LOAD DATA INPATH '/user/hive/warehouse/TestAvroSchemaUrl/data/column_with_long_type_definition_data.avro' " +
                        "INTO TABLE test_avro_schema_url_partitioned_long_column PARTITION(pkey='partition key value')");

        assertThat(onTrino().executeQuery("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_avro_schema_url_partitioned_long_column'"))
                .containsOnly(row("pkey"), row("string_col"), row("long_record"));
        assertThat(onTrino().executeQuery("" +
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

        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_partitioned_long_column");
    }

    private boolean isOnHdp()
    {
        try {
            QueryResult queryResult = onHive().executeQuery("SET system:hdp.version");
            String hdpVersion = (String) queryResult.row(0).get(0);
            return !isNullOrEmpty(hdpVersion);
        }
        catch (QueryExecutionException e) {
            return false;
        }
    }
}
