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
package io.prestosql.tests.hive;

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.AVRO;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;

public class TestAvroSchemaUrl
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory("/user/hive/warehouse/TestAvroSchemaUrl/schemas");
        saveResourceOnHdfs("avro/original_schema.avsc", "/user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc");
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete("/user/hive/warehouse/TestAvroSchemaUrl/schemas");
    }

    private void saveResourceOnHdfs(String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource(resource)).openStream()) {
            hdfsClient.saveFile(location, inputStream);
        }
    }

    @DataProvider
    public Object[][] avroSchemaLocations()
    {
        return new Object[][] {
                {"file:///docker/volumes/presto-product-tests/avro/original_schema.avsc"}, // mounted in hadoop and presto containers
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
        assertThat(onPresto().executeQuery("SELECT * FROM test_avro_schema_url_hive")).containsExactly(row("some text", 123042));

        onHive().executeQuery("DROP TABLE test_avro_schema_url_hive");
    }

    @Test(groups = {AVRO})
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

        assertThat(onPresto().executeQuery("SHOW COLUMNS FROM test_avro_schema_url_in_serde_properties"))
                .containsExactly(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "integer", "", ""));

        assertThat(() -> onPresto().executeQuery("ALTER TABLE test_avro_schema_url_in_serde_properties ADD COLUMN new_dummy_col varchar"))
                .failsWithMessage("ALTER TABLE not supported when Avro schema url is set");

        onHive().executeQuery("INSERT INTO test_avro_schema_url_in_serde_properties VALUES ('some text', 2147483635)");

        // Hive stores initial schema inferred from schema files in the Metastore DB.
        // We need to change the schema to test that current schema is used by Presto, not a snapshot saved during CREATE TABLE.
        saveResourceOnHdfs("avro/change_column_type_schema.avsc", schemaLocationOnHdfs);

        assertThat(onPresto().executeQuery("SHOW COLUMNS FROM test_avro_schema_url_in_serde_properties"))
                .containsExactly(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "bigint", "", ""));

        assertThat(onPresto().executeQuery("SELECT * FROM test_avro_schema_url_in_serde_properties"))
                .containsExactly(row("some text", 2147483635L));

        onHive().executeQuery("DROP TABLE test_avro_schema_url_in_serde_properties");
    }

    @Test(dataProvider = "avroSchemaLocations", groups = {AVRO, STORAGE_FORMATS})
    public void testPrestoCreatedTable(String schemaLocation)
    {
        onPresto().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_presto");
        onPresto().executeQuery(format("CREATE TABLE test_avro_schema_url_presto (dummy_col VARCHAR) WITH (format='AVRO', avro_schema_url='%s')", schemaLocation));
        onPresto().executeQuery("INSERT INTO test_avro_schema_url_presto VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_url_presto")).containsExactly(row("some text", 123042));
        assertThat(onPresto().executeQuery("SELECT * FROM test_avro_schema_url_presto")).containsExactly(row("some text", 123042));

        onPresto().executeQuery("DROP TABLE test_avro_schema_url_presto");
    }
}
