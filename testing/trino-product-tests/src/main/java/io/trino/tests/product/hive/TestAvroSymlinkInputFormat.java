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
import com.google.inject.name.Named;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.AVRO;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.nio.file.Files.newInputStream;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAvroSymlinkInputFormat
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Test(groups = {AVRO, STORAGE_FORMATS})
    public void testSymlinkTable()
            throws Exception
    {
        String table = "test_avro_symlink";
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);

        onHive().executeQuery("" +
                "CREATE TABLE " + table +
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "WITH SERDEPROPERTIES ('avro.schema.literal'='{" +
                "\"namespace\": \"io.trino.tests.product.hive\"," +
                "\"name\": \"test_avro_symlink\"," +
                "\"type\": \"record\"," +
                "\"fields\": [" +
                "{ \"name\":\"string_col\", \"type\":\"string\"}," +
                "{ \"name\":\"int_col\", \"type\":\"int\" }" +
                "]}') " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_avro_symlink";

        saveResourceOnHdfs("avro/original_data.avro", dataDir + "/original_data.avro");
        hdfsClient.saveFile(dataDir + "/dontread.txt", "This file will cause an error if read as avro.");
        hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/original_data.avro", dataDir));
        assertThat(onTrino().executeQuery("SELECT * FROM " + table)).containsExactlyInOrder(row("someValue", 1));

        onHive().executeQuery("DROP TABLE " + table);
        hdfsClient.delete(dataDir);
    }

    @Test(groups = {AVRO, STORAGE_FORMATS})
    public void testSymlinkTableWithMultipleParentDirectories()
            throws Exception
    {
        String table = "test_avro_symlink_with_multiple_parents";
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);

        onHive().executeQuery("" +
                "CREATE TABLE " + table +
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "WITH SERDEPROPERTIES ('avro.schema.literal'='{" +
                "\"namespace\": \"io.trino.tests.product.hive\"," +
                "\"name\": \"test_avro_symlink\"," +
                "\"type\": \"record\"," +
                "\"fields\": [" +
                "{ \"name\":\"string_col\", \"type\":\"string\"}," +
                "{ \"name\":\"int_col\", \"type\":\"int\" }" +
                "]}') " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_avro_symlink_with_multiple_parents";
        String anotherDataDir = warehouseDirectory + "/data2_test_avro_symlink_with_multiple_parents";

        saveResourceOnHdfs("avro/original_data.avro", dataDir + "/original_data.avro");
        saveResourceOnHdfs("avro/original_data.avro", anotherDataDir + "/more_data.avro");
        hdfsClient.saveFile(dataDir + "/dontread.txt", "This file will cause an error if read as avro.");
        hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/original_data.avro\nhdfs:%s/more_data.avro", dataDir, anotherDataDir));
        assertThat(onTrino().executeQuery("SELECT COUNT(*) as cnt FROM " + table)).containsExactlyInOrder(row(2));

        onHive().executeQuery("DROP TABLE " + table);
        hdfsClient.delete(dataDir);
        hdfsClient.delete(anotherDataDir);
    }

    @Test(groups = {AVRO, STORAGE_FORMATS})
    public void testSymlinkTableWithNestedDirectory()
            throws Exception
    {
        String table = "test_avro_symlink_with_nested_directory";
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);

        onHive().executeQuery("" +
                "CREATE TABLE " + table +
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "WITH SERDEPROPERTIES ('avro.schema.literal'='{" +
                "\"namespace\": \"io.trino.tests.product.hive\"," +
                "\"name\": \"test_avro_symlink\"," +
                "\"type\": \"record\"," +
                "\"fields\": [" +
                "{ \"name\":\"string_col\", \"type\":\"string\"}," +
                "{ \"name\":\"int_col\", \"type\":\"int\" }" +
                "]}') " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_avro_symlink_with_nested_directory/nested_directory";
        saveResourceOnHdfs("avro/original_data.avro", dataDir + "/original_data.avro");
        hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs://%s/original_data.avro", dataDir));
        assertThat(onTrino().executeQuery("SELECT * FROM " + table)).containsExactlyInOrder(row("someValue", 1));

        onHive().executeQuery("DROP TABLE " + table);
        hdfsClient.delete(dataDir);
    }

    private void saveResourceOnHdfs(String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = newInputStream(Paths.get("/docker/presto-product-tests", resource))) {
            hdfsClient.saveFile(location, inputStream);
        }
    }
}
