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
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static com.google.common.io.Resources.getResource;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTextfileSymlinkInputFormat
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Test(groups = STORAGE_FORMATS)
    public void testSymlinkTable()
            throws Exception
    {
        String table = "test_textfile_symlink";
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);
        onHive().executeQuery(
                """
                CREATE TABLE %s (col int)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
                """.formatted(table));

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_textfile_symlink";

        saveResourceOnHdfs("data.textfile", dataDir + "/data.textfile");
        hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.textfile", dataDir));
        assertThat(onTrino().executeQuery("SELECT * FROM " + table)).containsExactlyInOrder(row(42));

        onHive().executeQuery("DROP TABLE " + table);
        hdfsClient.delete(dataDir);
    }

    @Test(groups = STORAGE_FORMATS)
    public void testSymlinkTableWithSymlinkFileContainingNonExistentPath()
            throws Exception
    {
        String table = "test_textfile_invalid_symlink";
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);
        onHive().executeQuery(
                """
                CREATE TABLE %s (col int)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
                """.formatted(table));

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_textfile_invalid_symlink";

        saveResourceOnHdfs("data.textfile", dataDir + "/data.textfile");
        hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.textfile\nhdfs:%s/missingfile.textfile", dataDir, dataDir));
        assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM " + table))
                .hasMessageMatching(".*Manifest file from the location \\[.*data_test_textfile_invalid_symlink\\] contains non-existent path:.*missingfile.textfile");

        onHive().executeQuery("DROP TABLE " + table);
        hdfsClient.delete(dataDir);
    }

    @Test(groups = STORAGE_FORMATS)
    public void testSymlinkTableWithMultipleParentDirectories()
            throws Exception
    {
        String table = "test_textfile_symlink_with_multiple_parents";
        onHive().executeQuery("DROP TABLE IF EXISTS " + table);

        onHive().executeQuery(
                """
                CREATE TABLE %s (value int)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
                """.formatted(table));

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_textfile_symlink_with_multiple_parents";
        String anotherDataDir = warehouseDirectory + "/data2_test_textfile_symlink_with_multiple_parents";

        saveResourceOnHdfs("data.textfile", dataDir + "/data.textfile");
        saveResourceOnHdfs("data.textfile", anotherDataDir + "/data.textfile");
        // This file will cause an error if read as textfile
        saveResourceOnHdfs("data.avro", dataDir + "/dontread.avro");
        hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.textfile\nhdfs:%s/data.textfile", dataDir, anotherDataDir));
        assertThat(onTrino().executeQuery("SELECT COUNT(*) as cnt FROM " + table)).containsExactlyInOrder(row(2));

        onHive().executeQuery("DROP TABLE " + table);
        hdfsClient.delete(dataDir);
        hdfsClient.delete(anotherDataDir);
    }

    private void saveResourceOnHdfs(String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = getResource(Paths.get("io/trino/tests/product/hive/data/single_int_column/", resource).toString()).openStream()) {
            hdfsClient.saveFile(location, inputStream);
        }
    }
}
