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

import com.google.inject.name.Named;
import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.AVRO;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;
import static java.nio.file.Files.newInputStream;

public class TestAvroSymlinkInputFormat
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory(warehouseDirectory + "/TestAvroSymlinkInputFormat/data");
        saveResourceOnHdfs("avro/original_data.avro", warehouseDirectory + "/TestAvroSymlinkInputFormat/data/original_data.avro");
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete(warehouseDirectory + "/TestAvroSymlinkInputFormat");
    }

    private void saveResourceOnHdfs(String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = newInputStream(Paths.get("/docker/presto-product-tests", resource))) {
            hdfsClient.saveFile(location, inputStream);
        }
    }

    @Test(groups = {AVRO, STORAGE_FORMATS})
    public void testSymlinkTable()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_symlink");

        onHive().executeQuery("" +
                "CREATE TABLE test_avro_symlink " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "WITH SERDEPROPERTIES ('avro.schema.literal'='{" +
                "\"namespace\": \"io.prestosql.tests.hive\"," +
                "\"name\": \"test_avro_symlink\"," +
                "\"type\": \"record\"," +
                "\"fields\": [" +
                "{ \"name\":\"string_col\", \"type\":\"string\"}," +
                "{ \"name\":\"int_col\", \"type\":\"int\" }" +
                "]}') " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        hdfsClient.delete(warehouseDirectory + "/test_avro_schema_symlink/symlink.txt");
        hdfsClient.saveFile(warehouseDirectory + "/test_avro_symlink/symlink.txt", format("hdfs://%s/TestAvroSymlinkInputFormat/data/original_data.avro", warehouseDirectory));

        assertThat(onPresto().executeQuery("SELECT * FROM test_avro_symlink"))
                .containsExactly(row("someValue", 1));
        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_symlink");
    }
}
