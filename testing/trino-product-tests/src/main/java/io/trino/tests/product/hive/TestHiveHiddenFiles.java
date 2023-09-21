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
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.hive.util.TableLocationUtils.getTablePath;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveHiddenFiles
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Test
    public void testSelectFromTableContainingHiddenFiles()
            throws Exception
    {
        String tableName = "test_table_hidden_files" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE " + tableName + " (col integer)");

        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES 1");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES 2");

        List<QueryAssert.Row> tableRows = List.of(row(1), row(2));
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).containsOnly(tableRows);
        assertThat(onHive().executeQuery("SELECT * FROM " + tableName)).containsOnly(tableRows);

        String tableLocation = getTablePath(tableName);
        // Rename the table files to Hive hidden tableFiles (prefixed by `.` or `_` characters)
        List<String> tableFiles = hdfsClient.listDirectory(tableLocation);
        assertThat(tableFiles).hasSize(2);
        renameFile(tableLocation, tableFiles.get(0), '.' + tableFiles.get(0));
        renameFile(tableLocation, tableFiles.get(1), '_' + tableFiles.get(1));

        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).hasNoRows();
        assertThat(onHive().executeQuery("SELECT * FROM " + tableName)).hasNoRows();

        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testSelectFromTableContainingFilenamesWithUnderscore()
            throws Exception
    {
        String tableName = "test_table_visible_underscore_files" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE " + tableName + " AS SELECT 1 AS col");

        List<QueryAssert.Row> tableRows = List.of(row(1));
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).containsOnly(tableRows);
        assertThat(onHive().executeQuery("SELECT * FROM " + tableName)).containsOnly(tableRows);

        String tableLocation = getTablePath(tableName);
        // Prefix the table files with `f_` which should still keep them visible to Hive
        for (String filename : hdfsClient.listDirectory(tableLocation)) {
            // As long as the file is not hidden (starting with `.` or `_`), it should not be ignored by Hive
            renameFile(tableLocation, filename, "f_" + filename);
        }

        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).containsOnly(tableRows);
        assertThat(onHive().executeQuery("SELECT * FROM " + tableName)).containsOnly(tableRows);

        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    private void renameFile(String directoryLocation, String filename, String newFilename)
            throws IOException
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            hdfsClient.loadFile(directoryLocation + "/" + filename, bos);
            hdfsClient.saveFile(directoryLocation + "/" + newFilename, new ByteArrayInputStream(bos.toByteArray()));
            hdfsClient.delete(directoryLocation + "/" + filename);
        }
    }
}
