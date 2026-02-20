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
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Hive hidden files handling.
 * <p>
 * Ported from the Tempto-based TestHiveHiddenFiles.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHiveHiddenFiles
{
    private static final Pattern ACID_LOCATION_PATTERN = Pattern.compile("(.*)/delta_[^/]+");

    @Test
    void testSelectFromTableContainingHiddenFiles(HiveBasicEnvironment env)
    {
        String tableName = "test_table_hidden_files" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE TABLE hive.default." + tableName + " (col integer)");

        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES 1");
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES 2");

        List<Row> tableRows = List.of(row(1), row(2));
        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName)).containsOnly(tableRows.toArray(new Row[0]));
        assertThat(env.executeHive("SELECT * FROM " + tableName)).containsOnly(tableRows.toArray(new Row[0]));

        String tableLocation = getTablePath(env, "hive.default." + tableName);
        // Rename the table files to Hive hidden tableFiles (prefixed by `.` or `_` characters)
        HdfsClient hdfsClient = env.createHdfsClient();
        String[] tableFiles = hdfsClient.listDirectory(tableLocation);
        assertThat(tableFiles).hasSize(2);
        renameFile(hdfsClient, tableLocation, tableFiles[0], '.' + tableFiles[0]);
        renameFile(hdfsClient, tableLocation, tableFiles[1], '_' + tableFiles[1]);

        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName)).hasNoRows();
        assertThat(env.executeHive("SELECT * FROM " + tableName)).hasNoRows();

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
    }

    @Test
    void testSelectFromTableContainingFilenamesWithUnderscore(HiveBasicEnvironment env)
    {
        String tableName = "test_table_visible_underscore_files" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE TABLE hive.default." + tableName + " AS SELECT 1 AS col");

        List<Row> tableRows = List.of(row(1));
        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName)).containsOnly(tableRows.toArray(new Row[0]));
        assertThat(env.executeHive("SELECT * FROM " + tableName)).containsOnly(tableRows.toArray(new Row[0]));

        String tableLocation = getTablePath(env, "hive.default." + tableName);
        // Prefix the table files with `f_` which should still keep them visible to Hive
        HdfsClient hdfsClient = env.createHdfsClient();
        for (String filename : hdfsClient.listDirectory(tableLocation)) {
            // As long as the file is not hidden (starting with `.` or `_`), it should not be ignored by Hive
            renameFile(hdfsClient, tableLocation, filename, "f_" + filename);
        }

        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName)).containsOnly(tableRows.toArray(new Row[0]));
        assertThat(env.executeHive("SELECT * FROM " + tableName)).containsOnly(tableRows.toArray(new Row[0]));

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    private void renameFile(HdfsClient hdfsClient, String directoryLocation, String filename, String newFilename)
    {
        byte[] content = hdfsClient.readFile(directoryLocation + "/" + filename);
        hdfsClient.saveFile(directoryLocation + "/" + newFilename, content);
        hdfsClient.delete(directoryLocation + "/" + filename);
    }

    private String getTableLocation(HiveBasicEnvironment env, String tableName)
    {
        return getTableLocation(env, tableName, 0);
    }

    private String getTableLocation(HiveBasicEnvironment env, String tableName, int partitionColumns)
    {
        StringBuilder regex = new StringBuilder("/[^/]*$");
        for (int i = 0; i < partitionColumns; i++) {
            regex.insert(0, "/[^/]*");
        }
        String tableLocation = (String) env.executeTrino(
                        String.format("SELECT DISTINCT regexp_replace(\"$path\", '%s', '') FROM %s", regex, tableName))
                .getOnlyValue();

        // trim the /delta_... suffix for ACID tables
        Matcher acidLocationMatcher = ACID_LOCATION_PATTERN.matcher(tableLocation);
        if (acidLocationMatcher.matches()) {
            tableLocation = acidLocationMatcher.group(1);
        }
        return tableLocation;
    }

    private String getTablePath(HiveBasicEnvironment env, String tableName)
    {
        return getTablePath(env, tableName, 0);
    }

    private String getTablePath(HiveBasicEnvironment env, String tableName, int partitionColumns)
    {
        String location = getTableLocation(env, tableName, partitionColumns);
        return URI.create(location).getPath();
    }
}
