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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeLegacyCreateTableWithExistingLocation
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "delta_lake";

    private File dataDirectory;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.dataDirectory = Files.createTempDirectory("test_delta_lake").toFile();
        this.metastore = createTestingFileHiveMetastore(dataDirectory);

        return createDeltaLakeQueryRunner(
                CATALOG_NAME,
                ImmutableMap.of(),
                ImmutableMap.of(
                        "delta.unique-table-location", "true",
                        "hive.metastore", "file",
                        "hive.metastore.catalog.dir", dataDirectory.getPath()));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (dataDirectory != null) {
            deleteRecursively(dataDirectory.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testLegacyCreateTable()
    {
        String tableName = "test_legacy_create_table_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);
        metastore.dropTable("tpch", tableName, false);

        assertQueryFails(format("CREATE TABLE %s.%s.%s (dummy int) with (location = '%s')", CATALOG_NAME, "tpch", tableName, tableLocation),
                ".*Using CREATE TABLE with an existing table content is deprecated.*");

        Session sessionWithLegacyCreateTableEnabled = Session
                .builder(getSession())
                .setCatalogSessionProperty(CATALOG_NAME, "legacy_create_table_with_existing_location_enabled", "true")
                .build();
        assertQuerySucceeds(sessionWithLegacyCreateTableEnabled, format("CREATE TABLE %s.%s.%s (dummy int) with (location = '%s')", CATALOG_NAME, "tpch", tableName, tableLocation));

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        assertUpdate("DROP TABLE " + tableName);
        assertThat(metastore.getTable("tpch", tableName)).as("Table should be dropped from metastore").isEmpty();
    }
}
