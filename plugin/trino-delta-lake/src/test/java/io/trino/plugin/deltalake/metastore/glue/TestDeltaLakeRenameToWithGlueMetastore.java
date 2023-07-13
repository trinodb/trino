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
package io.trino.plugin.deltalake.metastore.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestDeltaLakeRenameToWithGlueMetastore
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_delta_lake_rename_to_with_glue_" + randomNameSuffix();
    protected static final String CATALOG_NAME = "test_delta_lake_rename_to_with_glue";

    private Path schemaLocation;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = DeltaLakeQueryRunner.builder(deltaLakeSession)
                .setCatalogName(CATALOG_NAME)
                .setDeltaProperties(ImmutableMap.of("hive.metastore", "glue"))
                .build();
        schemaLocation = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");
        schemaLocation.toFile().deleteOnExit();
        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = '" + schemaLocation.toUri() + "')");
        return queryRunner;
    }

    @Test
    public void testRenameOfExternalTable()
    {
        String oldTable = "test_table_external_to_be_renamed_" + randomNameSuffix();
        String newTable = "test_table_external_renamed_" + randomNameSuffix();
        String location = schemaLocation.toUri() + "/tableLocation/";
        try {
            assertUpdate(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS val ", oldTable, location), 1);
            String oldLocation = (String) computeScalar("SELECT \"$path\" FROM " + oldTable);
            assertQuery("SELECT val FROM " + oldTable, "VALUES (1)");

            assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);
            assertQueryReturnsEmptyResult("SHOW TABLES LIKE '" + oldTable + "'");
            assertQuery("SELECT val FROM " + newTable, "VALUES (1)");
            assertQuery("SELECT \"$path\" FROM " + newTable, "SELECT '" + oldLocation + "'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + oldTable);
            assertUpdate("DROP TABLE IF EXISTS " + newTable);
        }
    }

    @Test
    public void testRenameOfManagedTable()
    {
        String oldTable = "test_table_managed_to_be_renamed_" + randomNameSuffix();
        String newTable = "test_table_managed_renamed_" + randomNameSuffix();
        try {
            assertUpdate(format("CREATE TABLE %s AS SELECT 1 AS val ", oldTable), 1);
            String oldLocation = (String) computeScalar("SELECT \"$path\" FROM " + oldTable);
            assertQuery("SELECT val FROM " + oldTable, "VALUES (1)");

            assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);
            assertQueryReturnsEmptyResult("SHOW TABLES LIKE '" + oldTable + "'");
            assertQuery("SELECT val FROM " + newTable, "VALUES (1)");
            assertQuery("SELECT \"$path\" FROM " + newTable, "SELECT '" + oldLocation + "'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + oldTable);
            assertUpdate("DROP TABLE IF EXISTS " + newTable);
        }
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        assertUpdate("DROP SCHEMA IF EXISTS " + SCHEMA);
    }
}
