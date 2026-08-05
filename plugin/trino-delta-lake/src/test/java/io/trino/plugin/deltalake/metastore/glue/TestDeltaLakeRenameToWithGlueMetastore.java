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

import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeRenameToWithGlueMetastore
        extends AbstractTestQueryFramework
{
    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        bucketName = "test-delta-lake-rename-to-glue-" + randomNameSuffix();
        floci.createBucket(bucketName);
        String schemaName = "test_delta_lake_rename_to_with_glue_" + randomNameSuffix();
        return DeltaLakeQueryRunner.builder(schemaName)
                .addDeltaProperty("hive.metastore", "glue")
                .addDeltaProperty("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .addDeltaProperty("fs.s3.enabled", "true")
                .addDeltaProperties(floci.s3AndGlueProperties())
                .setSchemaLocation("s3://%s/%s".formatted(bucketName, schemaName))
                .build();
    }

    @AfterAll
    public void cleanup()
    {
        assertUpdate("DROP SCHEMA " + getSession().getSchema().orElseThrow() + " CASCADE");
    }

    @Test
    public void testRenameOfExternalTable()
    {
        String oldTable = "test_table_external_to_be_renamed_" + randomNameSuffix();
        String newTable = "test_table_external_renamed_" + randomNameSuffix();
        String tableLocation = "s3://%s/%s".formatted(bucketName, oldTable);
        try {
            assertUpdate(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS val ", oldTable, tableLocation), 1);
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
}
