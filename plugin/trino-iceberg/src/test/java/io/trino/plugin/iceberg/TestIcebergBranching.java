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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestIcebergBranching
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        verify(dataDirectory.toFile().mkdirs());

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "hive.metastore.catalog.dir", dataDirectory.toString(),
                "fs.hadoop.enabled", "true"));

        queryRunner.execute("CREATE SCHEMA tpch");

        return queryRunner;
    }

    @Test
    public void testCreateAndShowBranches()
    {
        String tableName = "test_create_show_branches_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        // Initially only "main" branch exists
        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .matches("VALUES VARCHAR 'main'");

        // Create a new branch
        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .matches("VALUES VARCHAR 'main', VARCHAR 'test_branch'");

        // Create another branch
        assertUpdate("CREATE BRANCH another_branch IN TABLE " + tableName);

        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .matches("VALUES VARCHAR 'main', VARCHAR 'test_branch', VARCHAR 'another_branch'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateBranchFromBranch()
    {
        String tableName = "test_create_branch_from_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        assertUpdate("CREATE BRANCH branch_a IN TABLE " + tableName);
        assertUpdate("CREATE BRANCH branch_b IN TABLE " + tableName + " FROM branch_a");

        // Both branches should see the same data as they point to the same snapshot
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'branch_a'",
                "VALUES (1, 'a')");
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'branch_b'",
                "VALUES (1, 'a')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateBranchIfNotExists()
    {
        String tableName = "test_branch_if_not_exists_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        // IF NOT EXISTS should not fail
        assertUpdate("CREATE BRANCH IF NOT EXISTS test_branch IN TABLE " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateOrReplaceBranch()
    {
        String tableName = "test_branch_or_replace_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        // Insert more data so main moves ahead
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);

        // OR REPLACE should succeed and point to the current snapshot
        assertUpdate("CREATE OR REPLACE BRANCH test_branch IN TABLE " + tableName);

        // After replace, branch should see the latest data
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'",
                "VALUES 1, 2");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropBranch()
    {
        String tableName = "test_drop_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .matches("VALUES VARCHAR 'main', VARCHAR 'test_branch'");

        assertUpdate("DROP BRANCH test_branch IN TABLE " + tableName);
        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .matches("VALUES VARCHAR 'main'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropBranchIfExists()
    {
        String tableName = "test_drop_branch_if_exists_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        // IF EXISTS should not fail for a non-existent branch
        assertUpdate("DROP BRANCH IF EXISTS nonexistent IN TABLE " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropMainBranchFails()
    {
        String tableName = "test_drop_main_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertThat(query("DROP BRANCH main IN TABLE " + tableName))
                .failure().hasMessageContaining("Cannot drop the main branch");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFastForwardBranch()
    {
        String tableName = "test_fast_forward_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        // Create branch at current state
        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        // Insert more data on main (main moves ahead)
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);

        // Branch still sees old data
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'",
                "VALUES 1");

        // Fast forward branch to main
        assertUpdate("ALTER BRANCH test_branch IN TABLE " + tableName + " FAST FORWARD TO main");

        // Now branch should see all data
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'",
                "VALUES 1, 2");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertIntoBranch()
    {
        String tableName = "test_insert_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        // Insert data on the branch
        assertUpdate("INSERT INTO " + tableName + "@test_branch VALUES (2, 'b')", 1);

        // Main should only have original data
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'a')");

        // Branch should have both rows
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'",
                "VALUES (1, 'a'), (2, 'b')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeleteFromBranch()
    {
        String tableName = "test_delete_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)", 3);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        // Delete from branch
        assertUpdate("DELETE FROM " + tableName + "@test_branch WHERE id = 2", 1);

        // Main should still have all rows
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        // Branch should be missing the deleted row
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'",
                "VALUES (1, 'a'), (3, 'c')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUpdateOnBranch()
    {
        String tableName = "test_update_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)", 2);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        // Update on branch
        assertUpdate("UPDATE " + tableName + "@test_branch SET name = 'updated' WHERE id = 1", 1);

        // Main should be unaffected
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'a'), (2, 'b')");

        // Branch should have the update
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'",
                "VALUES (1, 'updated'), (2, 'b')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeIntoBranch()
    {
        String tableName = "test_merge_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)", 2);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        // Merge into branch
        assertUpdate("MERGE INTO " + tableName + "@test_branch t " +
                "USING (VALUES (1, 'merged'), (3, 'new')) AS s(id, name) " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)", 2);

        // Main should be unaffected
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'a'), (2, 'b')");

        // Branch should have the merged data
        assertQuery(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'",
                "VALUES (1, 'merged'), (2, 'b'), (3, 'new')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testBranchVisibleInRefsTable()
    {
        String tableName = "test_branch_refs_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        // Verify the branch appears in the $refs system table
        assertThat(query("SELECT name, type FROM \"" + tableName + "$refs\" WHERE type = 'BRANCH'"))
                .skippingTypesCheck()
                .matches("VALUES (VARCHAR 'main', VARCHAR 'BRANCH'), (VARCHAR 'test_branch', VARCHAR 'BRANCH')");

        assertUpdate("DROP TABLE " + tableName);
    }
}
