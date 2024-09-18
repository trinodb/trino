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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergBranching
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder().build();
        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);
        return queryRunner;
    }

    @Test
    void testCreateBranch()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_branch", "(x int)")) {
            assertBranch(table.getName(), "main");

            createBranch(table.getName(), "test-branch");
            assertBranch(table.getName(), "main", "test-branch");

            createBranch(table.getName(), "TEST-BRANCH");
            assertBranch(table.getName(), "main", "test-branch", "TEST-BRANCH");

            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE create_branch('test-branch')", "Branch 'test-branch' already exists");
        }
    }

    @Test
    void testDropBranch()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_branch", "(x int)")) {
            assertBranch(table.getName(), "main");

            createBranch(table.getName(), "test-branch");
            createBranch(table.getName(), "TEST-BRANCH");
            assertBranch(table.getName(), "main", "test-branch", "TEST-BRANCH");

            dropBranch(table.getName(), "test-branch");
            dropBranch(table.getName(), "TEST-BRANCH");
            assertBranch(table.getName(), "main");

            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE drop_branch('test-branch')", "Branch 'test-branch' does not exit");
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE drop_branch('main')", "Cannot drop 'main' branch");
        }
    }

    @Test
    void testFastForward()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "(x int)")) {
            assertBranch(table.getName(), "main");

            createBranch(table.getName(), "test-branch");
            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' VALUES 1, 2, 3", 3);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(0L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .isEqualTo(3L);

            fastForward(table.getName(), "main", "test-branch");
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(3L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .isEqualTo(3L);

            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " EXECUTE fast_forward('non-existing-branch', 'main')",
                    "Branch 'non-existing-branch' does not exit");
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " EXECUTE fast_forward('main', 'non-existing-branch')",
                    "Branch 'non-existing-branch' does not exit");
        }
    }

    @Test
    void testFastForwardNotAncestor()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_fast_forward", "(x int)")) {
            createBranch(table.getName(), "test-branch");

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' VALUES 1, 2, 3", 3);

            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " EXECUTE fast_forward('main', 'test-branch')",
                    "Branch 'main' is not an ancestor of 'test-branch'");
        }
    }

    @Test
    void testInsert()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_into_branch", "(x int, y int)")) {
            createBranch(table.getName(), "test-branch");

            // insert into main (default) branch
            assertUpdate("INSERT INTO " + table.getName() + " @ 'main' VALUES (1, 2)", 1);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(1L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .isEqualTo(0L);

            // insert into another branch
            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' VALUES (10, 20), (30, 40)", 2);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(1L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .isEqualTo(2L);

            // insert into another branch with a partial column
            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' (x) VALUES 50", 1);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(1L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .isEqualTo(3L);

            assertQueryFails(
                    "INSERT INTO " + table.getName() + " @ 'non-existing' VALUES (1, 2, 3)",
                    "Cannot find snapshot with reference name: non-existing");
        }
    }

    @Test
    void testInsertAfterSchemaEvolution()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_into_branch", "(x int, y int)")) {
            createBranch(table.getName(), "test-branch");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 2)", 1);

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN z int");

            assertQueryFails(
                    "INSERT INTO " + table.getName() + " @ 'test-branch' VALUES (1, 2, 3)",
                    "\\Qline 1:1: Insert query has mismatched column types: Table: [integer, integer], Query: [integer, integer, integer]");

            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' SELECT x + 10, y + 10 FROM " + table.getName(), 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, 2, CAST(NULL AS integer))");
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .matches("VALUES (11, 12, CAST(NULL AS integer))");
        }
    }

    @Test
    void testInsertIntoTag()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_tag", "(x int, y int)")) {
            createTag(table.getName(), "test-tag");
            assertQueryFails(
                    "INSERT INTO " + table.getName() + " @ 'test-tag' VALUES (1, 2)",
                    "Branch 'test-tag' does not exist, but a tag with that name exists");
        }
    }

    @Test
    void testDelete()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_from_branch", "(x int, y int)")) {
            createBranch(table.getName(), "test-branch");

            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' VALUES (1, 10), (2, 20), (3, 30)", 3);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(0L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .isEqualTo(3L);

            assertUpdate("DELETE FROM " + table.getName() + " @ 'test-branch'");
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName()))
                    .isEqualTo(0L);
            assertThat(computeScalar("SELECT count(*) FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .isEqualTo(0L);

            assertQueryFails(
                    "DELETE FROM " + table.getName() + " @ 'non-existing'",
                    "Cannot find snapshot with reference name: non-existing");
        }
    }

    @Test
    void testDeleteAfterSchemaEvolution()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_from_branch", "(x int, y int)")) {
            createBranch(table.getName(), "test-branch");
            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' VALUES (1, 10), (2, 20), (3, 30)", 3);

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");

            // TODO This should be fixed after once https://github.com/trinodb/trino/issues/23601 is resolved
            assertThat(query("DELETE FROM " + table.getName() + " @ 'test-branch' WHERE y = 30")).nonTrinoExceptionFailure()
                    .hasMessageContaining("Invalid metadata file")
                    .hasStackTraceContaining("Cannot find field 'y'");

            // branch returns the latest schema once a new snapshot is created
            assertUpdate("DELETE FROM " + table.getName() + " @ 'test-branch' WHERE x = 1", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .matches("VALUES 2, 3");
        }
    }

    @Test
    void testDeleteFromTag()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_tag", "(x int, y int)")) {
            createTag(table.getName(), "test-tag");
            assertQueryFails(
                    "DELETE FROM " + table.getName() + " @ 'test-tag'",
                    "Branch 'test-tag' does not exist, but a tag with that name exists");
        }
    }

    @Test
    void testUpdate()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_branch", "(x int)")) {
            createBranch(table.getName(), "test-branch");
            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' VALUES 1, 2, 3", 3);

            assertUpdate("UPDATE " + table.getName() + " @ 'test-branch' SET x = x * 2", 3);
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .matches("VALUES 2, 4, 6");

            assertQueryFails(
                    "UPDATE " + table.getName() + " @ 'non-existing' SET x = x * 2",
                    "Cannot find snapshot with reference name: non-existing");
        }
    }

    @Test
    void testUpdateAfterSchemaEvolution()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_branch", "(x int, y int)")) {
            createBranch(table.getName(), "test-branch");
            assertUpdate("INSERT INTO " + table.getName() + " @ 'test-branch' VALUES (1, 10), (2, 20), (3, 30)", 3);

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertUpdate("UPDATE " + table.getName() + " @ 'test-branch' SET y = 10", 3);

            // branch returns the latest schema once a new snapshot is created
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .matches("VALUES 1, 2, 3");
        }
    }

    @Test
    void testUpdateTag()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_tag", "(x int, y int)")) {
            createTag(table.getName(), "test-tag");
            assertQueryFails(
                    "UPDATE " + table.getName() + " @ 'test-tag' SET x = 2",
                    "Branch 'test-tag' does not exist, but a tag with that name exists");
        }
    }

    @Test
    void testMerge()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_merge_branch", "(x int)")) {
            createBranch(table.getName(), "test-branch");

            assertUpdate("MERGE INTO " + table.getName() + " @ 'test-branch' USING (VALUES 42) t(dummy) ON false " +
                    " WHEN NOT MATCHED THEN INSERT VALUES (1)", 1);
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .matches("VALUES 1");

            assertUpdate("MERGE INTO " + table.getName() + " @ 'test-branch' USING (VALUES 42) t(dummy) ON true " +
                    " WHEN MATCHED THEN UPDATE SET x = 10", 1);
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .matches("VALUES 10");

            assertQueryFails(
                    "MERGE INTO " + table.getName() + " @ 'not-existing' USING (VALUES 42) t(dummy) ON false " +
                            " WHEN NOT MATCHED THEN INSERT VALUES (1)",
                    "Cannot find snapshot with reference name: not-existing");
        }
    }

    @Test
    void testMergeAfterSchemaEvolution()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_merge_branch", "(x int, y int)")) {
            createBranch(table.getName(), "test-branch");

            // change table definition on main branch
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertUpdate("MERGE INTO " + table.getName() + " @ 'test-branch' USING (VALUES 42) t(dummy) ON false " +
                    " WHEN NOT MATCHED THEN INSERT VALUES (1, 2)", 1);

            // branch returns the latest schema once a new snapshot is created
            assertThat(query("SELECT * FROM " + table.getName() + " FOR VERSION AS OF 'test-branch'"))
                    .matches("VALUES 1");
        }
    }

    @Test
    void testMergeIntoTag()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_tag", "(x int, y int)")) {
            createTag(table.getName(), "test-tag");
            assertQueryFails(
                    "MERGE INTO " + table.getName() + " @ 'test-tag' USING (VALUES 42) t(dummy) ON false  WHEN NOT MATCHED THEN INSERT VALUES (1, 2)",
                    "Branch 'test-tag' does not exist, but a tag with that name exists");
        }
    }

    private void createBranch(String table, String branch)
    {
        assertUpdate("ALTER TABLE " + table + " EXECUTE create_branch('" + branch + "')");
    }

    private void dropBranch(String table, String branch)
    {
        assertUpdate("ALTER TABLE " + table + " EXECUTE drop_branch('" + branch + "')");
    }

    private void fastForward(String table, String from, String to)
    {
        assertUpdate("ALTER TABLE " + table + " EXECUTE fast_forward('" + from + "', '" + to + "')");
    }

    private void createTag(String table, String tag)
    {
        BaseTable icebergTable = loadTable(table);
        icebergTable.manageSnapshots()
                .createTag(tag, icebergTable.currentSnapshot().snapshotId())
                .commit();
    }

    private void assertBranch(String tableName, String... branchNames)
    {
        Table table = loadTable(tableName);
        table.refresh();
        assertThat(table.refs()).containsOnlyKeys(branchNames);
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
