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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Functional tests for Copy-on-Write DELETE operations in the Iceberg connector.
 * Complements TestIcebergCopyOnWriteOperations with focused DELETE and MERGE-DELETE scenarios.
 */
public class TestIcebergCopyOnWriteDeleteOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @Test
    public void testCowDeleteWithVariousPredicates()
    {
        String tableName = "test_cow_delete_predicates_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) " +
                "WITH (format_version = 2, write_delete_mode = 'COPY_ON_WRITE')");
        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30), (4, 'Dave', 40), (5, 'Eve', 50)", 5);

        // Equality predicate: delete single row
        assertUpdate("DELETE FROM " + tableName + " WHERE id = 3", 1);
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 4");
        assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0", "VALUES 0");

        // Range predicate: delete rows where value > 30 (Dave=40, Eve=50 already remaining after id=3 delete)
        assertUpdate("DELETE FROM " + tableName + " WHERE value > 30", 2);
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 2");
        assertQuery("SELECT name FROM " + tableName + " ORDER BY id", "VALUES 'Alice', 'Bob'");
        assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0", "VALUES 0");

        // LIKE predicate (case-insensitive via lower()): 'Alice' and 'Bob' both remain — 'Bob' contains 'b'
        assertUpdate("DELETE FROM " + tableName + " WHERE lower(name) LIKE '%b%'", 1);
        assertQuery("SELECT name FROM " + tableName, "VALUES 'Alice'");
        assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0", "VALUES 0");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCowDeleteAllRows()
    {
        String tableName = "test_cow_delete_all_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR) " +
                "WITH (format_version = 2, write_delete_mode = 'COPY_ON_WRITE')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", 3);

        Set<String> filesBeforeDelete = getDataFilePaths(tableName);
        assertThat(filesBeforeDelete).isNotEmpty();

        // Delete all rows
        assertUpdate("DELETE FROM " + tableName, 3);

        // Table should be empty
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");

        // No delete files should exist (CoW rewrites data rather than leaving position deletes)
        assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0", "VALUES 0");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCowDeleteAndReinsert()
    {
        String tableName = "test_cow_delete_reinsert_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR) " +
                "WITH (format_version = 2, write_delete_mode = 'COPY_ON_WRITE')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Dave')", 4);

        Set<String> initialFilePaths = getDataFilePaths(tableName);
        assertThat(initialFilePaths).isNotEmpty();

        // Delete 2 rows
        assertUpdate("DELETE FROM " + tableName + " WHERE id IN (2, 4)", 2);

        // Insert 2 new rows
        assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'Eve'), (6, 'Frank')", 2);

        // Verify correct rows exist
        assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                "VALUES (1, 'Alice'), (3, 'Charlie'), (5, 'Eve'), (6, 'Frank')");
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 4");

        // No delete files
        assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content != 0", "VALUES 0");

        // File paths should have changed (data was rewritten by CoW)
        Set<String> finalFilePaths = getDataFilePaths(tableName);
        assertThat(finalFilePaths)
                .as("CoW DELETE should rewrite data files; original file paths should not appear in final state")
                .doesNotContainAnyElementsOf(initialFilePaths);
        assertThat(finalFilePaths).isNotEmpty();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCowMergeDeleteOnly()
    {
        String targetTable = "test_cow_merge_del_target_" + randomNameSuffix();
        String sourceTable = "test_cow_merge_del_source_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + targetTable + " (id INT, name VARCHAR) " +
                "WITH (format_version = 2, write_merge_mode = 'COPY_ON_WRITE')");
        assertUpdate("INSERT INTO " + targetTable + " VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Dave')", 4);

        assertUpdate("CREATE TABLE " + sourceTable + " (id INT, name VARCHAR) WITH (format_version = 2)");
        assertUpdate("INSERT INTO " + sourceTable + " VALUES (2, 'Bob'), (3, 'Charlie')", 2);

        Set<String> filesBeforeMerge = getDataFilePaths(targetTable);
        assertThat(filesBeforeMerge).isNotEmpty();

        // MERGE: delete matched rows
        assertUpdate(
                "MERGE INTO " + targetTable + " t USING " + sourceTable + " s ON (t.id = s.id) " +
                        "WHEN MATCHED THEN DELETE",
                2);

        // Rows 2 and 3 should be gone; rows 1 and 4 remain
        assertQuery("SELECT * FROM " + targetTable + " ORDER BY id",
                "VALUES (1, 'Alice'), (4, 'Dave')");
        assertQuery("SELECT count(*) FROM " + targetTable, "SELECT 2");

        // No delete files in target table
        assertQuery("SELECT count(*) FROM \"" + targetTable + "$files\" WHERE content != 0", "VALUES 0");

        // File paths in target should have changed (data was rewritten by CoW)
        Set<String> filesAfterMerge = getDataFilePaths(targetTable);
        assertThat(filesAfterMerge)
                .as("CoW MERGE should rewrite data files; original file paths should not appear after merge")
                .doesNotContainAnyElementsOf(filesBeforeMerge);
        assertThat(filesAfterMerge).isNotEmpty();

        assertUpdate("DROP TABLE " + targetTable);
        assertUpdate("DROP TABLE " + sourceTable);
    }

    private Set<String> getDataFilePaths(String tableName)
    {
        return computeActual("SELECT file_path FROM \"" + tableName + "$files\" WHERE content = 0")
                .getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(java.util.stream.Collectors.toSet());
    }

    private static String randomNameSuffix()
    {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
}
