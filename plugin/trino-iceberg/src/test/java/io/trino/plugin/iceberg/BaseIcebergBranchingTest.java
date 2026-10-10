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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.iceberg.SnapshotRef.MAIN_BRANCH;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseIcebergBranchingTest
        extends AbstractTestQueryFramework
{
    private final int formatVersion;

    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    protected BaseIcebergBranchingTest(int formatVersion)
    {
        this.formatVersion = formatVersion;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", String.valueOf(formatVersion))
                .build();
    }

    @BeforeAll
    public void initCatalogAccess()
    {
        metastore = getHiveMetastore(getQueryRunner());
        fileSystemFactory = getFileSystemFactory(getQueryRunner());
    }

    @Test
    public void testCreateAndShowBranches()
    {
        String tableName = "test_create_show_branches_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .result()
                .hasColumnNames("Branch")
                .matches("VALUES VARCHAR '" + MAIN_BRANCH + "'");

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .result()
                .hasColumnNames("Branch")
                .matches("VALUES VARCHAR '" + MAIN_BRANCH + "', VARCHAR 'test_branch'");

        assertUpdate("CREATE BRANCH another_branch IN TABLE " + tableName);

        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .result()
                .hasColumnNames("Branch")
                .matches("VALUES VARCHAR '" + MAIN_BRANCH + "', VARCHAR 'test_branch', VARCHAR 'another_branch'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateBranchFromBranch()
    {
        String tableName = "test_create_branch_from_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        assertUpdate("CREATE BRANCH branch_a IN TABLE " + tableName);
        assertUpdate("INSERT INTO " + tableName + "@branch_a VALUES (2, 'b')", 1);
        assertUpdate("CREATE BRANCH branch_b IN TABLE " + tableName + " FROM branch_a");

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST('a' AS VARCHAR))");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'branch_a'"))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (2, CAST('b' AS VARCHAR))");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'branch_b'"))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (2, CAST('b' AS VARCHAR))");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowBranchesWithSnapshotlessMain()
    {
        String tableName = "test_show_branches_snapshotless_main_" + randomNameSuffix();
        createTableWithoutSnapshot(tableName);
        try {
            assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES VARCHAR '" + MAIN_BRANCH + "'");

            assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
            assertUpdate("INSERT INTO " + tableName + "@test_branch VALUES 1", 1);
            assertThat(loadTable(tableName).currentSnapshot()).isNull();

            assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES VARCHAR '" + MAIN_BRANCH + "', VARCHAR 'test_branch'");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testCreateBranchFromSnapshotlessMain()
    {
        String tableName = "test_branch_from_snapshotless_main_" + randomNameSuffix();
        createTableWithoutSnapshot(tableName);
        try {
            assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName + " FROM " + MAIN_BRANCH);

            assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                    .returnsEmptyResult();
            assertThat(loadTable(tableName).currentSnapshot()).isNull();
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testCreateBranchIfNotExists()
    {
        String tableName = "test_branch_if_not_exists_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("CREATE BRANCH IF NOT EXISTS test_branch IN TABLE " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateOrReplaceBranch()
    {
        try (TestTable testTable = newTrinoTable("test_branch_or_replace_", "AS SELECT 1 AS id")) {
            String tableName = testTable.getName();
            assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
            SnapshotRef originalRef = setBranchRetention(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);

            assertUpdate("CREATE OR REPLACE BRANCH test_branch IN TABLE " + tableName);

            assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                    .matches("VALUES 1, 2");
            Table table = loadTable(tableName);
            assertThat(table.refs().get("test_branch"))
                    .isEqualTo(SnapshotRef.builderFrom(originalRef, table.currentSnapshot().snapshotId()).build());
        }
    }

    @Test
    public void testCreateOrReplaceMainBranch()
    {
        try (TestTable testTable = newTrinoTable("test_replace_main_", "AS SELECT 1 AS id")) {
            String tableName = testTable.getName();
            assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
            assertUpdate("INSERT INTO " + tableName + "@test_branch VALUES 2", 1);
            // Replacement must also work when main has diverged from the source branch.
            assertUpdate("INSERT INTO " + tableName + " VALUES 3", 1);

            assertUpdate("CREATE OR REPLACE BRANCH " + MAIN_BRANCH + " IN TABLE " + tableName + " FROM test_branch");

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 2");
            assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                    .matches("VALUES 1, 2");
        }
    }

    @Test
    public void testCreateOrReplaceBranchFromSnapshotlessMain()
    {
        String tableName = "test_branch_replace_no_snapshot_" + randomNameSuffix();
        createTableWithoutSnapshot(tableName);
        try {
            assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
            assertUpdate("INSERT INTO " + tableName + "@test_branch VALUES 1", 1);
            SnapshotRef originalRef = setBranchRetention(tableName);
            assertThat(loadTable(tableName).currentSnapshot()).isNull();

            assertUpdate("CREATE OR REPLACE BRANCH test_branch IN TABLE " + tableName);

            assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                    .returnsEmptyResult();
            Table table = loadTable(tableName);
            assertThat(table.currentSnapshot()).isNull();
            SnapshotRef replacementRef = table.refs().get("test_branch");
            assertThat(replacementRef)
                    .isEqualTo(SnapshotRef.builderFrom(originalRef, replacementRef.snapshotId()).build());
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testCreateOrReplaceBranchFromItself()
    {
        String tableName = "test_branch_replace_from_self_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);
        assertUpdate("CREATE OR REPLACE BRANCH test_branch IN TABLE " + tableName + " FROM test_branch");

        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .matches("VALUES 1");

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
                .result()
                .hasColumnNames("Branch")
                .matches("VALUES VARCHAR '" + MAIN_BRANCH + "', VARCHAR 'test_branch'");

        assertUpdate("DROP BRANCH test_branch IN TABLE " + tableName);
        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .result()
                .hasColumnNames("Branch")
                .matches("VALUES VARCHAR '" + MAIN_BRANCH + "'");

        assertThat(query("DROP BRANCH test_branch IN TABLE " + tableName))
                .failure().hasMessageContaining("Branch 'test_branch' does not exist");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropBranchIfExists()
    {
        String tableName = "test_drop_branch_if_exists_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("DROP BRANCH IF EXISTS nonexistent IN TABLE " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropMainBranchFails()
    {
        String tableName = "test_drop_main_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertThat(query("DROP BRANCH " + MAIN_BRANCH + " IN TABLE " + tableName))
                .failure().hasMessageContaining("Cannot drop the main branch");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testBranchNameCaseSensitivity()
    {
        String tableName = "test_branch_case_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("CREATE BRANCH TEST_BRANCH IN TABLE " + tableName);

        assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                .skippingTypesCheck()
                .result()
                .hasColumnNames("Branch")
                .matches("VALUES VARCHAR '" + MAIN_BRANCH + "', VARCHAR 'test_branch', VARCHAR 'TEST_BRANCH'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testBranchVisibleInRefsTable()
    {
        String tableName = "test_branch_refs_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);

        assertThat(query("SELECT name, type FROM \"" + tableName + "$refs\" WHERE type = 'BRANCH'"))
                .skippingTypesCheck()
                .matches("VALUES (VARCHAR '" + MAIN_BRANCH + "', VARCHAR 'BRANCH'), (VARCHAR 'test_branch', VARCHAR 'BRANCH')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFastForwardBranch()
    {
        String tableName = "test_fast_forward_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);

        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .matches("VALUES 1");

        assertUpdate("ALTER BRANCH test_branch IN TABLE " + tableName + " FAST FORWARD TO " + MAIN_BRANCH);

        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .matches("VALUES 1, 2");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFastForwardSnapshotlessMain()
    {
        String tableName = "test_fast_forward_snapshotless_main_" + randomNameSuffix();
        createTableWithoutSnapshot(tableName);
        try {
            assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
            assertUpdate("INSERT INTO " + tableName + "@test_branch VALUES 1", 1);
            assertThat(loadTable(tableName).currentSnapshot()).isNull();

            assertUpdate("ALTER BRANCH " + MAIN_BRANCH + " IN TABLE " + tableName + " FAST FORWARD TO test_branch");

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1");
            assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                    .matches("VALUES 1");
            Table table = loadTable(tableName);
            assertThat(table.currentSnapshot().snapshotId()).isEqualTo(table.refs().get("test_branch").snapshotId());
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testFastForwardToSnapshotlessMainFails()
    {
        String tableName = "test_fast_forward_to_snapshotless_main_" + randomNameSuffix();
        createTableWithoutSnapshot(tableName);
        try {
            assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
            assertUpdate("INSERT INTO " + tableName + "@test_branch VALUES 1", 1);

            assertThat(query("ALTER BRANCH test_branch IN TABLE " + tableName + " FAST FORWARD TO " + MAIN_BRANCH))
                    .failure().hasMessageContaining("Cannot fast-forward to branch 'main' without a snapshot");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testFastForwardBranchNonAncestorFails()
    {
        String tableName = "test_ff_non_ancestor_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH branch_a IN TABLE " + tableName);
        assertUpdate("CREATE BRANCH branch_b IN TABLE " + tableName);

        // Write to both branches so neither is an ancestor of the other
        assertUpdate("INSERT INTO " + tableName + "@branch_a VALUES 2", 1);
        assertUpdate("INSERT INTO " + tableName + "@branch_b VALUES 3", 1);

        assertThat(query("ALTER BRANCH branch_a IN TABLE " + tableName + " FAST FORWARD TO branch_b"))
                .failure().hasMessageContaining("is not an ancestor of");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertIntoBranch()
    {
        String tableName = "test_insert_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("INSERT INTO " + tableName + "@test_branch VALUES (2, 'b')", 1);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST('a' AS VARCHAR))");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (2, CAST('b' AS VARCHAR))");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertIntoSnapshotlessMain()
    {
        String tableName = "test_insert_snapshotless_main_" + randomNameSuffix();
        createTableWithoutSnapshot(tableName);
        try {
            assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
            assertThat(loadTable(tableName).currentSnapshot()).isNull();

            assertUpdate("INSERT INTO " + tableName + "@" + MAIN_BRANCH + " VALUES 1", 1);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1");
            assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                    .returnsEmptyResult();
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testDeleteFromBranch()
    {
        String tableName = "test_delete_branch_" + randomNameSuffix();
        assertUpdate(
                "CREATE TABLE " + tableName + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)",
                3);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("DELETE FROM " + tableName + "@test_branch WHERE id = 2", 1);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (2, CAST('b' AS VARCHAR)), (3, CAST('c' AS VARCHAR))");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (3, CAST('c' AS VARCHAR))");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMetadataDeleteFromBranch()
    {
        // DELETE without WHERE uses DeleteFiles rather than RowDelta
        String tableName = "test_metadata_delete_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("DELETE FROM " + tableName + "@test_branch", 2);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (2, CAST('b' AS VARCHAR))");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .returnsEmptyResult();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUpdateOnBranch()
    {
        String tableName = "test_update_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)", 2);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("UPDATE " + tableName + "@test_branch SET name = 'updated' WHERE id = 1", 1);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (2, CAST('b' AS VARCHAR))");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .matches("VALUES (1, CAST('updated' AS VARCHAR)), (2, CAST('b' AS VARCHAR))");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeIntoBranch()
    {
        String tableName = "test_merge_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)", 2);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("MERGE INTO " + tableName + "@test_branch t " +
                "USING (VALUES (1, 'merged'), (3, 'new')) AS s(id, name) " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)", 2);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (2, CAST('b' AS VARCHAR))");
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .matches("VALUES (1, CAST('merged' AS VARCHAR)), (2, CAST('b' AS VARCHAR)), (3, CAST('new' AS VARCHAR))");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDmlOnBranchAfterSchemaEvolution()
    {
        String tableName = "test_branch_schema_evolution_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

        assertUpdate("CREATE BRANCH test_branch IN TABLE " + tableName);
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR");

        // Branch writes use the current table schema, including the added column
        assertUpdate("INSERT INTO " + tableName + "@test_branch VALUES (2, 'b')", 1);
        assertUpdate("UPDATE " + tableName + "@test_branch SET name = 'a' WHERE id = 1", 1);

        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'test_branch'"))
                .matches("VALUES (1, CAST('a' AS VARCHAR)), (2, CAST('b' AS VARCHAR))");
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST(NULL AS VARCHAR))");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDmlIntoNonExistingBranchFails()
    {
        String tableName = "test_dml_nonexistent_branch_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        assertThat(query("INSERT INTO " + tableName + "@nonexistent VALUES (2, 'b')"))
                .failure().hasMessageContaining("Branch 'nonexistent' does not exist");
        assertThat(query("DELETE FROM " + tableName + "@nonexistent WHERE id = 1"))
                .failure().hasMessageContaining("Branch 'nonexistent' does not exist");
        assertThat(query("UPDATE " + tableName + "@nonexistent SET name = 'updated' WHERE id = 1"))
                .failure().hasMessageContaining("Branch 'nonexistent' does not exist");
        assertThat(query("MERGE INTO " + tableName + "@nonexistent t " +
                "USING (VALUES (1, 'merged')) AS s(id, name) " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name"))
                .failure().hasMessageContaining("Branch 'nonexistent' does not exist");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateBranchWithTagNameFails()
    {
        try (TestTable testTable = newTrinoTable("test_branch_tag_name_", "AS SELECT 1 AS id")) {
            String tableName = testTable.getName();
            Table table = loadTable(tableName);
            table.manageSnapshots().createTag("test_tag", table.currentSnapshot().snapshotId()).commit();
            SnapshotRef originalTag = table.refs().get("test_tag");

            for (String statement : List.of(
                    "CREATE BRANCH test_tag IN TABLE ",
                    "CREATE BRANCH IF NOT EXISTS test_tag IN TABLE ",
                    "CREATE OR REPLACE BRANCH test_tag IN TABLE ")) {
                assertThat(query(statement + tableName))
                        .failure()
                        .hasErrorCode(INVALID_ARGUMENTS)
                        .hasMessage("Cannot create branch 'test_tag': a tag with that name already exists");
                assertThat(loadTable(tableName).refs().get("test_tag")).isEqualTo(originalTag);
            }
        }
    }

    @Test
    public void testDmlIntoTagFails()
    {
        String tableName = "test_dml_into_tag_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        Table icebergTable = loadTable(tableName);
        icebergTable.manageSnapshots()
                .createTag("test_tag", icebergTable.currentSnapshot().snapshotId())
                .commit();

        assertThat(query("INSERT INTO " + tableName + "@test_tag VALUES (2, 'b')"))
                .failure().hasMessageContaining("Branch 'test_tag' does not exist");
        assertThat(query("DELETE FROM " + tableName + "@test_tag WHERE id = 1"))
                .failure().hasMessageContaining("Branch 'test_tag' does not exist");
        assertThat(query("UPDATE " + tableName + "@test_tag SET name = 'updated' WHERE id = 1"))
                .failure().hasMessageContaining("Branch 'test_tag' does not exist");
        assertThat(query("MERGE INTO " + tableName + "@test_tag t " +
                "USING (VALUES (1, 'merged')) AS s(id, name) " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name"))
                .failure().hasMessageContaining("Branch 'test_tag' does not exist");

        assertUpdate("DROP TABLE " + tableName);
    }

    private void createTableWithoutSnapshot(String tableName)
    {
        // Trino CREATE TABLE adds an empty snapshot, so use the Iceberg API instead.
        var catalog = getTrinoCatalog(metastore, fileSystemFactory, "iceberg");
        SchemaTableName name = new SchemaTableName("tpch", tableName);
        catalog.newCreateTableTransaction(
                        SESSION,
                        name,
                        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, name)),
                        ImmutableMap.of(FORMAT_VERSION, String.valueOf(formatVersion)))
                .commitTransaction();
        assertThat(loadTable(tableName).currentSnapshot()).isNull();
    }

    private SnapshotRef setBranchRetention(String tableName)
    {
        Table table = loadTable(tableName);
        table.manageSnapshots()
                .setMinSnapshotsToKeep("test_branch", 7)
                .setMaxSnapshotAgeMs("test_branch", DAYS.toMillis(30))
                .setMaxRefAgeMs("test_branch", DAYS.toMillis(60))
                .commit();
        return table.refs().get("test_branch");
    }

    private Table loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
