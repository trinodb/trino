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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableVersion;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.TableVersion.toTableVersion;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.spi.StandardErrorCode.BRANCH_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.PointerType.TARGET_ID;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergBranching
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @BeforeAll
    void initCatalogAccess()
    {
        metastore = getHiveMetastore(getQueryRunner());
        fileSystemFactory = getFileSystemFactory(getQueryRunner());
    }

    @Test
    void testShowBranches()
    {
        try (TestTable table = newTrinoTable("test_show_branches_", "(id integer)", ImmutableList.of("1"))) {
            assertThat(query("SHOW BRANCHES IN TABLE " + table.getName()))
                    .skippingTypesCheck()
                    .result()
                    .hasColumnNames("Branch")
                    .matches("VALUES 'main'");

            // Trino cannot create branches or tags, so create them through the Iceberg API
            BaseTable icebergTable = loadTable(table.getName());
            long firstSnapshotId = icebergTable.currentSnapshot().snapshotId();
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            icebergTable.refresh();
            icebergTable.manageSnapshots()
                    .createBranch("branch_a", firstSnapshotId)
                    .createBranch("branch_b", icebergTable.currentSnapshot().snapshotId())
                    .createTag("test_tag", firstSnapshotId)
                    .commit();

            assertThat(query("SHOW BRANCHES FROM TABLE " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES 'main', 'branch_a', 'branch_b'");
        }
    }

    @Test
    void testShowBranchesAfterDroppingBranch()
    {
        try (TestTable table = newTrinoTable("test_show_branches_after_drop_", "(id integer)", ImmutableList.of("1"))) {
            BaseTable icebergTable = loadTable(table.getName());
            long snapshotId = icebergTable.currentSnapshot().snapshotId();
            icebergTable.manageSnapshots()
                    .createBranch("branch_a", snapshotId)
                    .createBranch("branch_b", snapshotId)
                    .commit();
            assertThat(query("SHOW BRANCHES IN TABLE " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES 'main', 'branch_a', 'branch_b'");

            icebergTable.manageSnapshots()
                    .removeBranch("branch_a")
                    .commit();
            assertThat(query("SHOW BRANCHES IN TABLE " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES 'main', 'branch_b'");
        }
    }

    @Test
    void testShowBranchesIsCaseSensitive()
    {
        try (TestTable table = newTrinoTable("test_show_branches_case_", "(id integer)", ImmutableList.of("1"))) {
            BaseTable icebergTable = loadTable(table.getName());
            long snapshotId = icebergTable.currentSnapshot().snapshotId();
            icebergTable.manageSnapshots()
                    .createBranch("test_branch", snapshotId)
                    .createBranch("TEST_BRANCH", snapshotId)
                    .commit();

            assertThat(query("SHOW BRANCHES IN TABLE " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES 'main', 'test_branch', 'TEST_BRANCH'");
        }
    }

    @Test
    void testShowBranchesOnTableWithoutSnapshot()
    {
        String tableName = "test_show_branches_without_snapshot_" + randomNameSuffix();
        createTableWithoutSnapshot(tableName);
        try {
            // Iceberg creates the main branch with the first snapshot
            assertThat(query("SHOW BRANCHES IN TABLE " + tableName))
                    .result()
                    .hasColumnNames("Branch")
                    .isEmpty();
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testBranchExists()
    {
        try (TestTable table = newTrinoTable("test_branch_exists_", "(id integer)", ImmutableList.of("1"))) {
            BaseTable icebergTable = loadTable(table.getName());
            long snapshotId = icebergTable.currentSnapshot().snapshotId();
            icebergTable.manageSnapshots()
                    .createBranch("test_branch", snapshotId)
                    .createTag("test_tag", snapshotId)
                    .commit();

            assertThat(query("CREATE BRANCH test_branch IN TABLE " + table.getName()))
                    .failure()
                    .hasErrorCode(BRANCH_ALREADY_EXISTS)
                    .hasMessage("line 1:1: Branch 'test_branch' already exists");
            assertUpdate("CREATE BRANCH IF NOT EXISTS test_branch IN TABLE " + table.getName());
            assertThat(query("CREATE BRANCH test_tag IN TABLE " + table.getName()))
                    .failure()
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("This connector does not support creating branches");

            assertThat(query("DROP BRANCH missing_branch IN TABLE " + table.getName()))
                    .failure()
                    .hasErrorCode(BRANCH_NOT_FOUND)
                    .hasMessage("line 1:1: Branch 'missing_branch' does not exist");
            assertThat(query("DROP BRANCH test_tag IN TABLE " + table.getName()))
                    .failure()
                    .hasErrorCode(BRANCH_NOT_FOUND)
                    .hasMessage("line 1:1: Branch 'test_tag' does not exist");
            assertThat(query("DROP BRANCH test_branch IN TABLE " + table.getName()))
                    .failure()
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("This connector does not support dropping branches");
        }
    }

    @Test
    void testTableHandleBranch()
    {
        try (TestTable table = newTrinoTable("test_table_handle_branch_", "(id integer)", ImmutableList.of("1"))) {
            BaseTable icebergTable = loadTable(table.getName());
            long snapshotId = icebergTable.currentSnapshot().snapshotId();
            icebergTable.manageSnapshots()
                    .createBranch("test_branch", snapshotId)
                    .createTag("test_tag", snapshotId)
                    .commit();

            assertThat(tableHandle(table.getName(), Optional.of(toTableVersion("test_branch"))).getBranch()).contains("test_branch");
            assertThat(tableHandle(table.getName(), Optional.of(toTableVersion("main"))).getBranch()).contains("main");
            assertThat(tableHandle(table.getName(), Optional.of(toTableVersion("test_tag"))).getBranch()).isEmpty();
            assertThat(tableHandle(table.getName(), Optional.of(new TableVersion(TARGET_ID, BIGINT, snapshotId))).getBranch()).isEmpty();
            assertThat(tableHandle(table.getName(), Optional.empty()).getBranch()).isEmpty();
        }
    }

    @Test
    void testWriteToBranch()
    {
        try (TestTable table = newTrinoTable("test_write_to_branch_", "(id integer)", ImmutableList.of("1"))) {
            BaseTable icebergTable = loadTable(table.getName());
            icebergTable.manageSnapshots()
                    .createBranch("test_branch", icebergTable.currentSnapshot().snapshotId())
                    .commit();
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            for (String branch : ImmutableList.of("test_branch", "main")) {
                String target = table.getName() + "@" + branch;
                assertThat(query("INSERT INTO " + target + " VALUES 3"))
                        .failure()
                        .hasErrorCode(NOT_SUPPORTED)
                        .hasMessage("Writing to Iceberg branches is not supported");
                assertThat(query("UPDATE " + target + " SET id = 3"))
                        .failure()
                        .hasErrorCode(NOT_SUPPORTED)
                        .hasMessage("Writing to Iceberg branches is not supported");
                assertThat(query("DELETE FROM " + target))
                        .failure()
                        .hasErrorCode(NOT_SUPPORTED)
                        .hasMessage("Writing to Iceberg branches is not supported");
                assertThat(query("MERGE INTO " + target + " t USING (VALUES 1) s(id) ON t.id = s.id WHEN MATCHED THEN DELETE"))
                        .failure()
                        .hasErrorCode(NOT_SUPPORTED)
                        .hasMessage("Writing to Iceberg branches is not supported");
            }

            assertThat(query("SELECT id FROM " + table.getName()))
                    .matches("VALUES 1, 2");
            assertThat(query("SELECT id FROM " + table.getName() + " FOR VERSION AS OF 'test_branch'"))
                    .matches("VALUES 1");
        }
    }

    @Test
    void testWriteToMissingBranch()
    {
        try (TestTable table = newTrinoTable("test_write_to_missing_branch_", "(id integer)", ImmutableList.of("1"))) {
            BaseTable icebergTable = loadTable(table.getName());
            icebergTable.manageSnapshots()
                    .createTag("test_tag", icebergTable.currentSnapshot().snapshotId())
                    .commit();

            assertThat(query("INSERT INTO " + table.getName() + "@missing_branch VALUES 2"))
                    .failure()
                    .hasErrorCode(BRANCH_NOT_FOUND)
                    .hasMessage("line 1:1: Branch 'missing_branch' does not exist");
            assertThat(query("INSERT INTO " + table.getName() + "@test_tag VALUES 2"))
                    .failure()
                    .hasErrorCode(BRANCH_NOT_FOUND)
                    .hasMessage("line 1:1: Branch 'test_tag' does not exist");
        }
    }

    private void createTableWithoutSnapshot(String tableName)
    {
        // Trino CREATE TABLE adds an empty snapshot, so use the Iceberg API instead
        TrinoCatalog catalog = getTrinoCatalog(metastore, fileSystemFactory, "iceberg");
        SchemaTableName name = new SchemaTableName("tpch", tableName);
        catalog.newCreateTableTransaction(
                        SESSION,
                        name,
                        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, name)),
                        ImmutableMap.of())
                .commitTransaction();
        assertThat(loadTable(tableName).currentSnapshot()).isNull();
    }

    private IcebergTableHandle tableHandle(String tableName, Optional<TableVersion> endVersion)
    {
        Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();
        QualifiedObjectName name = new QualifiedObjectName(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName);
        return newTransaction().execute(getSession(), session -> {
            TableHandle handle = metadata.getTableHandle(session, name, Optional.empty(), endVersion).orElseThrow();
            return (IcebergTableHandle) handle.connectorHandle();
        });
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
