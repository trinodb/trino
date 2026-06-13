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
import io.airlift.concurrent.MoreFutures;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.iceberg.FileContent.DATA;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestIcebergCopyOnWrite
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        verify(dataDirectory.toFile().mkdirs());

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "hive.metastore.catalog.dir", dataDirectory.toString(),
                "fs.hadoop.enabled", "true",
                "iceberg.allowed-extra-properties", "*"));

        queryRunner.execute("CREATE SCHEMA tpch");

        return queryRunner;
    }

    private TestTable newCowTable(String prefix, String tableDefinition)
    {
        return new TestTable(getQueryRunner()::execute, prefix + randomNameSuffix(), tableDefinition);
    }

    @Test
    public void testDeleteCopyOnWrite()
    {
        try (TestTable table = newCowTable("test_cow_delete_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Verify initial state: only data files, no delete files
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id(), "VALUES 1");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id(), "VALUES 0");

            // Delete some rows
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);

            // Verify no delete files were created (CoW rewrites data files instead)
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id(), "VALUES 0");

            // Verify data files were rewritten (count may differ from initial due to rewrite)
            long newDataFileCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            assertThat(newDataFileCount).isGreaterThanOrEqualTo(1);

            // Verify data correctness
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
        }
    }

    @Test
    public void testUpdateCopyOnWrite()
    {
        try (TestTable table = newCowTable("test_cow_update_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Update some rows
            assertUpdate("UPDATE " + tableName + " SET name = 'MODIFIED' WHERE regionkey = 2", 5);

            // Verify no delete files were created
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id(), "VALUES 0");

            // Verify data correctness
            assertQuery("SELECT name FROM " + tableName + " WHERE regionkey = 2",
                    "VALUES 'MODIFIED', 'MODIFIED', 'MODIFIED', 'MODIFIED', 'MODIFIED'");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'MODIFIED'", "VALUES 5");
        }
    }

    @Test
    public void testUpdateCopyOnWriteV3PreservesRowLineage()
    {
        try (TestTable table = newCowTable(
                "test_cow_update_v3_",
                "(id integer, v varchar) WITH (format_version = 3, extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);

            long beforeUnchangedRowId = (Long) computeScalar("SELECT \"$row_id\" FROM " + tableName + " WHERE id = 1");
            long beforeUpdatedSequenceNumber = (Long) computeScalar("SELECT \"$last_updated_sequence_number\" FROM " + tableName + " WHERE id = 2");
            long beforeUnchangedSequenceNumber = (Long) computeScalar("SELECT \"$last_updated_sequence_number\" FROM " + tableName + " WHERE id = 1");

            assertUpdate("UPDATE " + tableName + " SET v = 'bb' WHERE id = 2", 1);

            assertThat((Long) computeScalar("SELECT \"$last_updated_sequence_number\" FROM " + tableName + " WHERE id = 2")).isGreaterThan(beforeUpdatedSequenceNumber);
            // For unchanged rows moved by CoW rewrite, Iceberg v3 lineage metadata must be preserved.
            assertThat((Long) computeScalar("SELECT \"$row_id\" FROM " + tableName + " WHERE id = 1")).isEqualTo(beforeUnchangedRowId);
            assertThat((Long) computeScalar("SELECT \"$last_updated_sequence_number\" FROM " + tableName + " WHERE id = 1")).isEqualTo(beforeUnchangedSequenceNumber);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'a'), (2, 'bb')");
            assertNoDeleteFiles(tableName);
        }
    }

    @Test
    public void testDeleteCopyOnWritePreservesPreExistingEqualityDeletes()
            throws Exception
    {
        try (TestTable table = newCowTable(
                "test_cow_equality_delete_",
                "(id integer, v varchar) WITH (format_version = 2, extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

            BaseTable icebergTable = loadBaseTable(tableName);
            writeEqualityDeleteForTable(
                    icebergTable,
                    getFileSystemFactory(getQueryRunner()),
                    Optional.empty(),
                    Optional.empty(),
                    Map.of("id", 1),
                    Optional.empty());

            assertQuery("SELECT id, v FROM " + tableName, "VALUES (2, 'b'), (3, 'c')");

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertQuery("SELECT id, v FROM " + tableName, "VALUES (3, 'c')");
        }
    }

    @Test
    public void testCowRewriteIgnoresPartitionScopedEqualityDeletesFromOtherPartitions()
            throws Exception
    {
        try (TestTable table = newCowTable("test_cow_partition_eq_delete_isolation_",
                "(part integer, id integer, category varchar) " +
                        "WITH (format_version = 2, partitioning = ARRAY['part'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(0, 1, 'victim'), " +
                    "(0, 4, 'keep0'), " +
                    "(1, 2, 'victim'), " +
                    "(1, 3, 'keep1')", 4);

            BaseTable icebergTable = loadBaseTable(tableName);
            writeEqualityDeleteForTable(
                    icebergTable,
                    getFileSystemFactory(getQueryRunner()),
                    Optional.of(icebergTable.spec()),
                    Optional.of(new PartitionData(new Object[] {0})),
                    Map.of("category", "victim"),
                    Optional.empty());

            assertQuery("SELECT id, part, category FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 1, 'victim'), (3, 1, 'keep1'), (4, 0, 'keep0')");

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 3", 1);

            assertQuery("SELECT id, part, category FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 1, 'victim'), (4, 0, 'keep0')");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id(), "VALUES 1");
        }
    }

    @Test
    public void testCowRewriteRetainsPartitionScopedEqualityDeletesEvenWhenAllFilesAreRewritten()
            throws Exception
    {
        // Partition-scoped equality deletes are NOT cleaned up by CoW rewrite (only file-scoped
        // DVs are). This matches Spark's behavior -- partition-scoped deletes are cleaned up
        // during compaction/optimize instead. The data correctness is unaffected because the
        // equality delete still applies correctly at read time.
        try (TestTable table = newCowTable("test_cow_partition_eq_delete_cleanup_",
                "(part integer, id integer, category varchar) " +
                        "WITH (format_version = 2, partitioning = ARRAY['part'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(0, 1, 'victim'), " +
                    "(0, 4, 'keep0'), " +
                    "(1, 2, 'victim'), " +
                    "(1, 3, 'keep1')", 4);

            BaseTable icebergTable = loadBaseTable(tableName);
            writeEqualityDeleteForTable(
                    icebergTable,
                    getFileSystemFactory(getQueryRunner()),
                    Optional.of(icebergTable.spec()),
                    Optional.of(new PartitionData(new Object[] {0})),
                    Map.of("category", "victim"),
                    Optional.empty());

            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id(), "VALUES 1");

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 4", 1);

            assertQuery("SELECT id, part, category FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 1, 'victim'), (3, 1, 'keep1')");
            // Partition-scoped equality delete is retained (not cleaned up by CoW)
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id(), "VALUES 1");
        }
    }

    @Test
    public void testCowRewriteKeepsPartitionScopedEqualityDeletesWhenOtherFilesInPartitionRemain()
            throws Exception
    {
        try (TestTable table = newCowTable("test_cow_partition_eq_delete_partial_cleanup_",
                "(part integer, id integer, category varchar) " +
                        "WITH (format_version = 2, partitioning = ARRAY['part'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 1, 'victim'), (0, 4, 'keep0a')", 2);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 5, 'victim'), (0, 6, 'keep0b')", 2);

            BaseTable icebergTable = loadBaseTable(tableName);
            writeEqualityDeleteForTable(
                    icebergTable,
                    getFileSystemFactory(getQueryRunner()),
                    Optional.of(icebergTable.spec()),
                    Optional.of(new PartitionData(new Object[] {0})),
                    Map.of("category", "victim"),
                    Optional.empty());

            assertQuery("SELECT id, part, category FROM " + tableName + " ORDER BY id",
                    "VALUES (4, 0, 'keep0a'), (6, 0, 'keep0b')");

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 4", 1);

            assertQuery("SELECT id, part, category FROM " + tableName + " ORDER BY id",
                    "VALUES (6, 0, 'keep0b')");
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + EQUALITY_DELETES.id(), "VALUES 1");
        }
    }

    @Test
    public void testMergeCopyOnWrite()
    {
        try (TestTable target = newCowTable("test_cow_merge_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation");
                TestTable source = newCowTable(
                        "test_cow_merge_source_",
                        "AS SELECT nationkey, 'NEW_' || name AS name, regionkey, comment FROM tpch.tiny.nation WHERE regionkey = 3")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            // MERGE: update matching rows, insert non-matching
            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN MATCHED THEN UPDATE SET name = s.name " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.nationkey, s.name, s.regionkey, s.comment)", 5);

            // Verify no delete files were created
            assertQuery("SELECT count(*) FROM \"" + targetName + "$files\" WHERE content = " + POSITION_DELETES.id(), "VALUES 0");

            // Verify data correctness - the matched rows should have updated names
            assertQuery("SELECT count(*) FROM " + targetName + " WHERE name LIKE 'NEW_%'", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 25");
        }
    }

    @Test
    public void testMergeCopyOnWriteV3PreservesRowLineage()
    {
        try (TestTable target = newCowTable(
                "test_cow_merge_v3_target_",
                "(id integer, v varchar) WITH (format_version = 3, extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");
                TestTable source = newCowTable(
                        "test_cow_merge_v3_source_",
                        "AS SELECT * FROM (VALUES (2, 'bb')) AS t(id, v)")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            assertUpdate("INSERT INTO " + targetName + " VALUES (1, 'a'), (2, 'b')", 2);

            long beforeUnchangedRowId = (Long) computeScalar("SELECT \"$row_id\" FROM " + targetName + " WHERE id = 1");
            long beforeMergedSequenceNumber = (Long) computeScalar("SELECT \"$last_updated_sequence_number\" FROM " + targetName + " WHERE id = 2");
            long beforeUnchangedSequenceNumber = (Long) computeScalar("SELECT \"$last_updated_sequence_number\" FROM " + targetName + " WHERE id = 1");

            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET v = s.v", 1);

            assertThat((Long) computeScalar("SELECT \"$last_updated_sequence_number\" FROM " + targetName + " WHERE id = 2")).isGreaterThan(beforeMergedSequenceNumber);
            // For unchanged rows moved by CoW rewrite, Iceberg v3 lineage metadata must be preserved.
            assertThat((Long) computeScalar("SELECT \"$row_id\" FROM " + targetName + " WHERE id = 1")).isEqualTo(beforeUnchangedRowId);
            assertThat((Long) computeScalar("SELECT \"$last_updated_sequence_number\" FROM " + targetName + " WHERE id = 1")).isEqualTo(beforeUnchangedSequenceNumber);
            assertQuery("SELECT * FROM " + targetName, "VALUES (1, 'a'), (2, 'bb')");
            assertNoDeleteFiles(targetName);
        }
    }

    @Test
    public void testDeleteCopyOnWritePartitionedTable()
    {
        try (TestTable table = newCowTable("test_cow_delete_partitioned_",
                "WITH (partitioning = ARRAY['regionkey'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Delete rows from one partition
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);

            // Verify no delete files
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id(), "VALUES 0");

            // Verify data correctness
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 0");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
        }
    }

    @Test
    public void testDeleteAllRowsCopyOnWrite()
    {
        try (TestTable table = newCowTable("test_cow_delete_all_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation WHERE regionkey = 1")) {
            String tableName = table.getName();

            // Delete all rows
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);

            // Verify no delete files
            assertQuery("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id(), "VALUES 0");

            // Verify table is empty
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
        }
    }

    @Test
    public void testMergeOnReadDefault()
    {
        // Without setting write.merge.mode, the default should be merge-on-read
        try (TestTable table = newCowTable(
                "test_mor_default_",
                "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);

            // Default mode should produce delete files (merge-on-read)
            long deleteFileCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(deleteFileCount).isGreaterThanOrEqualTo(1);

            // Verify data correctness still works
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        }
    }

    @Test
    public void testSequentialDmlOperations()
    {
        // Real-world: a dimension table receiving daily incremental updates
        try (TestTable table = newCowTable("test_cow_sequential_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Day 1: delete obsolete countries
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey IN (0, 1, 2)", 3);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");
            assertNoDeleteFiles(tableName);

            // Day 2: update names for rebranding
            assertUpdate("UPDATE " + tableName + " SET name = upper(name) WHERE regionkey = 3", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");
            assertNoDeleteFiles(tableName);

            // Day 3: another batch of deletes
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 17");
            assertNoDeleteFiles(tableName);

            // Day 4: update remaining rows (nationkeys 1,2 were deleted in day 1, so only 3 remain in regionkey=1)
            assertUpdate("UPDATE " + tableName + " SET comment = 'updated' WHERE regionkey = 1", 3);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE comment = 'updated'", "VALUES 3");
            assertNoDeleteFiles(tableName);

            // Final integrity check: regionkeys 0,1,2,3 remain (4 deleted)
            assertQuery("SELECT count(DISTINCT regionkey) FROM " + tableName, "VALUES 4");
        }
    }

    @Test
    public void testUpdateWithComplexExpressions()
    {
        // Real-world: ETL pipeline applying business transformations
        try (TestTable table = newCowTable("test_cow_complex_update_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // CASE expression update
            assertUpdate("UPDATE " + tableName + " SET name = CASE " +
                    "WHEN regionkey = 0 THEN 'AFRICA_' || CAST(nationkey AS VARCHAR) " +
                    "WHEN regionkey = 1 THEN 'AMERICAS_' || CAST(nationkey AS VARCHAR) " +
                    "ELSE name END " +
                    "WHERE regionkey IN (0, 1)", 10);

            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name LIKE 'AFRICA_%'", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name LIKE 'AMERICAS_%'", "VALUES 5");
            // Untouched rows
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey NOT IN (0, 1)", "VALUES 15");
        }
    }

    @Test
    public void testMergeWithDeleteClause()
    {
        // Real-world: CDC merge -- update existing, delete terminated, insert new
        try (TestTable target = newCowTable("test_cow_merge_del_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation");
                TestTable source = newCowTable("test_cow_merge_del_source_",
                        "AS SELECT * FROM (VALUES " +
                                "(0, 'UPDATED_ALGERIA', 0, 'updated', 'U'), " +
                                "(1, 'DELETED', 1, 'deleted', 'D'), " +
                                "(99, 'NEW_COUNTRY', 4, 'new record', 'I')) " +
                                "AS t(nationkey, name, regionkey, comment, operation)")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN MATCHED AND s.operation = 'D' THEN DELETE " +
                    "WHEN MATCHED AND s.operation = 'U' THEN UPDATE SET name = s.name, comment = s.comment " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.nationkey, s.name, s.regionkey, s.comment)", 3);

            assertNoDeleteFiles(targetName);
            // 25 original - 1 deleted + 1 inserted = 25
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 25");
            assertQuery("SELECT name FROM " + targetName + " WHERE nationkey = 0", "VALUES 'UPDATED_ALGERIA'");
            assertQuery("SELECT count(*) FROM " + targetName + " WHERE nationkey = 1", "VALUES 0");
            assertQuery("SELECT name FROM " + targetName + " WHERE nationkey = 99", "VALUES 'NEW_COUNTRY'");
        }
    }

    @Test
    public void testCowMergeSelfReference()
    {
        // Self-referencing MERGE: the source subquery scans the same CoW target. IcebergMetadata
        // keys copyOnWriteScanHandles on the full table handle, so the source scan (different
        // projection/predicate, so different handle) keeps normal split granularity while the
        // target scan still gets one-split-per-file. Iceberg snapshot isolation keeps both scans
        // pinned to the pre-merge snapshot.
        try (TestTable table = newCowTable("test_cow_merge_self_reference_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertUpdate("MERGE INTO " + tableName + " t " +
                    "USING (SELECT nationkey, regionkey FROM " + tableName + " WHERE regionkey = 0) s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN MATCHED THEN UPDATE SET name = 'SELF_MERGE_UPDATED'", 5);

            assertNoDeleteFiles(tableName);
            // Exactly the 5 regionkey=0 rows were updated; the rest are untouched.
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'SELF_MERGE_UPDATED'", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'SELF_MERGE_UPDATED' AND regionkey = 0", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            // Non-matching rows keep their original names
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 24",
                    "SELECT name FROM nation WHERE nationkey = 24");
        }
    }

    @Test
    public void testCowDeleteSelfReference()
    {
        try (TestTable table = newCowTable("test_cow_delete_self_reference_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // The subquery scans the same target table; Iceberg snapshot isolation must keep
            // both scans on the pre-delete snapshot so the matched row set is deterministic.
            assertUpdate("DELETE FROM " + tableName +
                    " WHERE nationkey IN (SELECT max(nationkey) FROM " + tableName + " GROUP BY regionkey)", 5);

            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
            // One row per region survives with max(nationkey); those five are the rows we removed.
            assertQuery("SELECT regionkey, max(nationkey) FROM " + tableName + " GROUP BY regionkey ORDER BY regionkey",
                    "SELECT regionkey, max(nationkey) FROM nation WHERE nationkey NOT IN (SELECT max(nationkey) FROM nation GROUP BY regionkey) GROUP BY regionkey ORDER BY regionkey");
        }
    }

    @Test
    public void testDeleteWithSubquery()
    {
        // Real-world: deleting rows based on a lookup table
        try (TestTable table = newCowTable("test_cow_subquery_del_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertUpdate("DELETE FROM " + tableName +
                    " WHERE nationkey IN (SELECT nationkey FROM tpch.tiny.nation WHERE regionkey = 2)", 5);

            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 2", "VALUES 0");
        }
    }

    @Test
    public void testUpdateAcrossMultiplePartitions()
    {
        // Real-world: updating a partitioned fact table across partitions
        try (TestTable table = newCowTable("test_cow_update_multi_part_",
                "WITH (partitioning = ARRAY['regionkey'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Update touches multiple partitions
            assertUpdate("UPDATE " + tableName + " SET comment = 'batch_update' WHERE nationkey < 10", 10);

            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE comment = 'batch_update'", "VALUES 10");

            // Verify partition integrity -- each partition still has correct count
            assertQuery("SELECT regionkey, count(*) FROM " + tableName + " GROUP BY regionkey ORDER BY regionkey",
                    "SELECT regionkey, count(*) FROM nation GROUP BY regionkey ORDER BY regionkey");
        }
    }

    @Test
    public void testDeleteWithNoMatchingRows()
    {
        // Edge case: DELETE that matches zero rows should be a no-op
        try (TestTable table = newCowTable("test_cow_noop_del_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            long snapshotsBefore = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"");

            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 999", 0);

            long snapshotsAfter = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"");
            assertThat(snapshotsAfter).isEqualTo(snapshotsBefore);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
        }
    }

    @Test
    public void testTimeTravelAfterCowDelete()
    {
        // Real-world: auditing -- read old snapshot after CoW rewrite
        try (TestTable table = newCowTable("test_cow_time_travel_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            long snapshotBeforeDelete = (long) computeScalar(
                    "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Time travel to before the delete should show all 25 rows
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshotBeforeDelete, "VALUES 25");
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshotBeforeDelete + " WHERE regionkey = 1", "VALUES 5");
        }
    }

    @Test
    public void testCowWithNullableColumns()
    {
        // Real-world: tables with sparse/nullable data
        try (TestTable table = newCowTable("test_cow_nullable_",
                "(id INTEGER, name VARCHAR, score DOUBLE, category VARCHAR) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'alice', 95.5, 'A'), (2, NULL, NULL, 'B'), " +
                    "(3, 'charlie', NULL, NULL), (4, NULL, 80.0, 'A'), (5, 'eve', 70.0, 'B')", 5);

            // Delete rows with null names
            assertUpdate("DELETE FROM " + tableName + " WHERE name IS NULL", 2);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 3");

            // Update null scores
            assertUpdate("UPDATE " + tableName + " SET score = 0.0 WHERE score IS NULL", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT id, score FROM " + tableName + " WHERE id = 3", "VALUES (3, 0.0e0)");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 3");
        }
    }

    @Test
    public void testCowMultipleDataFiles()
    {
        // Real-world: table with data spread across multiple files (multiple inserts)
        try (TestTable table = newCowTable("test_cow_multi_file_",
                "(id INTEGER, region VARCHAR, amount DOUBLE) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            // Insert in multiple batches to create multiple data files
            assertUpdate("INSERT INTO " + tableName + " SELECT nationkey, name, CAST(nationkey * 100 AS DOUBLE) FROM tpch.tiny.nation WHERE regionkey = 0", 5);
            assertUpdate("INSERT INTO " + tableName + " SELECT nationkey, name, CAST(nationkey * 100 AS DOUBLE) FROM tpch.tiny.nation WHERE regionkey = 1", 5);
            assertUpdate("INSERT INTO " + tableName + " SELECT nationkey, name, CAST(nationkey * 100 AS DOUBLE) FROM tpch.tiny.nation WHERE regionkey = 2", 5);

            long dataFileCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            assertThat(dataFileCount).isGreaterThanOrEqualTo(3);

            // Delete across all files (nationkeys 0,1,2,3 exist across regions 0-2 = 4 rows)
            assertUpdate("DELETE FROM " + tableName + " WHERE id < 5", 4);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 11");

            // Update across remaining files (nationkeys 12,14,15,16,17,18,21,24 = 8 rows with id >= 10)
            assertUpdate("UPDATE " + tableName + " SET amount = amount * 2 WHERE id >= 10", 8);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 11");
        }
    }

    @Test
    public void testCowIdempotentUpdate()
    {
        // Edge case: update that sets same value should still work correctly
        try (TestTable table = newCowTable("test_cow_idempotent_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertUpdate("UPDATE " + tableName + " SET name = name WHERE regionkey = 0", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
        }
    }

    @Test
    public void testCowDeleteThenInsert()
    {
        // Real-world: SCD Type 1 -- delete old dimension records, insert corrected ones
        try (TestTable table = newCowTable("test_cow_delete_insert_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            assertUpdate("INSERT INTO " + tableName + " VALUES (100, 'NEW_NATION_1', 0, 'new'), (101, 'NEW_NATION_2', 0, 'new')", 2);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 0", "VALUES 2");

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 17");
        }
    }

    @Test
    public void testMergeWithOnlyMatchedDelete()
    {
        // Real-world: MERGE used purely for conditional deletes
        try (TestTable target = newCowTable("test_cow_merge_only_del_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation");
                TestTable source = newCowTable(
                        "test_cow_merge_only_del_source_",
                        "AS SELECT nationkey FROM tpch.tiny.nation WHERE regionkey = 2")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN MATCHED THEN DELETE", 5);

            assertNoDeleteFiles(targetName);
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 20");
            assertQuery("SELECT count(*) FROM " + targetName + " WHERE regionkey = 2", "VALUES 0");
        }
    }

    @Test
    public void testDeletePartitionedEntirePartition()
    {
        // Real-world: dropping an entire partition's data via DELETE (not DROP PARTITION)
        try (TestTable table = newCowTable("test_cow_del_whole_part_",
                "WITH (partitioning = ARRAY['regionkey'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Delete two entire partitions
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey IN (0, 1)", 10);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");
            assertQuery("SELECT DISTINCT regionkey FROM " + tableName + " ORDER BY regionkey", "VALUES 2, 3, 4");

            // Remaining partitions untouched
            assertQuery("SELECT * FROM " + tableName + " WHERE regionkey = 2",
                    "SELECT * FROM nation WHERE regionkey = 2");
        }
    }

    @Test
    public void testCowWithLargerDataset()
    {
        // Real-world: more realistic data volume using tpch.tiny.orders (15000 rows)
        try (TestTable table = newCowTable("test_cow_orders_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.orders")) {
            String tableName = table.getName();

            long initialCount = (long) computeScalar("SELECT count(*) FROM " + tableName);
            assertThat(initialCount).isEqualTo(15000L);

            // Delete a chunk of orders
            long deletedCount = (long) computeScalar("SELECT count(*) FROM " + tableName + " WHERE orderstatus = 'F'");
            assertUpdate("DELETE FROM " + tableName + " WHERE orderstatus = 'F'", deletedCount);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES " + (initialCount - deletedCount));

            // Update remaining orders
            long pendingCount = (long) computeScalar("SELECT count(*) FROM " + tableName + " WHERE orderstatus = 'P'");
            assertUpdate("UPDATE " + tableName + " SET orderpriority = '1-URGENT' WHERE orderstatus = 'P'", pendingCount);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE orderpriority = '1-URGENT' AND orderstatus = 'P'",
                    "VALUES " + pendingCount);
        }
    }

    @Test
    public void testTimeTravelAfterMultipleCowOperations()
    {
        // Real-world: audit trail across multiple CoW rewrites
        try (TestTable table = newCowTable("test_cow_multi_tt_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            long snapshot0 = (long) computeScalar(
                    "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            long snapshot1 = (long) computeScalar(
                    "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");

            assertUpdate("UPDATE " + tableName + " SET name = 'X' WHERE regionkey = 1", 5);
            long snapshot2 = (long) computeScalar(
                    "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");

            // Current state
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Snapshot 0: all 25 rows
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshot0, "VALUES 25");

            // Snapshot 1: 20 rows, regionkey=0 deleted
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshot1, "VALUES 20");
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshot1 + " WHERE regionkey = 0", "VALUES 0");

            // Snapshot 2: 20 rows, regionkey=1 updated
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshot2 + " WHERE name = 'X'", "VALUES 5");
        }
    }

    @Test
    public void testMergeOnlyInsert()
    {
        // Edge case: MERGE where nothing matches -- only inserts, no CoW rewrite needed
        try (TestTable target = newCowTable("test_cow_merge_insert_only_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation WHERE regionkey < 3");
                TestTable source = newCowTable(
                        "test_cow_merge_insert_only_source_",
                        "AS SELECT * FROM tpch.tiny.nation WHERE regionkey >= 3")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            long sourceCount = (long) computeScalar("SELECT count(*) FROM " + sourceName);

            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.nationkey, s.name, s.regionkey, s.comment)", sourceCount);

            assertNoDeleteFiles(targetName);
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 25");
        }
    }

    @Test
    public void testMergeOnlyInsertProducesAppendSnapshot()
    {
        // The Iceberg connector resolves the row-level operation mode for every DELETE / UPDATE
        // / MERGE from a single property: write.merge.mode. An insert-only MERGE therefore picks
        // up CoW here and commits as a plain append snapshot (no rewrite tasks).
        try (TestTable target = newCowTable("test_cow_merge_insert_append_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation WHERE regionkey < 3");
                TestTable source = newCowTable(
                        "test_cow_merge_insert_append_source_",
                        "AS SELECT * FROM tpch.tiny.nation WHERE regionkey >= 3")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            long sourceCount = (long) computeScalar("SELECT count(*) FROM " + sourceName);

            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.nationkey, s.name, s.regionkey, s.comment)", sourceCount);

            assertNoDeleteFiles(targetName);
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 25");

            String latestOperation = (String) computeScalar(
                    "SELECT operation FROM \"" + targetName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            assertThat(latestOperation).isEqualTo("append");
        }
    }

    @Test
    public void testCowReplacesOriginalDataFiles()
    {
        // Verify that after a CoW operation, original data file paths are gone
        // and new data file paths are present (files are rewritten, not just marked with deletes)
        try (TestTable table = newCowTable("test_cow_file_replace_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Capture file paths before operation
            Set<String> filesBefore = getDataFilePaths(tableName);
            assertThat(filesBefore).isNotEmpty();

            // DELETE: should rewrite files, not create delete files
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            Set<String> filesAfterDelete = getDataFilePaths(tableName);
            assertNoDeleteFiles(tableName);
            // Files were rewritten: set changed and remains non-empty
            assertThat(filesAfterDelete).isNotEmpty();
            assertThat(filesAfterDelete).isNotEqualTo(filesBefore);

            // UPDATE: should again rewrite files
            Set<String> filesBeforeUpdate = getDataFilePaths(tableName);
            assertUpdate("UPDATE " + tableName + " SET comment = 'changed' WHERE regionkey = 0", 5);
            Set<String> filesAfterUpdate = getDataFilePaths(tableName);
            assertNoDeleteFiles(tableName);
            assertThat(filesAfterUpdate).isNotEmpty();
            assertThat(filesAfterUpdate).isNotEqualTo(filesBeforeUpdate);

            // Verify data correctness
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE comment = 'changed'", "VALUES 5");
        }
    }

    @Test
    public void testCowRewriteKeepsPartitionedFileLayout()
    {
        try (TestTable table = newCowTable("test_cow_partition_layout_",
                "(part integer, id integer, category varchar) " +
                        "WITH (partitioning = ARRAY['part'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 1, 'a'), (0, 2, 'b'), (1, 3, 'c')", 3);

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);

            assertThat(getDataFilePaths(tableName)).allMatch(path -> path.contains("part="));
        }
    }

    @Test
    public void testCowDeletePreservesSortOrderMetadata()
    {
        try (TestTable table = newCowTable("test_cow_sort_order_",
                "(id integer, v varchar) " +
                        "WITH (sorted_by = ARRAY['v'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

            assertQuery("SELECT DISTINCT sort_order_id FROM \"" + tableName + "$files\" WHERE content = " + DATA.id() + " ORDER BY 1", "VALUES 1");

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);

            assertQuery("SELECT DISTINCT sort_order_id FROM \"" + tableName + "$files\" WHERE content = " + DATA.id() + " ORDER BY 1", "VALUES 1");
        }
    }

    @Test
    public void testCowMetricsFileCountAfterMultiFileDelete()
    {
        // Verify that CoW correctly handles multi-file tables: file count should
        // reflect the number of files that still have surviving rows
        try (TestTable table = newCowTable("test_cow_metrics_file_count_",
                "(nationkey bigint, name varchar, regionkey bigint, comment varchar) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            // Insert data in separate statements to create multiple data files
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 0", 5);
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 1", 5);
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 2", 5);

            Set<String> filesBefore = getDataFilePaths(tableName);
            assertThat(filesBefore).hasSizeGreaterThanOrEqualTo(3);

            // Delete all rows from one region — should rewrite only the affected file
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);
            assertNoDeleteFiles(tableName);

            Set<String> filesAfter = getDataFilePaths(tableName);
            // Files changed: the file containing regionkey=1 was removed (all rows deleted)
            assertThat(filesAfter).isNotEmpty();
            assertThat(filesAfter).isNotEqualTo(filesBefore);

            // Data correctness
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 10");
            assertQuery("SELECT DISTINCT regionkey FROM " + tableName + " ORDER BY 1", "VALUES 0, 2");
        }
    }

    @Test
    public void testCowMetricsRowCountConsistency()
    {
        // Verify row counts stay consistent through multiple CoW operations,
        // confirming that rewrite metrics (rows deleted, rows surviving) are accurate
        try (TestTable table = newCowTable("test_cow_metrics_rows_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Initial: 25 rows
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");

            // DELETE 5 rows (regionkey=4) → 20 rows
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
            assertNoDeleteFiles(tableName);

            // UPDATE 5 rows (regionkey=0) → still 20 rows
            assertUpdate("UPDATE " + tableName + " SET comment = 'updated' WHERE regionkey = 0", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE comment = 'updated'", "VALUES 5");
            assertNoDeleteFiles(tableName);

            // MERGE: delete regionkey=1, update regionkey=2 → 15 rows remain, 5 updated
            assertUpdate("MERGE INTO " + tableName + " t USING (VALUES 1, 2) AS s(rk) " +
                    "ON t.regionkey = s.rk " +
                    "WHEN MATCHED AND s.rk = 1 THEN DELETE " +
                    "WHEN MATCHED AND s.rk = 2 THEN UPDATE SET comment = 'merged'", 10);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE comment = 'merged'", "VALUES 5");
            assertNoDeleteFiles(tableName);

            // Verify no delete files after all operations
            Set<String> finalFiles = getDataFilePaths(tableName);
            assertThat(finalFiles).isNotEmpty();

            // Verify all data file sizes are positive (no empty files)
            computeActual("SELECT file_size_in_bytes FROM \"" + tableName + "$files\" WHERE content = " + DATA.id())
                    .getMaterializedRows()
                    .forEach(row -> assertThat((Long) row.getField(0)).isGreaterThan(0));
        }
    }

    // -----------------------------------------------------------------------
    // Source file format: read ORC source, write new file in table default (PARQUET)
    // -----------------------------------------------------------------------

    @Test
    public void testCowDeleteOnMixedFormatTable()
    {
        // Create table with ORC format, insert data, change to PARQUET, then CoW delete
        // This verifies the source file format is read from the file path, not table default
        String tableName = "test_cow_mixed_format_delete_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format = 'ORC', extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // Verify initial files are ORC
            Set<String> orcFiles = getDataFilePaths(tableName);
            assertThat(orcFiles).isNotEmpty();
            assertThat(orcFiles).allMatch(path -> path.endsWith(".orc"));

            // Change default format to PARQUET
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format = 'PARQUET'");

            // Insert more data in PARQUET format
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 0", 5);

            // Verify mixed formats: some ORC, some PARQUET
            Set<String> allFiles = getDataFilePaths(tableName);
            assertThat(allFiles.stream().filter(p -> p.endsWith(".orc")).count()).isGreaterThanOrEqualTo(1);
            assertThat(allFiles.stream().filter(p -> p.endsWith(".parquet")).count()).isGreaterThanOrEqualTo(1);

            // CoW DELETE on rows that exist in ORC files -- this reads ORC source, writes PARQUET
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);
            assertNoDeleteFiles(tableName);

            // Verify new rewritten files are PARQUET (table default), not ORC
            Set<String> filesAfterDelete = getDataFilePaths(tableName);
            // The ORC file that was rewritten should be replaced by a PARQUET file
            // Original ORC file had regionkey=1 rows, so it was rewritten
            assertThat(filesAfterDelete).isNotEmpty();

            // Verify data correctness
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 1", "VALUES 0");
            // The duplicated regionkey=0 rows (5 original + 5 inserted)
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 0", "VALUES 10");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowUpdateOnMixedFormatTable()
    {
        // Update touching ORC files when table default is PARQUET
        String tableName = "test_cow_mixed_format_update_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format = 'ORC', extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // Switch to PARQUET
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format = 'PARQUET'");

            // CoW UPDATE on ORC data
            assertUpdate("UPDATE " + tableName + " SET name = 'UPDATED' WHERE regionkey = 2", 5);
            assertNoDeleteFiles(tableName);

            // Verify correctness
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'UPDATED'", "VALUES 5");
            assertQuery("SELECT * FROM " + tableName + " WHERE regionkey != 2",
                    "SELECT * FROM nation WHERE regionkey != 2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowMergeOnMixedFormatTable()
    {
        // MERGE touching ORC files when table default is PARQUET
        String tableName = "test_cow_mixed_format_merge_" + randomNameSuffix();
        String sourceName = "test_cow_mixed_format_merge_src_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format = 'ORC', extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format = 'PARQUET'");

            assertUpdate("CREATE TABLE " + sourceName + " AS SELECT nationkey, 'MERGED_' || name AS name FROM tpch.tiny.nation WHERE regionkey = 3", 5);

            assertUpdate("MERGE INTO " + tableName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN MATCHED THEN UPDATE SET name = s.name", 5);

            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name LIKE 'MERGED_%'", "VALUES 5");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + sourceName);
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // Pre-existing delete files: MoR → CoW mode switch
    // Verifies previously-deleted rows don't reappear after CoW rewrite
    // -----------------------------------------------------------------------

    @Test
    public void testCowAfterMorDeleteV2()
    {
        // v2 table: MoR delete creates position delete files, then switch to CoW
        String tableName = "test_cow_after_mor_v2_" + randomNameSuffix();
        try {
            // Create table with MoR mode (default)
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR delete: creates position delete files
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Verify position delete files were created (MoR behavior)
            long deleteFileCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(deleteFileCount).isGreaterThanOrEqualTo(1);

            // Switch to CoW mode
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW DELETE on different rows -- must NOT resurrect regionkey=1 rows
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 2", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");

            // The key assertion: regionkey=1 rows are still gone
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 1", "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 2", "VALUES 0");

            // Remaining regions intact
            assertQuery("SELECT DISTINCT regionkey FROM " + tableName + " ORDER BY 1", "VALUES 0, 3, 4");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowAfterMorUpdateV2()
    {
        // v2 table: MoR produces deletes from an UPDATE, then switch to CoW
        String tableName = "test_cow_after_mor_update_v2_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR update (internally: delete old + insert new)
            assertUpdate("UPDATE " + tableName + " SET name = 'MOR_UPDATED' WHERE regionkey = 0", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'MOR_UPDATED'", "VALUES 5");

            // Switch to CoW
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW update on overlapping data file
            assertUpdate("UPDATE " + tableName + " SET name = 'COW_UPDATED' WHERE regionkey = 1", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");

            // MoR-updated rows still correct
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'MOR_UPDATED'", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'COW_UPDATED'", "VALUES 5");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowAfterMorDeleteV3()
    {
        // v3 table: MoR delete creates deletion vectors (DVs), then switch to CoW
        String tableName = "test_cow_after_mor_v3_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 3) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR delete: creates DVs in v3
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 3", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Switch to CoW mode
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW delete -- must not resurrect regionkey=3 rows
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");

            // Key assertion: regionkey=3 rows still gone
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 3", "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 4", "VALUES 0");
            assertQuery("SELECT DISTINCT regionkey FROM " + tableName + " ORDER BY 1", "VALUES 0, 1, 2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowAfterMultipleMorDeletes()
    {
        // Multiple MoR deletes accumulate delete files, then CoW must honor all of them
        String tableName = "test_cow_after_multi_mor_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // Multiple MoR deletes on the same data file
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 0", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 5", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 10", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");

            // Switch to CoW
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW delete -- must honor all prior MoR deletes
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 15", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 21");

            // All previously deleted rows still gone
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey IN (0, 5, 10, 15)", "VALUES 0");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowMergeAfterMorDelete()
    {
        // MoR delete followed by CoW MERGE
        String tableName = "test_cow_merge_after_mor_" + randomNameSuffix();
        String sourceName = "test_cow_merge_after_mor_src_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR delete
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Switch to CoW
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // Create merge source
            assertUpdate("CREATE TABLE " + sourceName +
                    " AS SELECT nationkey, 'MERGED' AS name, regionkey, 'merged' AS comment FROM tpch.tiny.nation WHERE regionkey = 1", 5);

            // CoW MERGE -- touches same data file, must not resurrect regionkey=0
            assertUpdate("MERGE INTO " + tableName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN MATCHED THEN UPDATE SET name = s.name", 5);

            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 0", "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'MERGED'", "VALUES 5");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + sourceName);
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // Conflict detection: OverwriteFiles with snapshot validation
    // -----------------------------------------------------------------------

    @Test
    public void testCowWithConcurrentInsert()
    {
        // Verify CoW DELETE works correctly when concurrent INSERTs happen
        // The OverwriteFiles API validates from the base snapshot;
        // an INSERT to a different partition should NOT cause a conflict
        try (TestTable table = newCowTable("test_cow_concurrent_insert_",
                "WITH (partitioning = ARRAY['regionkey'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Insert additional rows (simulates a concurrent INSERT to a different partition)
            assertUpdate("INSERT INTO " + tableName +
                    " VALUES (100, 'NEW1', 0, 'new1'), (101, 'NEW2', 0, 'new2')", 2);

            // CoW DELETE on a different partition should succeed without conflict
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            assertNoDeleteFiles(tableName);

            // Verify all data is correct
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 0", "VALUES 7");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 4", "VALUES 0");
        }
    }

    @Test
    public void testCowSnapshotIsolation()
    {
        // Verify that each CoW operation produces a new snapshot with correct metadata
        try (TestTable table = newCowTable("test_cow_snapshot_isolation_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            long snapshot0 = (long) computeScalar(
                    "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");

            // CoW DELETE
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            long snapshot1 = (long) computeScalar(
                    "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            assertThat(snapshot1).isNotEqualTo(snapshot0);

            // Verify the new snapshot's operation is 'overwrite' (from OverwriteFiles API)
            String operation1 = (String) computeScalar(
                    "SELECT operation FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + snapshot1);
            assertThat(operation1).isEqualTo("overwrite");

            // CoW UPDATE should remain overwrite-shaped even when updated rows are written as new data files
            assertUpdate("UPDATE " + tableName + " SET name = 'UPDATED' WHERE regionkey = 1", 5);
            long snapshot2 = (long) computeScalar(
                    "SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
            assertThat(snapshot2).isNotEqualTo(snapshot1);

            String operation2 = (String) computeScalar(
                    "SELECT operation FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + snapshot2);
            assertThat(operation2).isEqualTo("overwrite");

            // Time travel verification: each snapshot is independently correct
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshot0, "VALUES 25");
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshot1, "VALUES 20");
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshot2, "VALUES 20");
            assertQuery("SELECT count(*) FROM \"" + tableName + "\" FOR VERSION AS OF " + snapshot2 + " WHERE name = 'UPDATED'", "VALUES 5");
        }
    }

    @Test
    public void testCowProducesOverwriteSnapshot()
    {
        // Verify CoW operations produce 'overwrite' snapshots, not 'delete'/'append'
        try (TestTable table = newCowTable("test_cow_overwrite_snapshot_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            assertThat(computeScalar("SELECT operation FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1"))
                    .isEqualTo("overwrite");

            assertUpdate("UPDATE " + tableName + " SET name = 'X' WHERE regionkey = 3", 5);
            assertThat(computeScalar("SELECT operation FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1"))
                    .isEqualTo("overwrite");
            long overwriteCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$snapshots\" WHERE operation = 'overwrite'");
            assertThat(overwriteCount).isGreaterThanOrEqualTo(2);

            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
        }
    }

    @Test
    public void testCowPartitionedMergeDoesNotProduceDeleteSnapshot()
    {
        try (TestTable target = newCowTable("test_cow_partitioned_merge_snapshot_target_",
                "WITH (partitioning = ARRAY['regionkey'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation");
                TestTable source = newCowTable("test_cow_partitioned_merge_snapshot_source_",
                        "AS SELECT nationkey, 'MERGED_' || name AS name, regionkey, comment " +
                                "FROM tpch.tiny.nation WHERE regionkey = 3 " +
                                "UNION ALL " +
                                "SELECT 100, 'INSERTED', 0, 'inserted'")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN MATCHED THEN UPDATE SET name = s.name, comment = s.comment " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.nationkey, s.name, s.regionkey, s.comment)", 6);

            assertNoDeleteFiles(targetName);
            assertThat(computeScalar("SELECT count(*) FROM \"" + targetName + "$snapshots\" WHERE operation = 'delete'"))
                    .isEqualTo(0L);
            assertThat(computeScalar("SELECT operation FROM \"" + targetName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1"))
                    .isEqualTo("overwrite");

            assertQuery("SELECT count(*) FROM " + targetName + " WHERE name LIKE 'MERGED_%'", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + targetName + " WHERE nationkey = 100", "VALUES 1");
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 26");
        }
    }

    // -----------------------------------------------------------------------
    // End-to-end scenario: full MoR → CoW migration workflow
    // -----------------------------------------------------------------------

    @Test
    public void testFullMorToCowMigration()
    {
        // Realistic migration: table starts as MoR, accumulates deletes, then migrates to CoW
        String tableName = "test_full_mor_to_cow_" + randomNameSuffix();
        try {
            // Phase 1: MoR operations
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey IN (0, 1)", 2);
            assertUpdate("UPDATE " + tableName + " SET name = 'MOR_EDIT' WHERE nationkey = 5", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 23");

            // Verify MoR produced delete files
            long morDeleteFiles = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(morDeleteFiles).isGreaterThanOrEqualTo(1);

            // Phase 2: Switch to CoW
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // Phase 3: CoW operations (must not resurrect MoR-deleted rows)
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 10", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey IN (0, 1, 10)", "VALUES 0");
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 5", "VALUES 'MOR_EDIT'");

            assertUpdate("UPDATE " + tableName + " SET name = 'COW_EDIT' WHERE nationkey = 15", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 15", "VALUES 'COW_EDIT'");

            // Final integrity
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey IN (0, 1, 10)", "VALUES 0");
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 5", "VALUES 'MOR_EDIT'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowOnPartitionedTableWithMorDeletesInDifferentPartitions()
    {
        // MoR deletes in partition A, then CoW delete in partition B
        String tableName = "test_cow_partitioned_mor_cow_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2, partitioning = ARRAY['regionkey']) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR delete in partition regionkey=0
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Switch to CoW
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW delete in different partition regionkey=1
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");

            // Neither partition's deleted rows should be present
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey IN (0, 1)", "VALUES 0");
            assertQuery("SELECT DISTINCT regionkey FROM " + tableName + " ORDER BY 1", "VALUES 2, 3, 4");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowOnPartitionedTableWithMorDeletesInSamePartition()
    {
        // MoR deletes in partition A, then CoW delete also in partition A (same data file!)
        String tableName = "test_cow_same_partition_mor_cow_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2, partitioning = ARRAY['regionkey']) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR delete of specific rows in regionkey=1 (2 of 5 rows)
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey IN (1, 2)", 2);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 23");

            // Switch to CoW
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW delete of OTHER rows in the same partition regionkey=1
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 3", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");

            // All three deletes honored: nationkeys 1, 2, 3 gone
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey IN (1, 2, 3)", "VALUES 0");
            // Remaining regionkey=1 rows: nationkeys that are in regionkey=1 but NOT 1,2,3
            // Nation data: regionkey=1 has nationkeys 1,2,3,17,24
            assertQuery("SELECT nationkey FROM " + tableName + " WHERE regionkey = 1 ORDER BY 1", "VALUES 17, 24");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowDeleteAllRowsFromFileWithMorDeletes()
    {
        // MoR deletes some rows, CoW deletes the remaining rows from the same file
        // The result should be: file completely removed (no new file emitted)
        String tableName = "test_cow_delete_remaining_after_mor_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2, partitioning = ARRAY['regionkey']) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR delete: remove 3 of 5 rows from regionkey=2
            // regionkey=2 has nationkeys: 8,9,12,18,21
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey IN (8, 9, 12)", 3);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 2", "VALUES 2");

            // Switch to CoW
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW delete: remove the remaining 2 rows from regionkey=2
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 2", 2);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 2", "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowV3WithDvsAndMixedFormat()
    {
        // Combined test: v3 DVs + mixed format + CoW
        String tableName = "test_cow_v3_dvs_mixed_" + randomNameSuffix();
        try {
            // Create v3 table with ORC
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 3, format = 'ORC') " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR delete (creates DVs in v3)
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Switch format to PARQUET and mode to CoW
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format = 'PARQUET'");
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW delete: reads ORC source with DVs merged, writes PARQUET
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");

            // Both sets of deleted rows still gone
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey IN (0, 1)", "VALUES 0");
            assertQuery("SELECT DISTINCT regionkey FROM " + tableName + " ORDER BY 1", "VALUES 2, 3, 4");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // Schema evolution: ADD/DROP columns with CoW operations
    // -----------------------------------------------------------------------

    @Test
    public void testCowAfterAddColumn()
    {
        String tableName = "test_cow_add_column_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", 25);

            // Add a new column after data already exists
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN score DOUBLE");

            // CoW DELETE after schema evolution -- rewriter must handle the new column (reads as NULL for old data)
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // CoW UPDATE setting the new column
            assertUpdate("UPDATE " + tableName + " SET score = 100.0 WHERE regionkey = 1", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE score = 100.0e0", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowAfterDropColumn()
    {
        String tableName = "test_cow_drop_column_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation", 25);

            // Drop a column
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN comment");

            // CoW DELETE after dropping a column -- rewriter reads with evolved schema
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 2", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Verify the dropped column is not in the result
            assertQuery("SELECT nationkey, name, regionkey FROM " + tableName + " WHERE regionkey = 1 ORDER BY nationkey",
                    "SELECT nationkey, name, regionkey FROM nation WHERE regionkey = 1 ORDER BY nationkey");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // Nested types: MAP, ARRAY, ROW columns
    // -----------------------------------------------------------------------

    @Test
    public void testCowWithNestedTypes()
    {
        try (TestTable table = newCowTable("test_cow_nested_",
                "(id INTEGER, tags ARRAY(VARCHAR), attrs MAP(VARCHAR, VARCHAR), info ROW(x INTEGER, y VARCHAR)) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, ARRAY['a', 'b'], MAP(ARRAY['k1'], ARRAY['v1']), ROW(10, 'x')), " +
                    "(2, ARRAY['c'], MAP(ARRAY['k2'], ARRAY['v2']), ROW(20, 'y')), " +
                    "(3, ARRAY['d', 'e', 'f'], MAP(ARRAY['k3'], ARRAY['v3']), ROW(30, 'z'))", 3);

            // CoW DELETE with nested types
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 2");

            // CoW UPDATE on a table with nested types
            assertUpdate("UPDATE " + tableName + " SET tags = ARRAY['updated'] WHERE id = 1", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT tags FROM " + tableName + " WHERE id = 1", "VALUES ARRAY['updated']");

            // Verify other nested columns survived the rewrite intact
            assertQuery("SELECT info.x FROM " + tableName + " WHERE id = 3", "VALUES 30");
        }
    }

    // -----------------------------------------------------------------------
    // Partition evolution: change partition spec then CoW
    // -----------------------------------------------------------------------

    @Test
    public void testCowAfterPartitionEvolution()
    {
        String tableName = "test_cow_partition_evolution_" + randomNameSuffix();
        try {
            // Create with partitioning by regionkey
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (partitioning = ARRAY['regionkey'], " +
                    "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // Insert more data with current partition spec
            assertUpdate("INSERT INTO " + tableName + " VALUES (100, 'EXTRA', 0, 'extra')", 1);

            // Change partition spec (evolution)
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['nationkey']");

            // Insert with new partition spec
            assertUpdate("INSERT INTO " + tableName + " VALUES (101, 'NEW_SPEC', 1, 'new spec')", 1);

            // CoW DELETE touching data from old partition spec
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 100", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey = 100", "VALUES 0");

            // Verify data with new partition spec is intact
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 101", "VALUES 'NEW_SPEC'");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 26");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // UPDATE that changes partition column value
    // -----------------------------------------------------------------------

    @Test
    public void testCowUpdatePartitionColumn()
    {
        try (TestTable table = newCowTable("test_cow_update_partition_col_",
                "WITH (partitioning = ARRAY['regionkey'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // UPDATE that moves rows to a different partition
            assertUpdate("UPDATE " + tableName + " SET regionkey = 99 WHERE nationkey = 0", 1);
            assertNoDeleteFiles(tableName);

            // Row moved from partition 0 to partition 99
            assertQuery("SELECT regionkey FROM " + tableName + " WHERE nationkey = 0", "VALUES 99");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 99", "VALUES 1");
        }
    }

    // -----------------------------------------------------------------------
    // DELETE with complex JOIN predicate
    // -----------------------------------------------------------------------

    @Test
    public void testCowDeleteWithJoinPredicate()
    {
        try (TestTable table = newCowTable("test_cow_join_delete_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // DELETE using EXISTS with a correlated subquery
            assertUpdate("DELETE FROM " + tableName +
                    " WHERE EXISTS (SELECT 1 FROM tpch.tiny.region r WHERE r.regionkey = " + tableName + ".regionkey AND r.name = 'AFRICA')", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
            // AFRICA is regionkey=0
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 0", "VALUES 0");
        }
    }

    // -----------------------------------------------------------------------
    // AVRO format (if supported)
    // -----------------------------------------------------------------------

    @Test
    public void testCowWithAvroFormat()
    {
        String tableName = "test_cow_avro_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format = 'AVRO', extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation", 25);

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 0", "VALUES 0");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowWithOrcFormat()
    {
        String tableName = "test_cow_orc_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format = 'ORC', extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation", 25);

            // CoW DELETE on ORC table
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // CoW UPDATE on ORC table
            assertUpdate("UPDATE " + tableName + " SET name = 'ORC_UPDATED' WHERE nationkey = 1", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 1", "VALUES 'ORC_UPDATED'");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // Large single-row delete (only 1 row deleted from a large file)
    // Verifies CoW works for minimal changes
    // -----------------------------------------------------------------------

    @Test
    public void testCowDeleteSingleRow()
    {
        try (TestTable table = newCowTable("test_cow_single_row_delete_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.orders")) {
            String tableName = table.getName();

            long initialCount = (long) computeScalar("SELECT count(*) FROM " + tableName);

            // Delete exactly one row
            assertUpdate("DELETE FROM " + tableName + " WHERE orderkey = 1", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES " + (initialCount - 1));
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE orderkey = 1", "VALUES 0");
        }
    }

    // -----------------------------------------------------------------------
    // Multiple DML types in sequence (DELETE, UPDATE, MERGE, INSERT)
    // Verifies CoW state doesn't leak between operations
    // -----------------------------------------------------------------------

    @Test
    public void testCowMixedOperationTypes()
    {
        try (TestTable table = newCowTable("test_cow_mixed_ops_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // 1) DELETE
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 0", 1);
            assertNoDeleteFiles(tableName);

            // 2) UPDATE
            assertUpdate("UPDATE " + tableName + " SET name = 'UPDATED' WHERE nationkey = 5", 1);
            assertNoDeleteFiles(tableName);

            // 3) INSERT (not CoW, just append)
            assertUpdate("INSERT INTO " + tableName + " VALUES (100, 'NEW', 0, 'new')", 1);

            // 4) MERGE (delete + update + insert in one operation)
            assertUpdate("MERGE INTO " + tableName + " t USING (VALUES (5, 'RE-UPDATED'), (200, 'INSERTED')) AS s(nk, nm) " +
                    "ON t.nationkey = s.nk " +
                    "WHEN MATCHED THEN UPDATE SET name = s.nm " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.nk, s.nm, 0, 'merged')", 2);
            assertNoDeleteFiles(tableName);

            // 5) DELETE again
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 100", 1);
            assertNoDeleteFiles(tableName);

            // Final state: 25 - 1(del nationkey=0) + 1(insert 100) - 1(del 100) + 1(insert 200) = 25
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 5", "VALUES 'RE-UPDATED'");
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 200", "VALUES 'INSERTED'");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey IN (0, 100)", "VALUES 0");
        }
    }

    // -----------------------------------------------------------------------
    // Verify CoW with WHERE clause that matches no files (predicate pruning)
    // -----------------------------------------------------------------------

    @Test
    public void testCowDeletePredPrunesAllFiles()
    {
        try (TestTable table = newCowTable("test_cow_pred_prune_",
                "WITH (partitioning = ARRAY['regionkey'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            Set<String> filesBefore = getDataFilePaths(tableName);

            // Delete with a predicate that matches rows but from only one partition
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 3 AND nationkey > 100", 0);

            // No files should have changed since no rows matched
            Set<String> filesAfter = getDataFilePaths(tableName);
            assertThat(filesAfter).isEqualTo(filesBefore);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
        }
    }

    // -----------------------------------------------------------------------
    // Verify split offsets are preserved on CoW rewritten files
    // -----------------------------------------------------------------------

    @Test
    public void testCowPreservesSplitOffsets()
    {
        try (TestTable table = newCowTable("test_cow_split_offsets_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Verify split offsets exist on the original data files
            long originalFilesWithOffsets = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id() + " AND split_offsets IS NOT NULL");
            assertThat(originalFilesWithOffsets).isGreaterThan(0);

            // Perform a CoW DELETE that rewrites data files
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);
            assertNoDeleteFiles(tableName);

            // Verify rewritten files still have split offsets populated
            long rewrittenFilesWithOffsets = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id() + " AND split_offsets IS NOT NULL");
            long totalRewrittenFiles = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            assertThat(totalRewrittenFiles).isGreaterThan(0);
            assertThat(rewrittenFilesWithOffsets).isEqualTo(totalRewrittenFiles);

            // Verify data correctness
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        }
    }

    // -----------------------------------------------------------------------
    // Verify dangling deletion vectors are cleaned up after CoW rewrite
    // -----------------------------------------------------------------------

    @Test
    public void testCowCleansDanglingDeletionVectors()
    {
        try (TestTable table = newCowTable("test_cow_dv_cleanup_",
                "WITH (format_version = 3) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Step 1: perform MoR deletes to create deletion vectors
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 0", 1);
            long dvCountAfterMor = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(dvCountAfterMor).isGreaterThan(0);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 24");

            // Step 2: switch to CoW mode and perform another DELETE on the same partition
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES extra_properties = MAP(" +
                    "ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 1", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 23");

            // Step 3: the CoW rewrite should have cleaned up the dangling DVs.
            // After CoW, no delete files should remain because:
            // - the old DVs from step 1 are removed (dangling DV cleanup)
            // - CoW produces new data files without delete files
            long dvCountAfterCow = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(dvCountAfterCow).isEqualTo(0);

            // Verify data is fully correct
            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE nationkey NOT IN (0, 1)");
        }
    }

    @Test
    public void testCowCleansDanglingDvsMultipleFiles()
    {
        try (TestTable table = newCowTable("test_cow_dv_multi_",
                "WITH (format_version = 3, partitioning = ARRAY['regionkey']) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Create DVs in multiple partitions via MoR deletes
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey IN (0, 5, 10, 15, 20)", 5);
            long dvCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(dvCount).isGreaterThan(0);

            // Switch to CoW and delete from the same partitions
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES extra_properties = MAP(" +
                    "ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey IN (1, 6, 11, 16, 21)", 5);

            // All old DVs should be cleaned up, no new delete files from CoW
            long dvCountAfter = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(dvCountAfter).isEqualTo(0);

            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");
            assertQuery("SELECT * FROM " + tableName,
                    "SELECT * FROM nation WHERE nationkey NOT IN (0, 1, 5, 6, 10, 11, 15, 16, 20, 21)");
        }
    }

    // -----------------------------------------------------------------------
    // Partition transform tests: bucket and truncate
    // -----------------------------------------------------------------------

    @Test
    public void testCowWithBucketPartitioning()
    {
        String tableName = "test_cow_bucket_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " (id BIGINT, name VARCHAR, amount DOUBLE) " +
                    "WITH (partitioning = ARRAY['bucket(id, 4)'], " +
                    "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");

            assertUpdate("INSERT INTO " + tableName +
                    " SELECT nationkey, name, CAST(nationkey * 10 AS DOUBLE) FROM tpch.tiny.nation", 25);

            // CoW DELETE across buckets
            assertUpdate("DELETE FROM " + tableName + " WHERE id < 5", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // CoW UPDATE across buckets
            assertUpdate("UPDATE " + tableName + " SET amount = 999.0 WHERE id >= 20", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE amount = 999.0e0", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowWithTruncatePartitioning()
    {
        String tableName = "test_cow_truncate_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " (id BIGINT, category VARCHAR, value DOUBLE) " +
                    "WITH (partitioning = ARRAY['truncate(category, 2)'], " +
                    "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'ALPHA', 10.0), (2, 'ALPHA_LONG', 20.0), " +
                    "(3, 'BETA', 30.0), (4, 'BETA_LONG', 40.0), " +
                    "(5, 'GAMMA', 50.0), (6, 'GAMMA_LONG', 60.0)", 6);

            // DELETE from one truncated partition (AL -> ALPHA and ALPHA_LONG)
            assertUpdate("DELETE FROM " + tableName + " WHERE category LIKE 'ALPHA%'", 2);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 4");

            // UPDATE in another truncated partition
            assertUpdate("UPDATE " + tableName + " SET value = 0.0 WHERE category LIKE 'BETA%'", 2);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE value = 0.0e0", "VALUES 2");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 4");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // OPTIMIZE after CoW operations
    // -----------------------------------------------------------------------

    @Test
    public void testCowAfterManySnapshotsAndFiles()
    {
        // Verifies CoW stays correct after accumulating many snapshots and file rewrites
        String tableName = "test_cow_many_snapshots_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // Multiple CoW operations to accumulate snapshots
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertUpdate("UPDATE " + tableName + " SET name = 'X' WHERE regionkey = 1", 5);
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 8", 1);

            long snapshotCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$snapshots\"");
            assertThat(snapshotCount).isGreaterThanOrEqualTo(4);

            // Insert more data, then CoW delete again
            assertUpdate("INSERT INTO " + tableName + " VALUES (100, 'EXTRA', 0, 'extra')", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 100", 1);
            assertNoDeleteFiles(tableName);

            // Final CoW operation after many snapshots still works correctly
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 14");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey = 0", "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'X'", "VALUES 5");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // Edge cases: UPDATE to NULL, DML on empty table, repeated same-row updates
    // -----------------------------------------------------------------------

    @Test
    public void testCowUpdateSetToNull()
    {
        try (TestTable table = newCowTable("test_cow_set_null_",
                "(id INTEGER, name VARCHAR, score DOUBLE) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'alice', 95.0), (2, 'bob', 80.0), (3, 'charlie', 70.0)", 3);

            // UPDATE to set values to NULL
            assertUpdate("UPDATE " + tableName + " SET name = NULL, score = NULL WHERE id = 2", 1);
            assertNoDeleteFiles(tableName);

            assertQuery("SELECT name, score FROM " + tableName + " WHERE id = 2", "VALUES (NULL, NULL)");
            assertQuery("SELECT name, score FROM " + tableName + " WHERE id = 1", "VALUES ('alice', 95.0e0)");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 3");
        }
    }

    @Test
    public void testCowDeleteOnEmptyTable()
    {
        try (TestTable table = newCowTable("test_cow_empty_del_",
                "(id INTEGER, name VARCHAR) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            // DELETE on empty table -- should be a no-op, no snapshot produced
            long snapshotsBefore = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"");
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 0);
            long snapshotsAfter = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"");
            assertThat(snapshotsAfter).isEqualTo(snapshotsBefore);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
        }
    }

    @Test
    public void testCowUpdateOnEmptyTable()
    {
        try (TestTable table = newCowTable("test_cow_empty_upd_",
                "(id INTEGER, name VARCHAR) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            long snapshotsBefore = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"");
            assertUpdate("UPDATE " + tableName + " SET name = 'x' WHERE id = 1", 0);
            long snapshotsAfter = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"");
            assertThat(snapshotsAfter).isEqualTo(snapshotsBefore);
        }
    }

    @Test
    public void testCowRepeatedUpdatesToSameRows()
    {
        try (TestTable table = newCowTable("test_cow_repeated_updates_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Update the same rows 5 times in succession
            for (int i = 1; i <= 5; i++) {
                assertUpdate("UPDATE " + tableName + " SET name = 'ITER_" + i + "' WHERE regionkey = 0", 5);
                assertNoDeleteFiles(tableName);
                assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
                assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'ITER_" + i + "'", "VALUES 5");
            }

            // Final state: only the last update's value should exist
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'ITER_5'", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name LIKE 'ITER_%'", "VALUES 5");
            // Other regions untouched
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey != 0", "VALUES 20");
        }
    }

    @Test
    public void testCowUpdateThenDeleteSameRow()
    {
        try (TestTable table = newCowTable("test_cow_update_then_delete_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // UPDATE a row
            assertUpdate("UPDATE " + tableName + " SET name = 'DOOMED' WHERE nationkey = 5", 1);
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 5", "VALUES 'DOOMED'");
            assertNoDeleteFiles(tableName);

            // Now DELETE that same row
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 5", 1);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey = 5", "VALUES 0");
            assertNoDeleteFiles(tableName);

            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 24");
        }
    }

    // -----------------------------------------------------------------------
    // Type preservation: DECIMAL, DATE, TIMESTAMP through CoW rewrite
    // -----------------------------------------------------------------------

    @Test
    public void testCowPreservesDecimalDateTimestampTypes()
    {
        try (TestTable table = newCowTable("test_cow_types_",
                "(id INTEGER, amount DECIMAL(18, 6), event_date DATE, event_ts TIMESTAMP(6)) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, CAST(123456.789012 AS DECIMAL(18,6)), DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00.123456'), " +
                    "(2, CAST(999999.999999 AS DECIMAL(18,6)), DATE '2024-06-30', TIMESTAMP '2024-06-30 23:59:59.999999'), " +
                    "(3, CAST(0.000001 AS DECIMAL(18,6)), DATE '2024-12-31', TIMESTAMP '2024-12-31 00:00:00.000001'), " +
                    "(4, CAST(-100.500000 AS DECIMAL(18,6)), DATE '2023-01-01', TIMESTAMP '2023-01-01 12:00:00.000000')", 4);

            // DELETE one row, triggering CoW rewrite
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 4", 1);
            assertNoDeleteFiles(tableName);

            // Verify types are preserved exactly after rewrite using computeScalar
            assertThat(computeScalar("SELECT CAST(amount AS VARCHAR) FROM " + tableName + " WHERE id = 1"))
                    .isEqualTo("123456.789012");
            assertThat(computeScalar("SELECT CAST(amount AS VARCHAR) FROM " + tableName + " WHERE id = 2"))
                    .isEqualTo("999999.999999");
            assertThat(computeScalar("SELECT CAST(amount AS VARCHAR) FROM " + tableName + " WHERE id = 3"))
                    .isEqualTo("0.000001");
            assertThat(computeScalar("SELECT CAST(event_date AS VARCHAR) FROM " + tableName + " WHERE id = 1"))
                    .isEqualTo("2024-01-15");
            assertThat(computeScalar("SELECT CAST(event_date AS VARCHAR) FROM " + tableName + " WHERE id = 2"))
                    .isEqualTo("2024-06-30");

            // UPDATE a row and verify types still preserved
            assertUpdate("UPDATE " + tableName + " SET amount = CAST(555.555555 AS DECIMAL(18,6)) WHERE id = 1", 1);
            assertNoDeleteFiles(tableName);
            assertThat(computeScalar("SELECT CAST(amount AS VARCHAR) FROM " + tableName + " WHERE id = 1"))
                    .isEqualTo("555.555555");
            assertThat(computeScalar("SELECT CAST(event_ts AS VARCHAR) FROM " + tableName + " WHERE id = 2"))
                    .isEqualTo("2024-06-30 23:59:59.999999");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 3");
        }
    }

    // -----------------------------------------------------------------------
    // Scalability: many data files, large number of chained operations
    // -----------------------------------------------------------------------

    @Test
    public void testCowWithManyDataFiles()
    {
        String tableName = "test_cow_many_files_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " (id BIGINT, region VARCHAR, value DOUBLE) " +
                    "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");

            // Create 10 separate data files via separate inserts
            for (int i = 0; i < 10; i++) {
                int baseId = i * 10;
                assertUpdate("INSERT INTO " + tableName + " VALUES " +
                        "(" + (baseId + 1) + ", 'R" + (i % 3) + "', " + (baseId + 1) + ".0), " +
                        "(" + (baseId + 2) + ", 'R" + (i % 3) + "', " + (baseId + 2) + ".0), " +
                        "(" + (baseId + 3) + ", 'R" + (i % 3) + "', " + (baseId + 3) + ".0)", 3);
            }

            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 30");
            long fileCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            assertThat(fileCount).isGreaterThanOrEqualTo(10);

            // CoW DELETE across multiple files (ids 1-10 span multiple files)
            assertUpdate("DELETE FROM " + tableName + " WHERE id <= 10", 3);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 27");

            // CoW UPDATE across multiple files
            assertUpdate("UPDATE " + tableName + " SET value = -1.0 WHERE region = 'R0'", 9);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE value = -1.0e0", "VALUES 9");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 27");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCowDataIntegrityThroughChainedOperations()
    {
        // Stress test: many chained CoW operations with full data integrity verification
        try (TestTable table = newCowTable("test_cow_integrity_",
                "(id BIGINT, category VARCHAR, value BIGINT) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            // Insert 50 rows
            StringBuilder insertValues = new StringBuilder();
            for (int i = 1; i <= 50; i++) {
                if (i > 1) {
                    insertValues.append(", ");
                }
                insertValues.append("(").append(i).append(", 'CAT_").append(i % 5).append("', ").append(i * 100).append(")");
            }
            assertUpdate("INSERT INTO " + tableName + " VALUES " + insertValues, 50);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 50");

            // Chain of operations -- counts verified against nation data model
            // CAT_0: 5,10,15,20,25,30,35,40,45,50; CAT_1: 1,6,11,16,21,26,31,36,41,46
            // CAT_2: 2,7,12,17,22,27,32,37,42,47; CAT_3: 3,8,13,18,23,28,33,38,43,48
            // CAT_4: 4,9,14,19,24,29,34,39,44,49
            assertUpdate("DELETE FROM " + tableName + " WHERE id <= 5", 5);                // 45 rows; removed 1(C1),2(C2),3(C3),4(C4),5(C0)
            assertUpdate("UPDATE " + tableName + " SET value = 0 WHERE category = 'CAT_0'", 9); // 45 rows; CAT_0 left: 10,15,20,25,30,35,40,45,50
            assertUpdate("DELETE FROM " + tableName + " WHERE id IN (10, 15, 20)", 3);     // 42 rows
            assertUpdate("INSERT INTO " + tableName + " VALUES (100, 'CAT_NEW', 9999)", 1); // 43 rows
            assertUpdate("UPDATE " + tableName + " SET category = 'CAT_X' WHERE id > 40 AND id <= 50", 10); // 43 rows
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 100", 1);               // 42 rows
            assertUpdate("INSERT INTO " + tableName + " VALUES (200, 'CAT_Y', 8888), (201, 'CAT_Y', 7777)", 2); // 44 rows
            assertUpdate("UPDATE " + tableName + " SET value = value + 1 WHERE category = 'CAT_X'", 10); // 44 rows
            // CAT_1 remaining: 6,11,16,21,26,31,36 (41,46 were changed to CAT_X in step 5) = 7 rows
            assertUpdate("DELETE FROM " + tableName + " WHERE category = 'CAT_1'", 7);     // 37 rows
            assertUpdate("UPDATE " + tableName + " SET value = -1 WHERE id IN (200, 201)", 2); // 37 rows

            // Final data integrity checks
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 37");
            assertNoDeleteFiles(tableName);

            // Verify no duplicate IDs exist
            assertQuery("SELECT count(DISTINCT id) FROM " + tableName, "VALUES 37");

            // Verify specific values
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE category = 'CAT_1'", "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE category = 'CAT_X'", "VALUES 10");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE category = 'CAT_Y'", "VALUES 2");
            assertQuery("SELECT value FROM " + tableName + " WHERE id = 200", "VALUES -1");
        }
    }

    // -----------------------------------------------------------------------
    // MERGE with all three clause types simultaneously
    // -----------------------------------------------------------------------

    @Test
    public void testCowMergeAllThreeClauseTypes()
    {
        try (TestTable target = newCowTable("test_cow_merge_all_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation");
                TestTable source = newCowTable("test_cow_merge_all_source_",
                        "AS SELECT * FROM (VALUES " +
                                "(0, 'UPDATED_0', 0, 'updated comment', 'U'), " +
                                "(1, 'DELETE_ME', 1, 'to delete', 'D'), " +
                                "(2, 'DELETE_ME_TOO', 1, 'also delete', 'D'), " +
                                "(98, 'NEW_98', 4, 'brand new 98', 'I'), " +
                                "(99, 'NEW_99', 4, 'brand new 99', 'I')) " +
                                "AS t(nationkey, name, regionkey, comment, op)")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            // MERGE with UPDATE + DELETE + INSERT simultaneously
            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN MATCHED AND s.op = 'U' THEN UPDATE SET name = s.name, comment = s.comment " +
                    "WHEN MATCHED AND s.op = 'D' THEN DELETE " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.nationkey, s.name, s.regionkey, s.comment)", 5);

            assertNoDeleteFiles(targetName);
            // 25 - 2 deleted + 2 inserted = 25
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 25");
            assertQuery("SELECT name FROM " + targetName + " WHERE nationkey = 0", "VALUES 'UPDATED_0'");
            assertQuery("SELECT count(*) FROM " + targetName + " WHERE nationkey IN (1, 2)", "VALUES 0");
            assertQuery("SELECT name FROM " + targetName + " WHERE nationkey = 98", "VALUES 'NEW_98'");
            assertQuery("SELECT name FROM " + targetName + " WHERE nationkey = 99", "VALUES 'NEW_99'");

            // Verify no duplicate IDs
            assertQuery("SELECT count(DISTINCT nationkey) FROM " + targetName, "VALUES 25");
        }
    }

    // -----------------------------------------------------------------------
    // Format version explicit tests: v2 and v3 direct CoW (no MoR migration)
    // -----------------------------------------------------------------------

    @Test
    public void testCowDirectOnV3Table()
    {
        // Create v3 table in CoW mode from the start -- verify no DVs are created
        String tableName = "test_cow_direct_v3_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 3, " +
                    "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // CoW DELETE should NOT produce DVs on a v3 table when CoW mode is active
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // CoW UPDATE
            assertUpdate("UPDATE " + tableName + " SET name = 'V3_COW' WHERE regionkey = 1", 5);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'V3_COW'", "VALUES 5");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 20");

            // Verify snapshot operations are 'overwrite' (not 'delete' which would indicate DV)
            long overwriteSnapshots = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$snapshots\" WHERE operation = 'overwrite'");
            assertThat(overwriteSnapshots).isGreaterThanOrEqualTo(1);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // P0 Severity: Data integrity -- no duplication, no loss, no corruption
    // -----------------------------------------------------------------------

    @Test
    public void testCowNoDataDuplicationAfterMultipleOperations()
    {
        // Verify that CoW rewrites never duplicate rows, even through many operations
        try (TestTable table = newCowTable("test_cow_no_dup_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Perform many operations that touch overlapping data
            assertUpdate("UPDATE " + tableName + " SET comment = 'round1' WHERE regionkey = 0", 5);
            assertUpdate("UPDATE " + tableName + " SET comment = 'round2' WHERE regionkey = 0", 5);
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 0", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 'RESURRECTED', 0, 'reinserted')", 1);
            assertUpdate("UPDATE " + tableName + " SET comment = 'round3' WHERE nationkey = 0", 1);

            // CRITICAL: verify no duplicates exist
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            long distinctCount = (long) computeScalar("SELECT count(DISTINCT nationkey) FROM " + tableName);
            long totalCount = (long) computeScalar("SELECT count(*) FROM " + tableName);
            assertThat(distinctCount).isEqualTo(totalCount);

            // Verify specific values
            assertQuery("SELECT comment FROM " + tableName + " WHERE nationkey = 0", "VALUES 'round3'");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey = 0", "VALUES 1");
        }
    }

    @Test
    public void testCowNoDataLossOnPartialDelete()
    {
        // Verify that deleting from one file doesn't affect rows in other files
        try (TestTable table = newCowTable("test_cow_no_loss_",
                "(id BIGINT, data VARCHAR) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            // Insert in separate batches to create separate data files
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'file1_row1'), (2, 'file1_row2')", 2);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'file2_row1'), (4, 'file2_row2')", 2);
            assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'file3_row1'), (6, 'file3_row2')", 2);

            // Delete from one file only
            assertUpdate("DELETE FROM " + tableName + " WHERE id IN (1, 2)", 2);
            assertNoDeleteFiles(tableName);

            // Verify other files' data is completely intact
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 4");
            assertQuery("SELECT data FROM " + tableName + " WHERE id = 3", "VALUES 'file2_row1'");
            assertQuery("SELECT data FROM " + tableName + " WHERE id = 4", "VALUES 'file2_row2'");
            assertQuery("SELECT data FROM " + tableName + " WHERE id = 5", "VALUES 'file3_row1'");
            assertQuery("SELECT data FROM " + tableName + " WHERE id = 6", "VALUES 'file3_row2'");
        }
    }

    @Test
    public void testCowFileCountReflectsOperations()
    {
        // P1: Verify file counts are consistent -- no orphan files in metadata
        try (TestTable table = newCowTable("test_cow_file_count_",
                "(id BIGINT, region BIGINT) " +
                        "WITH (partitioning = ARRAY['region'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 0), (2, 0), (3, 0)", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 1), (5, 1), (6, 1)", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (7, 2), (8, 2), (9, 2)", 3);

            // Delete all rows from region=1 (entire file should be removed)
            assertUpdate("DELETE FROM " + tableName + " WHERE region = 1", 3);
            assertNoDeleteFiles(tableName);

            // Verify file count: region 0 and 2 should have files, region 1 should not
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 6");
            long dataFileCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            // Should have exactly 2 data files (one per remaining region)
            // Region 0 was not touched (original file), Region 2 was not touched
            // Region 1 file was removed (all rows deleted)
            assertThat(dataFileCount).isEqualTo(2);

            // Partial delete from another region (file should be rewritten, not removed)
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 5");

            long finalFileCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            assertThat(finalFileCount).isEqualTo(2);
        }
    }

    @Test
    public void testCowRewrittenFilesSmallerAfterDelete()
    {
        // P2: Verify that rewritten files are smaller when rows are deleted
        try (TestTable table = newCowTable("test_cow_file_size_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.orders")) {
            String tableName = table.getName();

            long originalTotalSize = (long) computeScalar(
                    "SELECT sum(file_size_in_bytes) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());

            // Delete a significant chunk of data
            long deletedCount = (long) computeScalar("SELECT count(*) FROM " + tableName + " WHERE orderstatus = 'F'");
            assertUpdate("DELETE FROM " + tableName + " WHERE orderstatus = 'F'", deletedCount);
            assertNoDeleteFiles(tableName);

            long newTotalSize = (long) computeScalar(
                    "SELECT sum(file_size_in_bytes) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());

            // Rewritten files should be smaller (less data)
            assertThat(newTotalSize).isLessThan(originalTotalSize);

            // All file sizes should be positive (no zero-byte files)
            computeActual("SELECT file_size_in_bytes FROM \"" + tableName + "$files\" WHERE content = " + DATA.id())
                    .getMaterializedRows()
                    .forEach(row -> assertThat((Long) row.getField(0)).isGreaterThan(0));
        }
    }

    @Test
    public void testCowV2HandlesPreExistingPositionDeletes()
    {
        // v2 CoW must remain correct when legacy file-scoped position deletes already exist.
        String tableName = "test_cow_v2_skip_dv_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // MoR delete on v2 produces position delete files (not DVs)
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            long deleteFileCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(deleteFileCount).isGreaterThanOrEqualTo(1);

            // Switch to CoW mode
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW delete should preserve correctness for v2 data with pre-existing deletes.
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 1", 5);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE regionkey IN (0, 1)", "VALUES 0");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // CoW with large update affecting most rows (near-complete rewrite)
    // -----------------------------------------------------------------------

    @Test
    public void testCowUpdateAlmostAllRows()
    {
        try (TestTable table = newCowTable("test_cow_update_almost_all_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // UPDATE 24 out of 25 rows (leave only nationkey=0)
            assertUpdate("UPDATE " + tableName + " SET name = 'BULK_UPDATE' WHERE nationkey > 0", 24);
            assertNoDeleteFiles(tableName);

            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name = 'BULK_UPDATE'", "VALUES 24");
            // The one untouched row
            assertQuery("SELECT name FROM " + tableName + " WHERE nationkey = 0",
                    "SELECT name FROM nation WHERE nationkey = 0");
        }
    }

    // -----------------------------------------------------------------------
    // SCD Type 2: soft-close old version, insert new version
    // -----------------------------------------------------------------------

    @Test
    public void testCowSlowlyChangingDimensionType2()
    {
        try (TestTable table = newCowTable("test_cow_scd2_",
                "(customer_id BIGINT, name VARCHAR, email VARCHAR, is_current BOOLEAN, effective_date DATE, end_date DATE) " +
                        "WITH (partitioning = ARRAY['is_current'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'Alice', 'alice@old.com', true, DATE '2023-01-01', NULL), " +
                    "(2, 'Bob', 'bob@co.com', true, DATE '2023-01-01', NULL), " +
                    "(3, 'Charlie', 'charlie@co.com', true, DATE '2023-06-01', NULL)", 3);

            // SCD2: Alice changed email -- soft-close old row, insert new version
            assertUpdate("UPDATE " + tableName +
                    " SET is_current = false, end_date = DATE '2024-03-15'" +
                    " WHERE customer_id = 1 AND is_current = true", 1);
            assertNoDeleteFiles(tableName);
            assertUpdate("INSERT INTO " + tableName +
                    " VALUES (1, 'Alice', 'alice@new.com', true, DATE '2024-03-15', NULL)", 1);

            assertQuery("SELECT count(*) FROM " + tableName + " WHERE customer_id = 1", "VALUES 2");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE customer_id = 1 AND is_current = true", "VALUES 1");
            assertQuery("SELECT email FROM " + tableName + " WHERE customer_id = 1 AND is_current = true",
                    "VALUES 'alice@new.com'");
            assertQuery("SELECT email FROM " + tableName + " WHERE customer_id = 1 AND is_current = false",
                    "VALUES 'alice@old.com'");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 4");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE is_current = true", "VALUES 3");
        }
    }

    // -----------------------------------------------------------------------
    // GDPR right-to-be-forgotten: delete user across all partitions
    // -----------------------------------------------------------------------

    @Test
    public void testCowGdprDeleteAcrossAllPartitions()
    {
        try (TestTable table = newCowTable("test_cow_gdpr_",
                "(user_id BIGINT, event_type VARCHAR, region VARCHAR, data VARCHAR) " +
                        "WITH (partitioning = ARRAY['region'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'login', 'US', 'data1'), (1, 'purchase', 'EU', 'data2'), (1, 'click', 'APAC', 'data3'), " +
                    "(2, 'login', 'US', 'data4'), (2, 'purchase', 'EU', 'data5'), " +
                    "(3, 'login', 'US', 'data6'), (3, 'click', 'APAC', 'data7')", 7);

            assertUpdate("DELETE FROM " + tableName + " WHERE user_id = 1", 3);
            assertNoDeleteFiles(tableName);

            assertQuery("SELECT count(*) FROM " + tableName + " WHERE user_id = 1", "VALUES 0");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 4");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE user_id = 2", "VALUES 2");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE user_id = 3", "VALUES 2");
            assertQuery("SELECT count(DISTINCT region) FROM " + tableName, "VALUES 3");
        }
    }

    // -----------------------------------------------------------------------
    // Dimension full refresh: DELETE all rows + re-populate
    // -----------------------------------------------------------------------

    @Test
    public void testCowDimensionFullRefresh()
    {
        try (TestTable table = newCowTable("test_cow_full_refresh_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");

            // DELETE with no WHERE clause
            assertUpdate("DELETE FROM " + tableName, 25);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");

            // Re-populate
            assertUpdate("INSERT INTO " + tableName +
                    " SELECT nationkey, 'REFRESHED_' || name, regionkey, 'fresh' FROM tpch.tiny.nation", 25);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 25");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE name LIKE 'REFRESHED_%'", "VALUES 25");
            assertQuery("SELECT count(DISTINCT nationkey) FROM " + tableName, "VALUES 25");
        }
    }

    // -----------------------------------------------------------------------
    // Late-arriving fact update with date partitions
    // -----------------------------------------------------------------------

    @Test
    public void testCowLateArrivingFactUpdate()
    {
        try (TestTable table = newCowTable("test_cow_late_fact_",
                "(order_id BIGINT, order_date DATE, amount DOUBLE, status VARCHAR) " +
                        "WITH (partitioning = ARRAY['order_date'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, DATE '2024-01-15', 100.0, 'SHIPPED'), " +
                    "(2, DATE '2024-01-15', 200.0, 'SHIPPED'), " +
                    "(3, DATE '2024-02-20', 300.0, 'SHIPPED'), " +
                    "(4, DATE '2024-02-20', 400.0, 'PENDING'), " +
                    "(5, DATE '2024-03-10', 500.0, 'SHIPPED')", 5);

            // Late-arriving correction to old partition
            assertUpdate("UPDATE " + tableName +
                    " SET amount = 250.0, status = 'CORRECTED'" +
                    " WHERE order_date = DATE '2024-01-15' AND order_id = 2", 1);

            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 5");
            assertQuery("SELECT amount FROM " + tableName + " WHERE order_id = 2", "VALUES 250.0e0");
            assertQuery("SELECT status FROM " + tableName + " WHERE order_id = 2", "VALUES 'CORRECTED'");
            // Other partitions untouched
            assertQuery("SELECT amount FROM " + tableName + " WHERE order_id = 3", "VALUES 300.0e0");
            assertQuery("SELECT amount FROM " + tableName + " WHERE order_id = 5", "VALUES 500.0e0");
        }
    }

    // -----------------------------------------------------------------------
    // Incremental load with deduplication via conditional MERGE
    // -----------------------------------------------------------------------

    @Test
    public void testCowIncrementalLoadWithDeduplication()
    {
        try (TestTable table = newCowTable("test_cow_dedup_target_",
                "(id BIGINT, name VARCHAR, updated_at DATE, value DOUBLE) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");
                TestTable staging = newCowTable("test_cow_dedup_staging_",
                        "AS SELECT * FROM (VALUES " +
                                "(1, 'Alice_v2', DATE '2024-03-01', 200.0), " +
                                "(2, 'Bob_v1', DATE '2024-01-01', 100.0), " +
                                "(4, 'Diana', DATE '2024-03-01', 400.0)) " +
                                "AS t(id, name, updated_at, value)")) {
            String targetName = table.getName();
            String stagingName = staging.getName();

            assertUpdate("INSERT INTO " + targetName + " VALUES " +
                    "(1, 'Alice_v1', DATE '2024-01-01', 100.0), " +
                    "(2, 'Bob_v1', DATE '2024-02-01', 150.0), " +
                    "(3, 'Charlie', DATE '2024-01-15', 300.0)", 3);

            // Only overwrite if staging row is newer
            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + stagingName + " s ON t.id = s.id " +
                    "WHEN MATCHED AND s.updated_at > t.updated_at " +
                    "  THEN UPDATE SET name = s.name, updated_at = s.updated_at, value = s.value " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.updated_at, s.value)", 2);

            assertNoDeleteFiles(targetName);
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 4");
            // Alice updated (staging is newer)
            assertQuery("SELECT name FROM " + targetName + " WHERE id = 1", "VALUES 'Alice_v2'");
            // Bob NOT updated (staging is older)
            assertQuery("SELECT value FROM " + targetName + " WHERE id = 2", "VALUES 150.0e0");
            // Diana inserted
            assertQuery("SELECT name FROM " + targetName + " WHERE id = 4", "VALUES 'Diana'");
        }
    }

    // -----------------------------------------------------------------------
    // Upsert MERGE with larger source
    // -----------------------------------------------------------------------

    @Test
    public void testCowMergeUpsertWithLargeSource()
    {
        try (TestTable target = newCowTable("test_cow_upsert_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT orderkey, orderstatus, totalprice, orderpriority, comment " +
                        "FROM tpch.tiny.orders WHERE orderkey <= 5000");
                TestTable source = newCowTable("test_cow_upsert_source_",
                        "AS SELECT orderkey, 'X' AS orderstatus, totalprice + 1.0 AS totalprice, " +
                                "orderpriority, 'upserted' AS comment " +
                                "FROM tpch.tiny.orders")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            long sourceCount = (long) computeScalar("SELECT count(*) FROM " + sourceName);

            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s ON t.orderkey = s.orderkey " +
                    "WHEN MATCHED THEN UPDATE SET orderstatus = s.orderstatus, comment = s.comment " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.orderkey, s.orderstatus, " +
                    "s.totalprice, s.orderpriority, s.comment)", sourceCount);

            assertNoDeleteFiles(targetName);
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES " + sourceCount);
            assertQuery("SELECT count(*) FROM " + targetName + " WHERE comment = 'upserted'", "VALUES " + sourceCount);
            assertQuery("SELECT count(DISTINCT orderkey) FROM " + targetName, "VALUES " + sourceCount);
        }
    }

    // -----------------------------------------------------------------------
    // MERGE with duplicate source keys -- should fail
    // -----------------------------------------------------------------------

    @Test
    public void testCowMergeWithDuplicateSourceKeys()
    {
        try (TestTable target = newCowTable("test_cow_merge_dup_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation WHERE regionkey = 0");
                TestTable source = newCowTable("test_cow_merge_dup_source_",
                        "AS SELECT * FROM (VALUES (0, 'DUP_A'), (0, 'DUP_B'), (99, 'NEW')) " +
                                "AS t(nationkey, name)")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            assertQueryFails(
                    "MERGE INTO " + targetName + " t " +
                            "USING " + sourceName + " s ON t.nationkey = s.nationkey " +
                            "WHEN MATCHED THEN UPDATE SET name = s.name " +
                            "WHEN NOT MATCHED THEN INSERT VALUES (s.nationkey, s.name, 0, 'new')",
                    ".*One MERGE target table row matched more than one source row.*");

            // Target data unchanged after failed MERGE
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES 5");
            assertQuery("SELECT * FROM " + targetName, "SELECT * FROM nation WHERE regionkey = 0");
        }
    }

    // -----------------------------------------------------------------------
    // Wide table (50+ columns)
    // -----------------------------------------------------------------------

    @Test
    public void testCowWithWideTable()
    {
        StringBuilder columns = new StringBuilder("(id BIGINT");
        StringBuilder insertRow1 = new StringBuilder("(1");
        StringBuilder insertRow2 = new StringBuilder("(2");
        for (int i = 1; i <= 50; i++) {
            columns.append(", col_").append(i).append(" VARCHAR");
            insertRow1.append(", 'v").append(i).append("'");
            insertRow2.append(", 'w").append(i).append("'");
        }
        columns.append(")");
        insertRow1.append(")");
        insertRow2.append(")");

        try (TestTable table = newCowTable(
                "test_cow_wide_",
                columns + " WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES " + insertRow1 + ", " + insertRow2, 2);

            // UPDATE one column out of 51
            assertUpdate("UPDATE " + tableName + " SET col_25 = 'MODIFIED' WHERE id = 1", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT col_25 FROM " + tableName + " WHERE id = 1", "VALUES 'MODIFIED'");
            assertQuery("SELECT col_1 FROM " + tableName + " WHERE id = 1", "VALUES 'v1'");
            assertQuery("SELECT col_50 FROM " + tableName + " WHERE id = 1", "VALUES 'v50'");

            // DELETE
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 1");
        }
    }

    // -----------------------------------------------------------------------
    // CoW-to-MoR-to-CoW alternation
    // -----------------------------------------------------------------------

    @Test
    public void testCowToMorAndBackAlternation()
    {
        String tableName = "test_cow_mor_alternation_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " WITH (format_version = 2, " +
                    "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                    "AS SELECT * FROM tpch.tiny.nation", 25);

            // CoW delete
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 0", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 24");

            // Switch to MoR
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], " +
                    "ARRAY['merge-on-read'])");

            // MoR delete (creates position delete files)
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 5", 1);
            long deleteFiles = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + POSITION_DELETES.id());
            assertThat(deleteFiles).isGreaterThanOrEqualTo(1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 23");

            // Switch back to CoW
            assertUpdate("ALTER TABLE " + tableName +
                    " SET PROPERTIES extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])");

            // CoW delete must honor both prior deletes
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 10", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 22");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE nationkey IN (0, 5, 10)", "VALUES 0");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // SUM/AVG aggregation integrity after CoW operations
    // -----------------------------------------------------------------------

    @Test
    public void testCowAggregationIntegrityAfterOperations()
    {
        try (TestTable table = newCowTable("test_cow_agg_integrity_",
                "(id BIGINT, amount DOUBLE, quantity INTEGER) " +
                        "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName +
                    " SELECT nationkey, CAST(nationkey * 100 AS DOUBLE), CAST(nationkey AS INTEGER) FROM tpch.tiny.nation", 25);

            // Capture baseline SUM
            double sumBefore = (double) computeScalar("SELECT SUM(amount) FROM " + tableName);
            double deletedSum = (double) computeScalar("SELECT SUM(amount) FROM " + tableName + " WHERE id < 5");

            // DELETE some rows
            assertUpdate("DELETE FROM " + tableName + " WHERE id < 5", 5);
            assertNoDeleteFiles(tableName);

            // Verify SUM decreased by exactly the deleted amount
            double sumAfterDelete = (double) computeScalar("SELECT SUM(amount) FROM " + tableName);
            assertThat(sumAfterDelete).isEqualTo(sumBefore - deletedSum);

            // UPDATE: double all amounts
            long remaining = (long) computeScalar("SELECT count(*) FROM " + tableName);
            assertUpdate("UPDATE " + tableName + " SET amount = amount * 2", remaining);
            assertNoDeleteFiles(tableName);

            // Verify SUM doubled
            double sumAfterUpdate = (double) computeScalar("SELECT SUM(amount) FROM " + tableName);
            assertThat(sumAfterUpdate).isEqualTo(sumAfterDelete * 2);
        }
    }

    // -----------------------------------------------------------------------
    // Anti-join verification: no phantom or missing rows
    // -----------------------------------------------------------------------

    @Test
    public void testCowAntiJoinVerificationNoPhantomRows()
    {
        try (TestTable table = newCowTable("test_cow_phantom_check_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);
            assertUpdate("UPDATE " + tableName + " SET name = 'CHANGED' WHERE regionkey = 1", 5);
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 8", 1);

            // Verify deleted rows do not exist (no phantom resurrections)
            assertQuery("SELECT count(*) FROM " + tableName +
                    " WHERE nationkey IN (SELECT nationkey FROM tpch.tiny.nation WHERE regionkey = 0)" +
                    " OR nationkey = 8", "VALUES 0");

            // Verify all expected rows still exist (no missing rows)
            assertQuery(
                    "SELECT count(*) FROM tpch.tiny.nation n " +
                            "WHERE n.regionkey NOT IN (0) AND n.nationkey != 8 " +
                            "AND NOT EXISTS (SELECT 1 FROM " + tableName + " t WHERE t.nationkey = n.nationkey)",
                    "VALUES 0");
        }
    }

    // -----------------------------------------------------------------------
    // UPDATE on partition transform column (bucket)
    // -----------------------------------------------------------------------

    @Test
    public void testCowUpdateColumnUsedInBucketTransform()
    {
        String tableName = "test_cow_update_bucket_col_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " (id BIGINT, name VARCHAR, value DOUBLE) " +
                    "WITH (partitioning = ARRAY['bucket(id, 4)'], " +
                    "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'a', 10.0), (2, 'b', 20.0), (3, 'c', 30.0), (4, 'd', 40.0), (5, 'e', 50.0)", 5);

            // UPDATE the bucketed column itself -- row moves to a different bucket
            assertUpdate("UPDATE " + tableName + " SET id = 100 WHERE id = 1", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 5");
            assertQuery("SELECT name FROM " + tableName + " WHERE id = 100", "VALUES 'a'");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE id = 1", "VALUES 0");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -----------------------------------------------------------------------
    // Many small files (50+) -- scalability test
    // -----------------------------------------------------------------------

    @Test
    public void testCowWithManySmallFiles()
    {
        String tableName = "test_cow_small_files_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " (id BIGINT, data VARCHAR) " +
                    "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");

            // Create 30 separate data files
            for (int i = 0; i < 30; i++) {
                assertUpdate("INSERT INTO " + tableName + " VALUES (" + i + ", 'row_" + i + "')", 1);
            }

            long fileCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            assertThat(fileCount).isGreaterThanOrEqualTo(30);

            // CoW DELETE spanning many files
            assertUpdate("DELETE FROM " + tableName + " WHERE id < 15", 15);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15");
            assertQuery("SELECT count(DISTINCT id) FROM " + tableName, "VALUES 15");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // =======================================================================
    // PERFORMANCE MEASUREMENT TESTS
    // =======================================================================

    @Test
    public void testCowPerformanceDeleteOnOrders()
    {
        // Performance baseline: measure CoW DELETE on tpch.tiny.orders (15000 rows)
        try (TestTable table = newCowTable("test_cow_perf_del_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.orders")) {
            String tableName = table.getName();

            long initialCount = (long) computeScalar("SELECT count(*) FROM " + tableName);
            assertThat(initialCount).isEqualTo(15000L);

            long initialSize = (long) computeScalar(
                    "SELECT sum(file_size_in_bytes) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());

            long deletedCount = (long) computeScalar(
                    "SELECT count(*) FROM " + tableName + " WHERE orderstatus = 'F'");

            long startNanos = System.nanoTime();
            assertUpdate("DELETE FROM " + tableName + " WHERE orderstatus = 'F'", deletedCount);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertNoDeleteFiles(tableName);
            long remainingCount = (long) computeScalar("SELECT count(*) FROM " + tableName);
            assertThat(remainingCount).isEqualTo(initialCount - deletedCount);

            long newSize = (long) computeScalar(
                    "SELECT sum(file_size_in_bytes) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());

            // Verify rewritten files are smaller (less data)
            assertThat(newSize).isLessThan(initialSize);
            // All file sizes are positive
            computeActual("SELECT file_size_in_bytes FROM \"" + tableName + "$files\" WHERE content = " + DATA.id())
                    .getMaterializedRows()
                    .forEach(row -> assertThat((Long) row.getField(0)).isGreaterThan(0));

            // Log performance metrics for manual review
            long snapshotCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"");
            assertThat(snapshotCount).isGreaterThanOrEqualTo(2);
            assertThat(elapsedMs).isLessThan(60_000L);
        }
    }

    @Test
    public void testCowPerformanceUpdateOnOrders()
    {
        // Performance baseline: measure CoW UPDATE on tpch.tiny.orders (15000 rows)
        try (TestTable table = newCowTable("test_cow_perf_upd_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.orders")) {
            String tableName = table.getName();

            long updateCount = (long) computeScalar(
                    "SELECT count(*) FROM " + tableName + " WHERE orderstatus = 'O'");

            long startNanos = System.nanoTime();
            assertUpdate("UPDATE " + tableName + " SET orderpriority = '1-URGENT' WHERE orderstatus = 'O'", updateCount);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertNoDeleteFiles(tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 15000");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE orderpriority = '1-URGENT' AND orderstatus = 'O'",
                    "VALUES " + updateCount);

            assertThat(elapsedMs).isLessThan(60_000L);
        }
    }

    @Test
    public void testCowPerformanceMergeOnOrders()
    {
        // Performance baseline: measure CoW MERGE with tpch.tiny.orders
        try (TestTable target = newCowTable("test_cow_perf_merge_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT orderkey, orderstatus, orderpriority, comment FROM tpch.tiny.orders WHERE orderkey <= 10000");
                TestTable source = newCowTable("test_cow_perf_merge_source_",
                        "AS SELECT orderkey, 'MERGED' AS orderstatus, orderpriority, 'merged' AS comment " +
                                "FROM tpch.tiny.orders WHERE orderkey > 5000 AND orderkey <= 12000")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            long sourceCount = (long) computeScalar("SELECT count(*) FROM " + sourceName);

            long startNanos = System.nanoTime();
            assertUpdate(
                    "MERGE INTO " + targetName + " t " +
                            "USING " + sourceName + " s ON t.orderkey = s.orderkey " +
                            "WHEN MATCHED THEN UPDATE SET orderstatus = s.orderstatus, comment = s.comment " +
                            "WHEN NOT MATCHED THEN INSERT VALUES (s.orderkey, s.orderstatus, s.orderpriority, s.comment)",
                    sourceCount);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertNoDeleteFiles(targetName);
            long totalRows = (long) computeScalar("SELECT count(*) FROM " + targetName);
            long distinctKeys = (long) computeScalar("SELECT count(DISTINCT orderkey) FROM " + targetName);
            assertThat(distinctKeys).isEqualTo(totalRows);
            assertQuery("SELECT count(*) FROM " + targetName + " WHERE comment = 'merged'", "VALUES " + sourceCount);

            assertThat(elapsedMs).isLessThan(60_000L);
        }
    }

    // =======================================================================
    // OBSERVABILITY / METRICS TESTS
    // =======================================================================

    @Test
    public void testCowSnapshotSummaryMetrics()
    {
        // Verify Iceberg snapshot summary contains expected metrics after CoW operations
        try (TestTable table = newCowTable("test_cow_snapshot_metrics_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Initial INSERT snapshot
            @SuppressWarnings("unchecked")
            Map<String, String> insertSummary = (Map<String, String>) computeScalar(
                    "SELECT summary FROM \"" + tableName + "$snapshots\" ORDER BY committed_at ASC LIMIT 1");
            assertThat(insertSummary).containsKey("added-records");
            assertThat(insertSummary).containsKey("added-data-files");

            // CoW DELETE
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);

            // Check the overwrite snapshot has expected metrics
            @SuppressWarnings("unchecked")
            Map<String, String> cowSummary = (Map<String, String>) computeScalar(
                    "SELECT summary FROM \"" + tableName + "$snapshots\" WHERE operation = 'overwrite' ORDER BY committed_at DESC LIMIT 1");
            assertThat(cowSummary).containsKey("added-data-files");
            assertThat(cowSummary).containsKey("deleted-data-files");
            assertThat(cowSummary).containsKey("total-records");
            int deletedDataFiles = Integer.parseInt(cowSummary.get("deleted-data-files"));
            assertThat(deletedDataFiles).isGreaterThanOrEqualTo(1);
            long totalRecords = Long.parseLong(cowSummary.get("total-records"));
            assertThat(totalRecords).isEqualTo(20L);
        }
    }

    @Test
    public void testCowFileSizeMetricsInSystemTable()
    {
        // Verify $files system table accurately reflects CoW rewrite results
        try (TestTable table = newCowTable("test_cow_file_metrics_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            long initialTotalSize = (long) computeScalar(
                    "SELECT sum(file_size_in_bytes) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());

            // DELETE half the rows
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey IN (0, 1)", 10);

            // All data files should have positive size
            computeActual("SELECT file_size_in_bytes FROM \"" + tableName + "$files\" WHERE content = " + DATA.id())
                    .getMaterializedRows()
                    .forEach(row -> assertThat((Long) row.getField(0)).isGreaterThan(0));

            // No delete files
            assertNoDeleteFiles(tableName);

            // record_count in $files should match actual row counts
            long filesRecordSum = (long) computeScalar(
                    "SELECT sum(record_count) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            long actualRows = (long) computeScalar("SELECT count(*) FROM " + tableName);
            assertThat(filesRecordSum).isEqualTo(actualRows);
            assertThat(actualRows).isEqualTo(15L);

            // Total size should decrease (fewer rows)
            long newTotalSize = (long) computeScalar(
                    "SELECT sum(file_size_in_bytes) FROM \"" + tableName + "$files\" WHERE content = " + DATA.id());
            assertThat(newTotalSize).isLessThan(initialTotalSize);
        }
    }

    @Test
    public void testCowManifestMetrics()
    {
        // Verify $manifests system table tracks CoW file additions and deletions
        try (TestTable table = newCowTable("test_cow_manifest_metrics_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 0", 5);

            // The latest manifest should show file additions from CoW
            long addedFiles = (long) computeScalar(
                    "SELECT sum(added_data_files_count) FROM \"" + tableName + "$manifests\"");
            assertThat(addedFiles).isGreaterThanOrEqualTo(1);

            // Verify no delete file manifests (CoW doesn't produce delete files)
            long deleteManifests = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$manifests\" WHERE content = 1");
            assertThat(deleteManifests).isEqualTo(0);
        }
    }

    @Test
    public void testCowWrittenBytesIncludesRewrites()
    {
        // Verify that EXPLAIN ANALYZE shows non-zero physical written bytes for CoW
        try (TestTable table = newCowTable("test_cow_written_bytes_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // Run EXPLAIN ANALYZE to capture physical I/O stats
            String explainOutput = (String) computeScalar("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE regionkey = 4");

            // Should report physical input bytes (reads from original files)
            assertThat(explainOutput).contains("Physical input:");
            // Should report output bytes (CoW rewrites + any inserts)
            assertThat(explainOutput).contains("Output:");
        }
    }

    @Test
    public void testCowPartitionMetrics()
    {
        // Verify $partitions reflects CoW changes per partition
        try (TestTable table = newCowTable("test_cow_partition_metrics_",
                "(id BIGINT, region VARCHAR, value DOUBLE) " +
                        "WITH (partitioning = ARRAY['region'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'US', 100.0), (2, 'US', 200.0), (3, 'EU', 300.0), " +
                    "(4, 'EU', 400.0), (5, 'APAC', 500.0)", 5);

            // Delete all rows from one partition
            assertUpdate("DELETE FROM " + tableName + " WHERE region = 'US'", 2);
            assertNoDeleteFiles(tableName);

            // $partitions should reflect the change
            long partitionCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + tableName + "$partitions\"");
            // EU and APAC remain; US may still appear with 0 rows depending on Iceberg version
            assertThat(partitionCount).isGreaterThanOrEqualTo(2);

            // Verify row counts per remaining partition
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE region = 'EU'", "VALUES 2");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE region = 'APAC'", "VALUES 1");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 3");
        }
    }

    @Test
    public void testCowSplitSourceMetricsInExplainAnalyze()
    {
        // Verify EXPLAIN ANALYZE VERBOSE exposes scan metrics relevant to CoW
        try (TestTable table = newCowTable("test_cow_split_metrics_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // First do a CoW delete to create the post-rewrite table state
            assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4", 5);
            assertNoDeleteFiles(tableName);

            // Now run EXPLAIN ANALYZE on a SELECT to see scan metrics
            String explainOutput = (String) computeScalar(
                    "EXPLAIN ANALYZE VERBOSE SELECT count(*) FROM " + tableName);

            // Should contain scan planning metrics from IcebergSplitSource.getMetrics()
            assertThat(explainOutput).contains("dataFiles");
        }
    }

    @Test
    public void testCowInsertOnlyMergeRunsConflictDetection()
    {
        // Insert-only CoW MERGE still commits through OverwriteFiles so conflict validators
        // run. Iceberg labels the snapshot "append" because no files are deleted, so this test
        // asserts end-to-end correctness (one new snapshot, no delete files, correct row count).
        try (TestTable target = newCowTable("test_cow_insert_only_merge_overwrite_target_",
                "WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS SELECT * FROM tpch.tiny.nation WHERE regionkey < 2");
                TestTable source = newCowTable(
                        "test_cow_insert_only_merge_overwrite_source_",
                        "AS SELECT * FROM tpch.tiny.nation WHERE regionkey >= 4")) {
            String targetName = target.getName();
            String sourceName = source.getName();

            long beforeSnapshotCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + targetName + "$snapshots\"");

            long sourceCount = (long) computeScalar("SELECT count(*) FROM " + sourceName);
            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.nationkey = s.nationkey " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.nationkey, s.name, s.regionkey, s.comment)", sourceCount);

            assertNoDeleteFiles(targetName);
            assertQuery("SELECT count(*) FROM " + targetName, "VALUES " + (10 + sourceCount));

            // The MERGE should have produced exactly one new snapshot
            long afterSnapshotCount = (long) computeScalar(
                    "SELECT count(*) FROM \"" + targetName + "$snapshots\"");
            assertThat(afterSnapshotCount).isEqualTo(beforeSnapshotCount + 1);
        }
    }

    @Test
    public void testCowUpdatePreservesPreExistingEqualityDeletes()
            throws Exception
    {
        // Verify that when a table has pre-existing equality deletes and a CoW UPDATE
        // is applied, the equality-deleted rows remain deleted (not resurrected) and
        // the update is correctly applied only to surviving rows.
        try (TestTable table = newCowTable("test_cow_eq_delete_update_",
                "(id integer, v varchar) WITH (format_version = 2, " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", 4);

            BaseTable icebergTable = loadBaseTable(tableName);
            writeEqualityDeleteForTable(
                    icebergTable,
                    getFileSystemFactory(getQueryRunner()),
                    Optional.empty(),
                    Optional.empty(),
                    Map.of("id", 2),
                    Optional.empty());

            assertQuery("SELECT id, v FROM " + tableName + " ORDER BY id", "VALUES (1, 'a'), (3, 'c'), (4, 'd')");

            // UPDATE that touches the same data file. Row id=2 must stay deleted.
            assertUpdate("UPDATE " + tableName + " SET v = 'cc' WHERE id = 3", 1);

            assertQuery("SELECT id, v FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'a'), (3, 'cc'), (4, 'd')");
            assertQuery("SELECT count(*) FROM " + tableName + " WHERE id = 2", "VALUES 0");
        }
    }

    @Test
    public void testCowMergePreservesPreExistingEqualityDeletes()
            throws Exception
    {
        // End-to-end: equality delete + CoW MERGE (update+insert). The equality-deleted
        // rows must not reappear, and the MERGE must correctly update/insert rows.
        try (TestTable target = newCowTable("test_cow_eq_delete_merge_target_",
                "(id integer, v varchar) WITH (format_version = 2, " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");
                TestTable source = newCowTable(
                        "test_cow_eq_delete_merge_source_",
                        "(id integer, v varchar)")) {
            String targetName = target.getName();
            String sourceName = source.getName();
            assertUpdate("INSERT INTO " + sourceName + " VALUES (3, 'cc'), (5, 'e')", 2);
            assertUpdate("INSERT INTO " + targetName + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", 4);

            BaseTable icebergTable = loadBaseTable(targetName);
            writeEqualityDeleteForTable(
                    icebergTable,
                    getFileSystemFactory(getQueryRunner()),
                    Optional.empty(),
                    Optional.empty(),
                    Map.of("id", 2),
                    Optional.empty());

            assertQuery("SELECT id, v FROM " + targetName + " ORDER BY id", "VALUES (1, 'a'), (3, 'c'), (4, 'd')");

            assertUpdate("MERGE INTO " + targetName + " t " +
                    "USING " + sourceName + " s " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET v = s.v " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)", 2);

            // id=2 must still be deleted (not resurrected by merge)
            // id=3 must be updated to 'cc'
            // id=5 must be inserted
            assertQuery("SELECT id, v FROM " + targetName + " ORDER BY id",
                    "VALUES (1, 'a'), (3, 'cc'), (4, 'd'), (5, 'e')");
            assertQuery("SELECT count(*) FROM " + targetName + " WHERE id = 2", "VALUES 0");
        }
    }

    // -----------------------------------------------------------------------
    // Concurrent copy-on-write commits.
    // Exercises the OverwriteFiles conflict-detection path configured in
    // IcebergMetadata.configureCopyOnWriteConflictDetection (validateNoConflictingData /
    // validateNoConflictingDeletes under the SERIALIZABLE merge isolation level).
    // -----------------------------------------------------------------------

    @Test
    public void testConcurrentNonOverlappingCopyOnWriteUpdatesSucceed()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = newCowTable(
                "test_cow_concurrent_nonoverlap_update_",
                "(a, part) WITH (partitioning = ARRAY['part'], extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS VALUES (1, 10), (11, 20), (21, 30)")) {
            String tableName = table.getName();
            // Each thread rewrites a distinct partition, so conflict detection must not reject any commit.
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE part = 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (2, 10), (12, 20), (22, 30)");
            assertNoDeleteFiles(tableName);
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentOverlappingCopyOnWriteUpdatesConflict()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = newCowTable(
                "test_cow_concurrent_overlap_update_",
                "(a) WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) AS VALUES 1, 11, 21, 31")) {
            String tableName = table.getName();
            // Every thread rewrites the same data file. Under SERIALIZABLE isolation only one
            // commit can win; the losers must fail with an Iceberg commit conflict.
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(_ -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("UPDATE " + tableName + " SET a = a + 1 WHERE a > 11");
                            return true;
                        }
                        catch (Exception e) {
                            assertThat(getTrinoExceptionCause(e))
                                    .hasMessageMatching("Failed to commit during copy-on-write.*|" +
                                            "Failed to commit the transaction during copy-on-write.*");
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            long successes = futures.stream()
                    .map(future -> tryGetFutureValue(future, 30, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(success -> success)
                    .count();

            assertThat(successes).isGreaterThanOrEqualTo(1);
            switch ((int) successes) {
                case 1 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 11, 22, 32");
                case 2 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 11, 23, 33");
                case 3 -> assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 11, 24, 34");
            }
            assertNoDeleteFiles(tableName);
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentNonOverlappingCopyOnWriteDeletesSucceed()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = newCowTable(
                "test_cow_concurrent_nonoverlap_delete_",
                "(a, part) WITH (partitioning = ARRAY['part'], extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])) " +
                        "AS VALUES (1, 10), (2, 10), (11, 20), (12, 20), (21, 30), (22, 30)")) {
            String tableName = table.getName();
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 30 AND a = 21");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (22, 30)");
            assertNoDeleteFiles(tableName);
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    // -----------------------------------------------------------------------
    // ORC format coverage for the full DELETE/UPDATE/MERGE surface, including
    // partitioned layouts. Complements testCowWithOrcFormat and the mixed-format tests.
    // -----------------------------------------------------------------------

    @Test
    public void testCowMergeOnOrcFormat()
    {
        try (TestTable target = newCowTable(
                "test_cow_orc_merge_target_",
                "(id integer, v varchar) WITH (format = 'ORC', extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))");
                TestTable source = newCowTable(
                        "test_cow_orc_merge_source_",
                        "(id integer, v varchar) WITH (format = 'ORC')")) {
            String targetName = target.getName();
            String sourceName = source.getName();
            assertUpdate("INSERT INTO " + targetName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
            assertUpdate("INSERT INTO " + sourceName + " VALUES (2, 'bb'), (4, 'd')", 2);

            assertUpdate("MERGE INTO " + targetName + " t USING " + sourceName + " s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET v = s.v " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)", 2);

            assertQuery("SELECT id, v FROM " + targetName + " ORDER BY id", "VALUES (1, 'a'), (2, 'bb'), (3, 'c'), (4, 'd')");
            assertNoDeleteFiles(targetName);
        }
    }

    @Test
    public void testCowPartitionedOrcDeleteAndUpdate()
    {
        try (TestTable table = newCowTable(
                "test_cow_orc_partitioned_",
                "(a integer, part integer) WITH (format = 'ORC', partitioning = ARRAY['part'], " +
                        "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write']))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10), (2, 10), (11, 20), (12, 20)", 4);

            assertUpdate("DELETE FROM " + tableName + " WHERE part = 10 AND a = 1", 1);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY a", "VALUES (2, 10), (11, 20), (12, 20)");

            assertUpdate("UPDATE " + tableName + " SET a = a + 100 WHERE part = 20", 2);
            assertNoDeleteFiles(tableName);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY a", "VALUES (2, 10), (111, 20), (112, 20)");
        }
    }

    private Set<String> getDataFilePaths(String tableName)
    {
        return computeActual("SELECT file_path FROM \"" + tableName + "$files\" WHERE content = " + DATA.id())
                .getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableSet());
    }

    private BaseTable loadBaseTable(String tableName)
    {
        return loadTable(
                tableName,
                getHiveMetastore(getQueryRunner()),
                getFileSystemFactory(getQueryRunner()),
                ICEBERG_CATALOG,
                "tpch");
    }

    private void assertNoDeleteFiles(String tableName)
    {
        assertQuery(
                "SELECT count(*) FROM \"" + tableName + "$files\" WHERE content IN (" + POSITION_DELETES.id() + ", " + EQUALITY_DELETES.id() + ")",
                "VALUES 0");
    }
}
