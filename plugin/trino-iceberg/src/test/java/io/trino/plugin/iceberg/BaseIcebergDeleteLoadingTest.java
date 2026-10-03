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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileContent;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.tpch.TpchTable.NATION;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies reading Iceberg tables with delete files. Subclasses choose {@code iceberg.delete-loading-threads},
 * which selects between reading the delete files on a shared thread pool and reading them on the thread
 * reading the split.
 */
public abstract class BaseIcebergDeleteLoadingTest
        extends AbstractTestQueryFramework
{
    private static final int DELETE_FILE_COUNT = 8;
    private static final int NATION_ROW_COUNT = 25;

    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    /**
     * Value for {@code iceberg.delete-loading-threads}, where 0 reads the delete files on the split thread.
     */
    protected abstract int deleteLoadingThreads();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.delete-loading-threads", Integer.toString(deleteLoadingThreads()))
                .setInitialTables(NATION)
                .build();
        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);
        return queryRunner;
    }

    @Test
    void testMultiplePositionDeleteFilesForSingleDataFile()
    {
        // format_version 2 writes position deletes.
        try (TestTable table = newTrinoTable(
                "test_delete_loading_position_",
                "WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            for (int nationkey = 0; nationkey < DELETE_FILE_COUNT; nationkey++) {
                assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = " + nationkey, 1);
            }
            assertThat(deleteFileCount(tableName, POSITION_DELETES)).isEqualTo(DELETE_FILE_COUNT);

            assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE nationkey >= " + DELETE_FILE_COUNT);
            assertThat(computeScalar("SELECT count(*) FROM " + tableName)).isEqualTo((long) (NATION_ROW_COUNT - DELETE_FILE_COUNT));
            assertQuery("SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE nationkey >= " + DELETE_FILE_COUNT);
        }
    }

    @Test
    void testDeletionVectors()
    {
        // format_version 3 writes deletion vectors instead of position delete files.
        try (TestTable table = newTrinoTable(
                "test_delete_loading_deletion_vector_",
                "(id BIGINT) WITH (format_version = 3)")) {
            String tableName = table.getName();

            // one INSERT per data file, so the table is read by several splits each with its own deletion vector
            int rowsPerDataFile = 10;
            int lastId = DELETE_FILE_COUNT * rowsPerDataFile;
            for (int firstId = 1; firstId <= lastId; firstId += rowsPerDataFile) {
                assertUpdate(
                        "INSERT INTO " + tableName + " SELECT x FROM UNNEST(sequence(%s, %s)) t(x)".formatted(firstId, firstId + rowsPerDataFile - 1),
                        rowsPerDataFile);
            }

            // every data file contains both deleted and retained rows, so each one gets a deletion vector
            assertUpdate("DELETE FROM " + tableName + " WHERE id % 2 = 0", lastId / 2);
            assertThat(query("SELECT count_if(file_format = 'PUFFIN') FROM \"%s$files\" WHERE content = %s".formatted(tableName, POSITION_DELETES.id())))
                    .matches("VALUES BIGINT '%s'".formatted(DELETE_FILE_COUNT));

            assertRetainedIds(tableName, lastId, "x % 2 = 1");

            // deleting again rewrites each deletion vector, so the merged contents have to be read back correctly
            long remainingMultiplesOfThree = (long) computeScalar("SELECT count(*) FROM " + tableName + " WHERE id % 3 = 0");
            assertUpdate("DELETE FROM " + tableName + " WHERE id % 3 = 0", remainingMultiplesOfThree);
            assertRetainedIds(tableName, lastId, "x % 2 = 1 AND x % 3 <> 0");
        }
    }

    @Test
    void testMultipleEqualityDeleteFilesSharedByMultipleDataFiles()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_delete_loading_equality_", "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation", NATION_ROW_COUNT);

            BaseTable icebergTable = loadTable(tableName);
            for (long regionkey = 0; regionkey < 3; regionkey++) {
                writeEqualityDeleteForTable(
                        icebergTable,
                        fileSystemFactory,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of("regionkey", regionkey),
                        Optional.empty());
            }
            assertThat(deleteFileCount(tableName, EQUALITY_DELETES)).isEqualTo(3);

            assertQuery(
                    "SELECT * FROM " + tableName,
                    "SELECT * FROM nation WHERE regionkey >= 3 UNION ALL SELECT * FROM nation WHERE regionkey >= 3");
        }
    }

    /**
     * A query that finishes early abandons the equality delete loads its splits started. Reading the table again
     * afterwards must still work: abandoning a load must not poison the cache for later scans, and must not touch
     * the delete loading executor, which is shared by every scan of the catalog.
     */
    @Test
    void testEarlyTerminationWithEqualityDeletes()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_delete_loading_early_exit_", "AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation", NATION_ROW_COUNT);

            BaseTable icebergTable = loadTable(tableName);
            for (long regionkey = 0; regionkey < 3; regionkey++) {
                writeEqualityDeleteForTable(
                        icebergTable,
                        fileSystemFactory,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of("regionkey", regionkey),
                        Optional.empty());
            }
            assertThat(deleteFileCount(tableName, EQUALITY_DELETES)).isEqualTo(3);

            // the splits are closed as soon as the limit is met, while their delete loads may still be running
            assertThat(computeActual("SELECT * FROM " + tableName + " LIMIT 5").getRowCount()).isEqualTo(5);

            assertQuery(
                    "SELECT * FROM " + tableName,
                    "SELECT * FROM nation WHERE regionkey >= 3 UNION ALL SELECT * FROM nation WHERE regionkey >= 3");
        }
    }

    @Test
    void testPositionAndEqualityDeleteFilesTogether()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_delete_loading_mixed_",
                "WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation")) {
            String tableName = table.getName();

            // nationkey 0 is in regionkey 0, so the position delete and the equality delete overlap
            for (int nationkey = 0; nationkey < DELETE_FILE_COUNT; nationkey++) {
                assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = " + nationkey, 1);
            }

            BaseTable icebergTable = loadTable(tableName);
            writeEqualityDeleteForTable(
                    icebergTable,
                    fileSystemFactory,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of("regionkey", 0L),
                    Optional.empty());

            assertThat(deleteFileCount(tableName, POSITION_DELETES)).isEqualTo(DELETE_FILE_COUNT);
            assertThat(deleteFileCount(tableName, EQUALITY_DELETES)).isEqualTo(1);

            assertQuery(
                    "SELECT * FROM " + tableName,
                    "SELECT * FROM nation WHERE nationkey >= " + DELETE_FILE_COUNT + " AND regionkey != 0");
        }
    }

    /**
     * Asserts that the table holds exactly the ids from 1 to lastId.
     */
    private void assertRetainedIds(String tableName, int lastId, String predicate)
    {
        assertThat(query("SELECT id FROM " + tableName))
                .matches("SELECT x FROM UNNEST(sequence(1, %s)) t(x) WHERE %s".formatted(lastId, predicate));
    }

    private long deleteFileCount(String tableName, FileContent content)
    {
        return (long) computeScalar("SELECT count(*) FROM \"%s$files\" WHERE content = %s".formatted(tableName, content.id()));
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }
}
