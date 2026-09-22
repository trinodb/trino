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
package io.trino.plugin.lakehouse;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static java.nio.file.Files.createTempDirectory;

final class TestLakehouseProcedures
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = LakehouseQueryRunner.builder()
                .addLakehouseProperty("hive.metastore", "file")
                .addLakehouseProperty("hive.metastore.catalog.dir", createTempDirectory("lakehouse_procedures").toUri().toString())
                .addLakehouseProperty("hive.metastore-cache-ttl", "1m")
                .addLakehouseProperty("fs.hadoop.enabled", "true")
                .build();
        queryRunner.execute("CREATE SCHEMA lakehouse.tpch");
        return queryRunner;
    }

    @Test
    void testOptimizeIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_optimize_iceberg", "(id integer)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize");

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize (sorted_by => ARRAY['id'])");
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE \"OPTIMIZE\" (\"SORTED_BY\" => ARRAY['id'])");
        }
    }

    @Test
    void testOptimizeHiveTable()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("lakehouse", "non_transactional_optimize_enabled", "true")
                .build();
        try (TestTable table = newTrinoTable("test_optimize_hive", "WITH (type = 'HIVE', format = 'PARQUET') AS SELECT 1 id")) {
            assertUpdate(session, "ALTER TABLE " + table.getName() + " EXECUTE optimize");

            assertQueryFails(session, "ALTER TABLE " + table.getName() + " EXECUTE optimize (sorted_by => ARRAY['id'])", "sorted_by option not supported for HIVE tables: OPTIMIZE");
            assertQueryFails(session, "ALTER TABLE " + table.getName() + " EXECUTE \"OPTIMIZE\" (\"SORTED_BY\" => ARRAY['id'])", "sorted_by option not supported for HIVE tables: OPTIMIZE");
        }
    }

    @Test
    void testOptimizeDeltaTable()
    {
        try (TestTable table = newTrinoTable("test_optimize_delta", "WITH (type = 'DELTA') AS SELECT 1 id")) {
            // the local file system provides no Delta transaction log synchronizer, so the statement resolves and then fails on the write
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE optimize", "Writes are not enabled on the file filesystem.*");

            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE optimize (sorted_by => ARRAY['id'])", "sorted_by option not supported for DELTA tables: OPTIMIZE");
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE \"OPTIMIZE\" (\"SORTED_BY\" => ARRAY['id'])", "sorted_by option not supported for DELTA tables: OPTIMIZE");
        }
    }

    @Test
    void testExpireSnapshotsIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_expire_snapshots_iceberg", "(id integer)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE expire_snapshots(retention_threshold => '7d')");
        }
    }

    @Test
    void testExpireSnapshotsHiveTable()
    {
        try (TestTable table = newTrinoTable("test_expire_snapshots_hive", "WITH (type = 'HIVE', format = 'PARQUET') AS SELECT 1 id")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " EXECUTE expire_snapshots(retention_threshold => '7d')",
                    "Table procedure not supported for HIVE tables: EXPIRE_SNAPSHOTS");
        }
    }

    @Test
    void testRemoveOrphanFilesIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_remove_orphan_files_iceberg", "(id integer)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE remove_orphan_files(retention_threshold => '7d')");
        }
    }

    @Test
    void testRemoveOrphanFilesDeltaTable()
    {
        try (TestTable table = newTrinoTable("test_remove_orphan_files_delta", "WITH (type = 'DELTA') AS SELECT 1 id")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " EXECUTE remove_orphan_files(retention_threshold => '7d')",
                    "Table procedure not supported for DELTA tables: REMOVE_ORPHAN_FILES");
        }
    }

    @Test
    void testRollbackToSnapshotIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_rollback_to_snapshot_iceberg", "(id integer)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            long snapshotId = (long) computeScalar("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1".formatted(table.getName()));
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            assertUpdate("ALTER TABLE %s EXECUTE rollback_to_snapshot(%s)".formatted(table.getName(), snapshotId));
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 1");
        }
    }

    @Test
    void testDropExtendedStatsIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_drop_extended_stats_iceberg", "(id integer)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE drop_extended_stats");
        }
    }

    @Test
    void testDropExtendedStatsHiveTable()
    {
        try (TestTable table = newTrinoTable("test_drop_extended_stats_hive", "WITH (type = 'HIVE', format = 'PARQUET') AS SELECT 1 id")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " EXECUTE drop_extended_stats",
                    "Table procedure not supported for HIVE tables: DROP_EXTENDED_STATS");
        }
    }

    @Test
    void testVacuumDeltaTable()
    {
        try (TestTable table = newTrinoTable("test_vacuum_delta", "WITH (type = 'DELTA') AS SELECT 1 id")) {
            assertUpdate("CALL system.vacuum(CURRENT_SCHEMA, '%s', '7d')".formatted(table.getName()));
        }
    }

    @Test
    void testVacuumIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_vacuum_iceberg", "(id integer)")) {
            assertQueryFails("CALL system.vacuum(CURRENT_SCHEMA, '%s', '7d')".formatted(table.getName()), ".* is not a Delta Lake table");
        }
    }

    @Test
    void testDropStatsHiveTable()
    {
        try (TestTable table = newTrinoTable("test_drop_stats_hive", "WITH (type = 'HIVE', format = 'PARQUET') AS SELECT 1 id")) {
            assertUpdate("CALL system.drop_stats(CURRENT_SCHEMA, '%s')".formatted(table.getName()));
        }
    }

    @Test
    void testDropStatsIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_drop_stats_iceberg", "(id integer)")) {
            assertQueryFails("CALL system.drop_stats(CURRENT_SCHEMA, '%s')".formatted(table.getName()), "Cannot query Iceberg table .*");
        }
    }

    @Test
    void testSyncPartitionMetadataHiveTable()
    {
        try (TestTable table = newTrinoTable("test_sync_partitions_hive", "WITH (type = 'HIVE', format = 'PARQUET', partitioned_by = ARRAY['part']) AS SELECT 1 id, 'a' part")) {
            assertUpdate("CALL system.sync_partition_metadata(schema_name => CURRENT_SCHEMA, table_name => '%s', mode => 'FULL')".formatted(table.getName()));
        }
    }

    @Test
    void testSyncPartitionMetadataIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_sync_partitions_iceberg", "(id integer)")) {
            assertQueryFails(
                    "CALL system.sync_partition_metadata(schema_name => CURRENT_SCHEMA, table_name => '%s', mode => 'FULL')".formatted(table.getName()),
                    "Not a Hive table .*");
        }
    }

    @Test
    void testProceduresWithClashingNameAreNotExposed()
    {
        assertQueryFails("CALL system.flush_metadata_cache()", "Procedure not registered: system.flush_metadata_cache");
        assertQueryFails("CALL system.register_table(CURRENT_SCHEMA, 'test', 'local:///test')", "Procedure not registered: system.register_table");
        assertQueryFails("CALL system.unregister_table(CURRENT_SCHEMA, 'test')", "Procedure not registered: system.unregister_table");
    }

    @Test
    void testDeprecatedProcedureIsNotExposed()
    {
        assertQueryFails("CALL system.rollback_to_snapshot(CURRENT_SCHEMA, 'test', 1)", "Procedure not registered: system.rollback_to_snapshot");
    }
}
