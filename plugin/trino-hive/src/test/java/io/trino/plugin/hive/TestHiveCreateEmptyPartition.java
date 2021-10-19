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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.fs.Path;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveCreateEmptyPartition
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of(
                        "hive.allow-create-partition-with-location", "true",
                        "hive.allow-register-partition-procedure", "true",
                        "hive.non-managed-table-writes-enabled", "true"))
                .build();
    }

    @Test
    public void testCreateEmptyBucketedPartition()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testCreateEmptyBucketedPartition(storageFormat.getFormat());
        }
    }

    private void testCreateEmptyBucketedPartition(HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_empty_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String sql = format("CALL system.create_empty_partition('%s', '%s', ARRAY['orderstatus'], ARRAY['%s'])", TPCH_SCHEMA, tableName, orderStatusList.get(i));
            assertUpdate(sql);
            assertQuery(
                    format("SELECT count(*) FROM \"%s$partitions\"", tableName),
                    "SELECT " + (i + 1));

            assertQueryFails(sql, "Partition already exists.*");
        }

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private static class TestingHiveStorageFormat
    {
        private final Session session;
        private final HiveStorageFormat format;

        TestingHiveStorageFormat(Session session, HiveStorageFormat format)
        {
            this.session = requireNonNull(session, "session is null");
            this.format = requireNonNull(format, "format is null");
        }

        public Session getSession()
        {
            return session;
        }

        public HiveStorageFormat getFormat()
        {
            return format;
        }
    }

    private List<TestingHiveStorageFormat> getAllTestingHiveStorageFormat()
    {
        Session session = getSession();
        String catalog = session.getCatalog().orElseThrow();
        ImmutableList.Builder<TestingHiveStorageFormat> formats = ImmutableList.builder();
        for (HiveStorageFormat hiveStorageFormat : HiveStorageFormat.values()) {
            if (hiveStorageFormat == HiveStorageFormat.CSV) {
                // CSV supports only unbounded VARCHAR type
                continue;
            }
            if (hiveStorageFormat == HiveStorageFormat.PARQUET) {
                formats.add(new TestingHiveStorageFormat(
                        Session.builder(session)
                                .setCatalogSessionProperty(catalog, "experimental_parquet_optimized_writer_enabled", "false")
                                .build(),
                        hiveStorageFormat));
                formats.add(new TestingHiveStorageFormat(
                        Session.builder(session)
                                .setCatalogSessionProperty(catalog, "experimental_parquet_optimized_writer_enabled", "true")
                                .build(),
                        hiveStorageFormat));
                continue;
            }
            formats.add(new TestingHiveStorageFormat(session, hiveStorageFormat));
        }
        return formats.build();
    }

    private void createPartitionedBucketedTable(String tableName, HiveStorageFormat storageFormat)
    {
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");
    }

    @Test
    public void testCreateEmptyPartitionOnNonExistingTable()
    {
        assertQueryFails(
                format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, "non_existing_table", "empty"),
                format("Table '%s.%s' does not exist", TPCH_SCHEMA, "non_existing_table"));
    }

    @Test
    public void testCreateEmptyNonBucketedPartition()
    {
        String tableName = "test_insert_empty_partitioned_unbucketed_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'part' ] " +
                ")");
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 0");

        assertAccessDenied(
                format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, tableName, "empty"),
                format("Cannot insert into table hive.tpch.%s", tableName),
                privilege(tableName, INSERT_TABLE));

        // create an empty partition
        assertUpdate(format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, tableName, "empty"));
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyPartitionWithLocationForWritableExternalTable()
            throws Exception
    {
        String tableName = "test_insert_empty_partitioned_unbucketed_table_with_location";
        File tempTableDir = createTempDir();
        File tempOtherDir = createTempDir();

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE hive.%s.%s (\n" +
                        "   col1 varchar,\n" +
                        "   part varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC',\n" +
                        "   partitioned_by = Array[ 'part' ],\n" +
                        "   external_location = '%s'\n" +
                        ")",
                TPCH_SCHEMA,
                tableName,
                new Path(tempTableDir.toURI().toASCIIString()));

        assertUpdate(createTableSql);

        Path tempOtherDirPath = new Path(tempOtherDir.toURI().toASCIIString());
        assertUpdate(format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'], '%s')", TPCH_SCHEMA, tableName, "empty", tempOtherDirPath));
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 1");

        boolean partitionFileExistsAtLocation = Files.walk(tempOtherDir.toPath(), 1).anyMatch(path -> path.endsWith("part=empty"));
        assertTrue(partitionFileExistsAtLocation, format("expected 'part=empty' to be present at '%s'", tempOtherDir.toPath()));

        boolean partitionFileDoesNotExistAtDefaultLocation = Files.walk(tempTableDir.toPath(), 1).anyMatch(path -> path.endsWith("part=empty"));
        assertFalse(partitionFileDoesNotExistAtDefaultLocation, format("expected 'part=empty' not to exist at default location '%s'", tempTableDir.toPath()));

        assertUpdate(format("DROP TABLE %s", tableName));

        boolean partitionFileExistsAtLocationAfterTableWasDropped = Files.walk(tempOtherDir.toPath(), 1).anyMatch(path -> path.endsWith("part=empty"));
        assertTrue(partitionFileExistsAtLocationAfterTableWasDropped, format("expected 'part=empty' to be present at '%s'", tempOtherDir.toPath()));

        deleteRecursively(tempTableDir.toPath(), ALLOW_INSECURE);
        deleteRecursively(tempOtherDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCreateEmptyPartitionWithLocationForManagedTable()
            throws Exception
    {
        String tableName = "test_insert_empty_partitioned_unbucketed_table_with_location";

        // Currently, FileHiveMetastore only allows managed tables to write to its base data directory
        java.nio.file.Path baseDataDir = getDistributedQueryRunner().getCoordinator().getBaseDataDir().resolve("hive_data");
        File defaultTableLocation = new File(format("%s/%s/%s", baseDataDir, TPCH_SCHEMA, tableName));

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE hive.%s.%s (\n" +
                        "   col1 varchar,\n" +
                        "   part varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC',\n" +
                        "   partitioned_by = Array[ 'part' ]\n" +
                        ")",
                TPCH_SCHEMA,
                tableName);

        assertUpdate(createTableSql);
        assertUpdate(format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'], '%s')", TPCH_SCHEMA, tableName, "empty", new Path(defaultTableLocation.toURI().toASCIIString())));
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 1");

        boolean partitionFileExistsAtLocation = Files.walk(defaultTableLocation.toPath(), 1).anyMatch(path -> path.endsWith("part=empty"));
        assertTrue(partitionFileExistsAtLocation, format("expected 'part=empty' to be present at '%s'", defaultTableLocation.toPath()));

        assertUpdate(format("DROP TABLE %s", tableName));

        boolean partitionFileExistsAtLocationAfterTableWasDropped = defaultTableLocation.exists();
        assertFalse(partitionFileExistsAtLocationAfterTableWasDropped, "expected 'part=empty' to be deleted after managed table was dropped");
    }
}
