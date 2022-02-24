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
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

// some tests may invalidate the whole cache affecting therefore other concurrent tests
@Test(singleThreaded = true)
public class TestCachingDirectoryLister
        extends AbstractTestQueryFramework
{
    private CachingDirectoryLister cachingDirectoryLister;
    private FileHiveMetastore fileHiveMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path temporaryMetastoreDirectory = createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(temporaryMetastoreDirectory, ALLOW_INSECURE));

        cachingDirectoryLister = new CachingDirectoryLister(Duration.valueOf("5m"), 1_000_000L, List.of("tpch.*"));

        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of(
                        "hive.allow-register-partition-procedure", "true",
                        "hive.file-status-cache-expire-time", "5m",
                        "hive.file-status-cache-size", "1000000",
                        "hive.file-status-cache-tables", "tpch.*"))
                .setMetastore(distributedQueryRunner -> fileHiveMetastore = createTestingFileHiveMetastore(temporaryMetastoreDirectory.toFile()))
                .setCachingDirectoryLister(cachingDirectoryLister)
                .build();
    }

    @Test
    public void testCacheInvalidationIsAppliedSpecificallyOnTheNonPartitionedTableBeingChanged()
    {
        assertUpdate("CREATE TABLE partial_cache_invalidation_table1 (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO partial_cache_invalidation_table1 VALUES (1), (2), (3)", 3);
        // The listing for the invalidate_non_partitioned_table1 should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM partial_cache_invalidation_table1", "VALUES (6)");
        org.apache.hadoop.fs.Path cachedTable1Location = getTableLocation(TPCH_SCHEMA, "partial_cache_invalidation_table1");
        assertThat(cachingDirectoryLister.isCached(cachedTable1Location)).isTrue();

        assertUpdate("CREATE TABLE partial_cache_invalidation_table2 (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO partial_cache_invalidation_table2 VALUES (11), (12)", 2);
        // The listing for the invalidate_non_partitioned_table2 should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM partial_cache_invalidation_table2", "VALUES (23)");
        org.apache.hadoop.fs.Path cachedTable2Location = getTableLocation(TPCH_SCHEMA, "partial_cache_invalidation_table2");
        assertThat(cachingDirectoryLister.isCached(cachedTable2Location)).isTrue();

        assertUpdate("INSERT INTO partial_cache_invalidation_table1 VALUES (4), (5)", 2);
        // Inserting into the invalidate_non_partitioned_table1 should invalidate only the cached listing of the files belonging only to this table.
        assertThat(cachingDirectoryLister.isCached(cachedTable1Location)).isFalse();
        assertThat(cachingDirectoryLister.isCached(cachedTable2Location)).isTrue();

        assertQuery("SELECT sum(col1) FROM partial_cache_invalidation_table1", "VALUES (15)");
        assertQuery("SELECT sum(col1) FROM partial_cache_invalidation_table2", "VALUES (23)");

        assertUpdate("DROP TABLE partial_cache_invalidation_table1");
        assertUpdate("DROP TABLE partial_cache_invalidation_table2");
    }

    @Test
    public void testCacheInvalidationIsAppliedOnTheEntireCacheOnPartitionedTableDrop()
    {
        assertUpdate("CREATE TABLE full_cache_invalidation_non_partitioned_table (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO full_cache_invalidation_non_partitioned_table VALUES (1), (2), (3)", 3);
        // The listing for the invalidate_non_partitioned_table1 should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM full_cache_invalidation_non_partitioned_table", "VALUES (6)");
        org.apache.hadoop.fs.Path nonPartitionedTableLocation = getTableLocation(TPCH_SCHEMA, "full_cache_invalidation_non_partitioned_table");
        assertThat(cachingDirectoryLister.isCached(nonPartitionedTableLocation)).isTrue();

        assertUpdate("CREATE TABLE full_cache_invalidation_partitioned_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO full_cache_invalidation_partitioned_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2')", 4);
        assertQuery("SELECT col2, sum(col1) FROM full_cache_invalidation_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7)");
        org.apache.hadoop.fs.Path partitionedTableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table", ImmutableList.of("group1"));
        org.apache.hadoop.fs.Path partitionedTableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table", ImmutableList.of("group2"));
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup1PartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup2PartitionLocation)).isTrue();

        assertUpdate("INSERT INTO full_cache_invalidation_non_partitioned_table VALUES (4), (5)", 2);
        // Inserting into the invalidate_non_partitioned_table1 should invalidate only the cached listing of the files belonging only to this table.
        assertThat(cachingDirectoryLister.isCached(nonPartitionedTableLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup1PartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup2PartitionLocation)).isTrue();

        assertUpdate("DROP TABLE full_cache_invalidation_partitioned_table");
        // Invalidation of the partitioned table causes the full invalidation of the cache
        assertThat(cachingDirectoryLister.isCached(nonPartitionedTableLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup1PartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup2PartitionLocation)).isFalse();

        assertQuery("SELECT sum(col1) FROM full_cache_invalidation_non_partitioned_table", "VALUES (15)");

        assertUpdate("DROP TABLE full_cache_invalidation_non_partitioned_table");
    }

    @Test
    public void testCacheInvalidationIsAppliedSpecificallyOnPartitionDropped()
    {
        assertUpdate("CREATE TABLE partition_path_cache_invalidation_non_partitioned_table (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO partition_path_cache_invalidation_non_partitioned_table VALUES (1), (2), (3)", 3);
        // The listing for the invalidate_non_partitioned_table1 should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM partition_path_cache_invalidation_non_partitioned_table", "VALUES (6)");
        org.apache.hadoop.fs.Path nonPartitionedTableLocation = getTableLocation(TPCH_SCHEMA, "partition_path_cache_invalidation_non_partitioned_table");
        assertThat(cachingDirectoryLister.isCached(nonPartitionedTableLocation)).isTrue();

        assertUpdate("CREATE TABLE partition_path_cache_invalidation_partitioned_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO partition_path_cache_invalidation_partitioned_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2')", 4);
        assertQuery("SELECT col2, sum(col1) FROM partition_path_cache_invalidation_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7)");
        org.apache.hadoop.fs.Path partitionedTableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "partition_path_cache_invalidation_partitioned_table", ImmutableList.of("group1"));
        org.apache.hadoop.fs.Path partitionedTableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "partition_path_cache_invalidation_partitioned_table", ImmutableList.of("group2"));
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup1PartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup2PartitionLocation)).isTrue();

        assertUpdate("DELETE FROM partition_path_cache_invalidation_partitioned_table WHERE col2='group1'");
        assertThat(cachingDirectoryLister.isCached(nonPartitionedTableLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup1PartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(partitionedTableGroup2PartitionLocation)).isTrue();

        assertQuery("SELECT sum(col1) FROM partition_path_cache_invalidation_non_partitioned_table", "VALUES (6)");
        assertQuery("SELECT col2, sum(col1) FROM partition_path_cache_invalidation_partitioned_table GROUP BY col2", "VALUES ('group2', 7)");

        assertUpdate("DROP TABLE partition_path_cache_invalidation_non_partitioned_table");
        assertUpdate("DROP TABLE partition_path_cache_invalidation_partitioned_table");
    }

    @Test
    public void testInsertIntoNonPartitionedTable()
    {
        assertUpdate("CREATE TABLE insert_into_non_partitioned_table (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO insert_into_non_partitioned_table VALUES (1), (2), (3)", 3);
        // The listing for the table should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM insert_into_non_partitioned_table", "VALUES (6)");
        assertThat(cachingDirectoryLister.isCached(getTableLocation(TPCH_SCHEMA, "insert_into_non_partitioned_table"))).isTrue();
        assertUpdate("INSERT INTO insert_into_non_partitioned_table VALUES (4), (5)", 2);
        // Inserting into the table should invalidate the cached listing of the files belonging to the table.
        assertThat(cachingDirectoryLister.isCached(getTableLocation(TPCH_SCHEMA, "insert_into_non_partitioned_table"))).isFalse();

        assertQuery("SELECT sum(col1) FROM insert_into_non_partitioned_table", "VALUES (15)");

        assertUpdate("DROP TABLE insert_into_non_partitioned_table");
    }

    @Test
    public void testInsertIntoPartitionedTable()
    {
        assertUpdate("CREATE TABLE insert_into_partitioned_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO insert_into_partitioned_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2')", 4);
        // The listing for the table partitions should be in the directory cache after this call
        assertQuery("SELECT col2, sum(col1) FROM insert_into_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7)");
        org.apache.hadoop.fs.Path tableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "insert_into_partitioned_table", ImmutableList.of("group1"));
        org.apache.hadoop.fs.Path tableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "insert_into_partitioned_table", ImmutableList.of("group2"));
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isTrue();

        assertUpdate("INSERT INTO insert_into_partitioned_table  VALUES (5, 'group2'), (6, 'group3')", 2);
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isTrue();
        // Inserting into the table should invalidate the cached listing of the partitions affected by the insert statement
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isFalse();
        assertQuery("SELECT col2, sum(col1) FROM insert_into_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 12), ('group3', 6)");

        assertUpdate("DROP TABLE insert_into_partitioned_table");
    }

    @Test
    public void testDropPartition()
    {
        assertUpdate("CREATE TABLE delete_from_partitioned_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO delete_from_partitioned_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2'), (5, 'group3')", 5);
        // The listing for the table partitions should be in the directory cache after this call
        assertQuery("SELECT col2, sum(col1) FROM delete_from_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7), ('group3', 5)");
        org.apache.hadoop.fs.Path tableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("group1"));
        org.apache.hadoop.fs.Path tableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("group2"));
        org.apache.hadoop.fs.Path tableGroup3PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("group3"));
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isTrue();
        assertUpdate("DELETE FROM delete_from_partitioned_table WHERE col2 = 'group1' OR col2 = 'group2'");
        // Deleting from the table should invalidate the cached listing of the partitions dropped from the table.
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(tableGroup3PartitionLocation)).isTrue();
        assertQuery("SELECT col2, sum(col1) FROM delete_from_partitioned_table GROUP BY col2", "VALUES ('group3', 5)");

        assertUpdate("DROP TABLE delete_from_partitioned_table");
    }

    @Test
    public void testDropMultiLevelPartition()
    {
        assertUpdate("CREATE TABLE delete_from_partitioned_table (clicks bigint, day date, country varchar) WITH (format = 'ORC', partitioned_by = ARRAY['day', 'country'])");
        assertUpdate("INSERT INTO delete_from_partitioned_table VALUES (1000, DATE '2022-02-01', 'US'), (2000, DATE '2022-02-01', 'US'), (4000, DATE '2022-02-02', 'US'), (1500, DATE '2022-02-01', 'AT'), (2500, DATE '2022-02-02', 'AT')", 5);
        // The listing for the table partitions should be in the directory cache after this call
        assertQuery("SELECT day, country, sum(clicks) FROM delete_from_partitioned_table GROUP BY day, country", "VALUES (DATE '2022-02-01', 'US', 3000), (DATE '2022-02-02', 'US', 4000), (DATE '2022-02-01', 'AT', 1500), (DATE '2022-02-02', 'AT', 2500)");
        org.apache.hadoop.fs.Path table20220201UsPartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("2022-02-01", "US"));
        org.apache.hadoop.fs.Path table20220202UsPartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("2022-02-02", "US"));
        org.apache.hadoop.fs.Path table20220201AtPartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("2022-02-01", "AT"));
        org.apache.hadoop.fs.Path table20220202AtPartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("2022-02-02", "AT"));
        assertThat(cachingDirectoryLister.isCached(table20220201UsPartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(table20220202UsPartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(table20220201AtPartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(table20220202AtPartitionLocation)).isTrue();
        assertUpdate("DELETE FROM delete_from_partitioned_table WHERE day = DATE '2022-02-01'");
        // Deleting from the table should invalidate the cached listing of the partitions dropped from the table.
        assertThat(cachingDirectoryLister.isCached(table20220201UsPartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(table20220202UsPartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(table20220201AtPartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(table20220202AtPartitionLocation)).isTrue();
        assertUpdate("DELETE FROM delete_from_partitioned_table WHERE country = 'US'");
        assertThat(cachingDirectoryLister.isCached(table20220202UsPartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(table20220202AtPartitionLocation)).isTrue();
        assertQuery("SELECT day, country, sum(clicks) FROM delete_from_partitioned_table GROUP BY day, country", "VALUES (DATE '2022-02-02', 'AT', 2500)");

        assertUpdate("DROP TABLE delete_from_partitioned_table");
    }

    @Test
    public void testUnregisterRegisterPartition()
    {
        assertUpdate("CREATE TABLE register_unregister_partition_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO register_unregister_partition_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2')", 4);
        // The listing for the table partitions should be in the directory cache after this call
        assertQuery("SELECT col2, sum(col1) FROM register_unregister_partition_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7)");
        org.apache.hadoop.fs.Path tableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "register_unregister_partition_table", ImmutableList.of("group1"));
        org.apache.hadoop.fs.Path tableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "register_unregister_partition_table", ImmutableList.of("group2"));
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isTrue();

        List<MaterializedRow> paths = getQueryRunner().execute(getSession(), "SELECT \"$path\" FROM register_unregister_partition_table WHERE col2 = 'group1' LIMIT 1").toTestTypes().getMaterializedRows();
        String group1PartitionPath = new org.apache.hadoop.fs.Path((String) paths.get(0).getField(0)).getParent().toString();

        assertUpdate(format("CALL system.unregister_partition('%s', '%s', ARRAY['col2'], ARRAY['group1'])", TPCH_SCHEMA, "register_unregister_partition_table"));
        // Unregistering the partition in the table should invalidate the cached listing of all the partitions belonging to the table.
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isTrue();
        assertQuery("SELECT col2, sum(col1) FROM register_unregister_partition_table GROUP BY col2", "VALUES ('group2', 7)");
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isTrue();

        assertUpdate(format("CALL system.register_partition('%s', '%s', ARRAY['col2'], ARRAY['group1'], '%s')", TPCH_SCHEMA, "register_unregister_partition_table", group1PartitionPath));
        // Registering the partition in the table should invalidate the cached listing of all the partitions belonging to the table.
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isTrue();

        assertQuery("SELECT col2, sum(col1) FROM register_unregister_partition_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7)");

        assertUpdate("DROP TABLE register_unregister_partition_table");
    }

    @Test
    public void testRenameTable()
    {
        assertUpdate("CREATE TABLE table_to_be_renamed (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO table_to_be_renamed VALUES (1), (2), (3)", 3);
        // The listing for the table should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM table_to_be_renamed", "VALUES (6)");
        org.apache.hadoop.fs.Path tableLocation = getTableLocation(TPCH_SCHEMA, "table_to_be_renamed");
        assertThat(cachingDirectoryLister.isCached(tableLocation)).isTrue();
        assertUpdate("ALTER TABLE table_to_be_renamed RENAME TO table_renamed");
        // Altering the table should invalidate the cached listing of the files belonging to the table.
        assertThat(cachingDirectoryLister.isCached(tableLocation)).isFalse();

        assertUpdate("DROP TABLE table_renamed");
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE table_to_be_dropped (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO table_to_be_dropped VALUES (1), (2), (3)", 3);
        // The listing for the table should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM table_to_be_dropped", "VALUES (6)");
        org.apache.hadoop.fs.Path tableLocation = getTableLocation(TPCH_SCHEMA, "table_to_be_dropped");
        assertThat(cachingDirectoryLister.isCached(tableLocation)).isTrue();
        assertUpdate("DROP TABLE table_to_be_dropped");
        // Dropping the table should invalidate the cached listing of the files belonging to the table.
        assertThat(cachingDirectoryLister.isCached(tableLocation)).isFalse();
    }

    @Test
    public void testDropPartitionedTable()
    {
        assertUpdate("CREATE TABLE drop_partitioned_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO drop_partitioned_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2'), (5, 'group3')", 5);
        // The listing for the table partitions should be in the directory cache after this call
        assertQuery("SELECT col2, sum(col1) FROM drop_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7), ('group3', 5)");
        org.apache.hadoop.fs.Path tableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "drop_partitioned_table", ImmutableList.of("group1"));
        org.apache.hadoop.fs.Path tableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "drop_partitioned_table", ImmutableList.of("group2"));
        org.apache.hadoop.fs.Path tableGroup3PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "drop_partitioned_table", ImmutableList.of("group3"));
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isTrue();
        assertThat(cachingDirectoryLister.isCached(tableGroup3PartitionLocation)).isTrue();
        assertUpdate("DROP TABLE drop_partitioned_table");
        assertThat(cachingDirectoryLister.isCached(tableGroup1PartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(tableGroup2PartitionLocation)).isFalse();
        assertThat(cachingDirectoryLister.isCached(tableGroup3PartitionLocation)).isFalse();
    }

    private org.apache.hadoop.fs.Path getTableLocation(String schemaName, String tableName)
    {
        return fileHiveMetastore.getTable(schemaName, tableName)
                .map(table -> table.getStorage().getLocation())
                .map(tableLocation -> new org.apache.hadoop.fs.Path(tableLocation))
                .orElseThrow(() -> new NoSuchElementException(format("The table %s.%s could not be found", schemaName, tableName)));
    }

    private org.apache.hadoop.fs.Path getPartitionLocation(String schemaName, String tableName, List<String> partitionValues)
    {
        Table table = fileHiveMetastore.getTable(schemaName, tableName)
                .orElseThrow(() -> new NoSuchElementException(format("The table %s.%s could not be found", schemaName, tableName)));

        return fileHiveMetastore.getPartition(table, partitionValues)
                .map(partition -> partition.getStorage().getLocation())
                .map(partitionLocation -> new org.apache.hadoop.fs.Path(partitionLocation))
                .orElseThrow(() -> new NoSuchElementException(format("The partition %s from the table %s.%s could not be found", partitionValues, schemaName, tableName)));
    }
}
