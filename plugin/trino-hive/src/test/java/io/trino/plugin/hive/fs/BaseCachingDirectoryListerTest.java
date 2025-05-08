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
package io.trino.plugin.hive.fs;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseCachingDirectoryListerTest
        extends AbstractTestQueryFramework
{
    private CachingDirectoryLister directoryLister;
    private HiveMetastore metastore;

    protected QueryRunner createQueryRunner(Map<String, String> properties)
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.builder()
                .setHiveProperties(properties)
                .build();

        directoryLister = getConnectorService(queryRunner, CachingDirectoryLister.class);

        metastore = getConnectorService(queryRunner, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        return queryRunner;
    }

    @Test
    public void testCacheInvalidationIsAppliedSpecificallyOnTheNonPartitionedTableBeingChanged()
    {
        assertUpdate("CREATE TABLE partial_cache_invalidation_table1 (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO partial_cache_invalidation_table1 VALUES (1), (2), (3)", 3);
        // The listing for the invalidate_non_partitioned_table1 should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM partial_cache_invalidation_table1", "VALUES (6)");
        String cachedTable1Location = getTableLocation(TPCH_SCHEMA, "partial_cache_invalidation_table1");
        assertThat(isCached(cachedTable1Location, schemaTableName(TPCH_SCHEMA, "partial_cache_invalidation_table1"))).isTrue();

        assertUpdate("CREATE TABLE partial_cache_invalidation_table2 (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO partial_cache_invalidation_table2 VALUES (11), (12)", 2);
        // The listing for the invalidate_non_partitioned_table2 should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM partial_cache_invalidation_table2", "VALUES (23)");
        String cachedTable2Location = getTableLocation(TPCH_SCHEMA, "partial_cache_invalidation_table2");
        assertThat(isCached(cachedTable2Location, schemaTableName(TPCH_SCHEMA, "partial_cache_invalidation_table2"))).isTrue();

        assertUpdate("INSERT INTO partial_cache_invalidation_table1 VALUES (4), (5)", 2);
        // Inserting into the invalidate_non_partitioned_table1 should invalidate only the cached listing of the files belonging only to this table.
        assertThat(isCached(cachedTable1Location, schemaTableName(TPCH_SCHEMA, "partial_cache_invalidation_table1"))).isFalse();
        assertThat(isCached(cachedTable2Location, schemaTableName(TPCH_SCHEMA, "partial_cache_invalidation_table2"))).isTrue();

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
        String nonPartitionedTableLocation = getTableLocation(TPCH_SCHEMA, "full_cache_invalidation_non_partitioned_table");
        assertThat(isCached(nonPartitionedTableLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_non_partitioned_table"))).isTrue();

        assertUpdate("CREATE TABLE full_cache_invalidation_partitioned_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO full_cache_invalidation_partitioned_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2')", 4);
        assertQuery("SELECT col2, sum(col1) FROM full_cache_invalidation_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7)");
        String partitionedTableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table", ImmutableList.of("group1"));
        String partitionedTableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table", ImmutableList.of("group2"));
        assertThat(isCached(partitionedTableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table"))).isTrue();
        assertThat(isCached(partitionedTableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table"))).isTrue();

        assertUpdate("INSERT INTO full_cache_invalidation_non_partitioned_table VALUES (4), (5)", 2);
        // Inserting into the invalidate_non_partitioned_table1 should invalidate only the cached listing of the files belonging only to this table.
        assertThat(isCached(nonPartitionedTableLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_non_partitioned_table"))).isFalse();
        assertThat(isCached(partitionedTableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table"))).isTrue();
        assertThat(isCached(partitionedTableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table"))).isTrue();

        assertUpdate("DROP TABLE full_cache_invalidation_partitioned_table");
        // Invalidation of the partitioned table causes the full invalidation of the cache
        assertThat(isCached(nonPartitionedTableLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_non_partitioned_table"))).isFalse();
        assertThat(isCached(partitionedTableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table"))).isFalse();
        assertThat(isCached(partitionedTableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "full_cache_invalidation_partitioned_table"))).isFalse();

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
        String nonPartitionedTableLocation = getTableLocation(TPCH_SCHEMA, "partition_path_cache_invalidation_non_partitioned_table");
        assertThat(isCached(nonPartitionedTableLocation, schemaTableName(TPCH_SCHEMA, "partition_path_cache_invalidation_non_partitioned_table"))).isTrue();

        assertUpdate("CREATE TABLE partition_path_cache_invalidation_partitioned_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO partition_path_cache_invalidation_partitioned_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2')", 4);
        assertQuery("SELECT col2, sum(col1) FROM partition_path_cache_invalidation_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7)");
        String partitionedTableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "partition_path_cache_invalidation_partitioned_table", ImmutableList.of("group1"));
        String partitionedTableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "partition_path_cache_invalidation_partitioned_table", ImmutableList.of("group2"));
        assertThat(isCached(partitionedTableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "partition_path_cache_invalidation_partitioned_table"))).isTrue();
        assertThat(isCached(partitionedTableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "partition_path_cache_invalidation_partitioned_table"))).isTrue();

        assertUpdate("DELETE FROM partition_path_cache_invalidation_partitioned_table WHERE col2='group1'");
        assertThat(isCached(nonPartitionedTableLocation, schemaTableName(TPCH_SCHEMA, "partition_path_cache_invalidation_non_partitioned_table"))).isTrue();
        assertThat(isCached(partitionedTableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "partition_path_cache_invalidation_partitioned_table"))).isFalse();
        assertThat(isCached(partitionedTableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "partition_path_cache_invalidation_partitioned_table"))).isTrue();

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
        assertThat(isCached(getTableLocation(TPCH_SCHEMA, "insert_into_non_partitioned_table"), schemaTableName(TPCH_SCHEMA, "insert_into_non_partitioned_table"))).isTrue();
        assertUpdate("INSERT INTO insert_into_non_partitioned_table VALUES (4), (5)", 2);
        // Inserting into the table should invalidate the cached listing of the files belonging to the table.
        assertThat(isCached(getTableLocation(TPCH_SCHEMA, "insert_into_non_partitioned_table"), schemaTableName(TPCH_SCHEMA, "insert_into_non_partitioned_table"))).isFalse();

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
        String tableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "insert_into_partitioned_table", ImmutableList.of("group1"));
        String tableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "insert_into_partitioned_table", ImmutableList.of("group2"));
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "insert_into_partitioned_table"))).isTrue();
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "insert_into_partitioned_table"))).isTrue();

        assertUpdate("INSERT INTO insert_into_partitioned_table  VALUES (5, 'group2'), (6, 'group3')", 2);
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "insert_into_partitioned_table"))).isTrue();
        // Inserting into the table should invalidate the cached listing of the partitions affected by the insert statement
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "insert_into_partitioned_table"))).isFalse();
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
        String tableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("group1"));
        String tableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("group2"));
        String tableGroup3PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("group3"));
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
        assertUpdate("DELETE FROM delete_from_partitioned_table WHERE col2 = 'group1' OR col2 = 'group2'");
        // Deleting from the table should invalidate the cached listing of the partitions dropped from the table.
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isFalse();
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isFalse();
        assertThat(isCached(tableGroup3PartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
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
        String table20220201UsPartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("2022-02-01", "US"));
        String table20220202UsPartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("2022-02-02", "US"));
        String table20220201AtPartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("2022-02-01", "AT"));
        String table20220202AtPartitionLocation = getPartitionLocation(TPCH_SCHEMA, "delete_from_partitioned_table", ImmutableList.of("2022-02-02", "AT"));
        assertThat(isCached(table20220201UsPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
        assertThat(isCached(table20220202UsPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
        assertThat(isCached(table20220201AtPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
        assertThat(isCached(table20220202AtPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
        assertUpdate("DELETE FROM delete_from_partitioned_table WHERE day = DATE '2022-02-01'");
        // Deleting from the table should invalidate the cached listing of the partitions dropped from the table.
        assertThat(isCached(table20220201UsPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isFalse();
        assertThat(isCached(table20220202UsPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
        assertThat(isCached(table20220201AtPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isFalse();
        assertThat(isCached(table20220202AtPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
        assertUpdate("DELETE FROM delete_from_partitioned_table WHERE country = 'US'");
        assertThat(isCached(table20220202UsPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isFalse();
        assertThat(isCached(table20220202AtPartitionLocation, schemaTableName(TPCH_SCHEMA, "delete_from_partitioned_table"))).isTrue();
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
        String tableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "register_unregister_partition_table", ImmutableList.of("group1"));
        String tableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "register_unregister_partition_table", ImmutableList.of("group2"));
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "register_unregister_partition_table"))).isTrue();
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "register_unregister_partition_table"))).isTrue();

        List<MaterializedRow> paths = getQueryRunner().execute(getSession(), "SELECT \"$path\" FROM register_unregister_partition_table WHERE col2 = 'group1' LIMIT 1").toTestTypes().getMaterializedRows();
        String group1PartitionPath = Location.of((String) paths.get(0).getField(0)).parentDirectory().toString();

        assertUpdate(format("CALL system.unregister_partition('%s', '%s', ARRAY['col2'], ARRAY['group1'])", TPCH_SCHEMA, "register_unregister_partition_table"));
        // Unregistering the partition in the table should invalidate the cached listing of all the partitions belonging to the table.
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "register_unregister_partition_table"))).isFalse();
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "register_unregister_partition_table"))).isTrue();
        assertQuery("SELECT col2, sum(col1) FROM register_unregister_partition_table GROUP BY col2", "VALUES ('group2', 7)");
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "register_unregister_partition_table"))).isTrue();

        assertUpdate(format("CALL system.register_partition('%s', '%s', ARRAY['col2'], ARRAY['group1'], '%s')", TPCH_SCHEMA, "register_unregister_partition_table", group1PartitionPath));
        // Registering the partition in the table should invalidate the cached listing of all the partitions belonging to the table.
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "register_unregister_partition_table"))).isFalse();
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "register_unregister_partition_table"))).isTrue();

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
        String tableLocation = getTableLocation(TPCH_SCHEMA, "table_to_be_renamed");
        assertThat(isCached(tableLocation, schemaTableName(TPCH_SCHEMA, "table_to_be_renamed"))).isTrue();
        assertUpdate("ALTER TABLE table_to_be_renamed RENAME TO table_renamed");
        // Altering the table should invalidate the cached listing of the files belonging to the table.
        assertThat(isCached(tableLocation, schemaTableName(TPCH_SCHEMA, "table_to_be_renamed"))).isFalse();

        assertUpdate("DROP TABLE table_renamed");
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE table_to_be_dropped (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO table_to_be_dropped VALUES (1), (2), (3)", 3);
        // The listing for the table should be in the directory cache after this call
        assertQuery("SELECT sum(col1) FROM table_to_be_dropped", "VALUES (6)");
        String tableLocation = getTableLocation(TPCH_SCHEMA, "table_to_be_dropped");
        assertThat(isCached(tableLocation, schemaTableName(TPCH_SCHEMA, "table_to_be_dropped"))).isTrue();
        assertUpdate("DROP TABLE table_to_be_dropped");
        // Dropping the table should invalidate the cached listing of the files belonging to the table.
        assertThat(isCached(tableLocation, schemaTableName(TPCH_SCHEMA, "table_to_be_dropped"))).isFalse();
    }

    @Test
    public void testDropPartitionedTable()
    {
        assertUpdate("CREATE TABLE drop_partitioned_table (col1 int, col2 varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col2'])");
        assertUpdate("INSERT INTO drop_partitioned_table VALUES (1, 'group1'), (2, 'group1'), (3, 'group2'), (4, 'group2'), (5, 'group3')", 5);
        // The listing for the table partitions should be in the directory cache after this call
        assertQuery("SELECT col2, sum(col1) FROM drop_partitioned_table GROUP BY col2", "VALUES ('group1', 3), ('group2', 7), ('group3', 5)");
        String tableGroup1PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "drop_partitioned_table", ImmutableList.of("group1"));
        String tableGroup2PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "drop_partitioned_table", ImmutableList.of("group2"));
        String tableGroup3PartitionLocation = getPartitionLocation(TPCH_SCHEMA, "drop_partitioned_table", ImmutableList.of("group3"));
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "drop_partitioned_table"))).isTrue();
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "drop_partitioned_table"))).isTrue();
        assertThat(isCached(tableGroup3PartitionLocation, schemaTableName(TPCH_SCHEMA, "drop_partitioned_table"))).isTrue();

        // Create another table and ensure it's in the cache
        assertUpdate("CREATE TABLE other_table (col1 int) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO other_table VALUES (1), (2), (3)", 3);
        assertQuery("SELECT sum(col1) FROM other_table", "VALUES (6)");
        String otherTableLocation = getTableLocation(TPCH_SCHEMA, "other_table");
        assertThat(isCached(otherTableLocation, schemaTableName(TPCH_SCHEMA, "other_table"))).isTrue();

        assertUpdate("DROP TABLE drop_partitioned_table");
        assertThat(isCached(tableGroup1PartitionLocation, schemaTableName(TPCH_SCHEMA, "drop_partitioned_table"))).isFalse();
        assertThat(isCached(tableGroup2PartitionLocation, schemaTableName(TPCH_SCHEMA, "drop_partitioned_table"))).isFalse();
        assertThat(isCached(tableGroup3PartitionLocation, schemaTableName(TPCH_SCHEMA, "drop_partitioned_table"))).isFalse();

        // Verify that other_table is still cached
        assertThat(isCached(otherTableLocation, schemaTableName(TPCH_SCHEMA, "other_table"))).isTrue();
        assertQuery("SELECT sum(col1) FROM other_table", "VALUES (6)");

        assertUpdate("DROP TABLE other_table");
    }

    @Test
    public void testTableExclusion()
    {
        CachingDirectoryLister lister = new CachingDirectoryLister(
                new Duration(5, TimeUnit.MINUTES),
                DataSize.of(1, DataSize.Unit.MEGABYTE),
                ImmutableList.of("db.*", "test.*"),
                ImmutableList.of("db.excluded", "test.excluded"),
                _ -> true);

        // Test included tables
        assertThat(isCacheEnabled(lister, "db", "table1")).isTrue();
        assertThat(isCacheEnabled(lister, "test", "table2")).isTrue();

        // Test excluded tables
        assertThat(isCacheEnabled(lister, "db", "excluded")).isFalse();
        assertThat(isCacheEnabled(lister, "test", "excluded")).isFalse();

        // Test non-matching tables
        assertThat(isCacheEnabled(lister, "other", "table")).isFalse();
    }

    @Test
    public void testWildcardExclusion()
    {
        CachingDirectoryLister lister = new CachingDirectoryLister(
                new Duration(5, TimeUnit.MINUTES),
                DataSize.of(1, DataSize.Unit.MEGABYTE),
                ImmutableList.of("db.*"),
                ImmutableList.of("db.*"),  // Exclude all tables in db schema
                _ -> true);

        // All tables in db schema should be excluded
        assertThat(isCacheEnabled(lister, "db", "table1")).isFalse();
        assertThat(isCacheEnabled(lister, "db", "production")).isFalse();
        assertThat(isCacheEnabled(lister, "db", "temp_table")).isFalse();

        // Other schemas should still not be cached
        assertThat(isCacheEnabled(lister, "other", "table")).isFalse();
    }

    @Test
    public void testForExcludingOneTableFromSchema()
    {
        CachingDirectoryLister lister = new CachingDirectoryLister(
                new Duration(5, TimeUnit.MINUTES),
                DataSize.of(1, DataSize.Unit.MEGABYTE),
                ImmutableList.of("db.*"),
                ImmutableList.of("db.a"),  // Exclude table a in db schema
                _ -> true);

        // All tables in db schema should be included except for a
        assertThat(isCacheEnabled(lister, "db", "table1")).isTrue();
        assertThat(isCacheEnabled(lister, "db", "production")).isTrue();
        assertThat(isCacheEnabled(lister, "db", "temp_table")).isTrue();
        assertThat(isCacheEnabled(lister, "db", "a")).isFalse();

        // Other schemas should still not be cached
        assertThat(isCacheEnabled(lister, "other", "table")).isFalse();
    }

    private boolean isCacheEnabled(CachingDirectoryLister lister, String schema, String table)
    {
        return lister.isCacheEnabledFor(new SchemaTableName(schema, table));
    }

    protected Optional<Table> getTable(String schemaName, String tableName)
    {
        return metastore.getTable(schemaName, tableName);
    }

    protected void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        metastore.createTable(table, principalPrivileges);
    }

    protected void dropTable(String schemaName, String tableName, boolean deleteData)
    {
        metastore.dropTable(schemaName, tableName, deleteData);
    }

    protected String getTableLocation(String schemaName, String tableName)
    {
        return getTable(schemaName, tableName)
                .map(table -> table.getStorage().getLocation())
                .orElseThrow(() -> new NoSuchElementException(format("The table %s.%s could not be found", schemaName, tableName)));
    }

    protected String getPartitionLocation(String schemaName, String tableName, List<String> partitionValues)
    {
        Table table = getTable(schemaName, tableName)
                .orElseThrow(() -> new NoSuchElementException(format("The table %s.%s could not be found", schemaName, tableName)));

        return metastore.getPartition(table, partitionValues)
                .map(partition -> partition.getStorage().getLocation())
                .orElseThrow(() -> new NoSuchElementException(format("The partition %s from the table %s.%s could not be found", partitionValues, schemaName, tableName)));
    }

    protected boolean isCached(String path, SchemaTableName schemaTableName)
    {
        return directoryLister.isCached(Location.of(path), schemaTableName);
    }
}
