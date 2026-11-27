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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.BUCKET_PREFIX;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.CATALOG_WITHOUT_NATIVE_S3;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.CATALOG_WITHOUT_STAGING;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.CATALOG_WITH_RECURSIVE_DIRECTORIES;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.CATALOG_WITH_UNREACHABLE_S3;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.EXTENDED_SETTINGS_CATALOG;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.S3_MOUNT_PATH;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.VIEWFS_S3_MOUNT_PATH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests Hive tables whose {@code hdfs://} location is resolved by the Hadoop ViewFS overload
 * scheme ({@code ViewDistributedFileSystem}), either to the HDFS fallback or to an {@code s3a://}
 * mount link.
 * <p>
 * Tables behind a plain {@code viewfs://} mount table are covered as well.
 * <p>
 * Without the legacy S3 file system, nothing provides a Hadoop {@code FileSystem} for {@code s3a}
 * unless native S3 is enabled in the catalog. Otherwise ViewFS falls back to
 * {@code org.apache.hadoop.fs.s3a.S3AFileSystem}, which Trino does not ship, and reading tables
 * behind an {@code s3a://} mount link fails with {@code ClassNotFoundException}.
 */
@ProductTest
@RequiresEnvironment(HiveViewFsEnvironment.class)
@TestGroup.HiveViewFs
@TestGroup.ProfileSpecificTests
class TestHiveViewFsMountLinks
{
    @Test
    void testReadTableOnViewFsHdfsFallback(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_hdfs_fallback";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        try {
            env.executeTrinoUpdate("CREATE TABLE hive.default." + tableName + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)");
            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"), row(3, "c"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    void testReadTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            // hdfs:// alias that ViewFS resolves to s3a://viewfs-bucket/s3_data/<table>
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n2,b\n3,c\n");

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"), row(3, "c"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadPartitionedTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_partitioned";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    PARTITIONED BY (event_date STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "event_date=2026-11-01/data.csv", "1,a\n2,b\n");
            env.putS3Object(objectPrefix + "event_date=2026-11-02/data.csv", "3,c\n");

            env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");

            assertThat(env.executeTrino("SELECT id, name, event_date FROM hive.default." + tableName))
                    .containsOnly(
                            row(1, "a", "2026-11-01"),
                            row(2, "b", "2026-11-01"),
                            row(3, "c", "2026-11-02"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadMultiLevelPartitionedTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_multi_level";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    PARTITIONED BY (region STRING, category STRING, event_date STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "region=eu/category=a/event_date=2026-11-01/data.csv", "1,a\n");
            env.putS3Object(objectPrefix + "region=eu/category=a/event_date=2026-11-02/data.csv", "2,b\n");
            env.putS3Object(objectPrefix + "region=eu/category=b/event_date=2026-11-01/data.csv", "3,c\n");
            env.putS3Object(objectPrefix + "region=us/category=a/event_date=2026-11-01/data.csv", "4,d\n5,e\n");
            // An empty partition directory, represented only by a directory marker object
            env.putS3Object(objectPrefix + "region=eu/category=empty/", "");

            // Discovering partitions lists every directory level through ViewFS
            env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");

            assertThat(env.executeTrino("SELECT region, category, event_date FROM hive.default.\"" + tableName + "$partitions\""))
                    .containsOnly(
                            row("eu", "a", "2026-11-01"),
                            row("eu", "a", "2026-11-02"),
                            row("eu", "b", "2026-11-01"),
                            row("us", "a", "2026-11-01"));
            assertThat(env.executeTrino("SELECT id, name, region, category, event_date FROM hive.default." + tableName))
                    .containsOnly(
                            row(1, "a", "eu", "a", "2026-11-01"),
                            row(2, "b", "eu", "a", "2026-11-02"),
                            row(3, "c", "eu", "b", "2026-11-01"),
                            row(4, "d", "us", "a", "2026-11-01"),
                            row(5, "e", "us", "a", "2026-11-01"));
            assertThat(env.executeTrino("SELECT id FROM hive.default." + tableName + " WHERE region = 'eu' AND category = 'a'"))
                    .containsOnly(row(1), row(2));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadTableOnViewFsSchemeMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_scheme_s3_mount_link";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            // viewfs://cluster/s3_data/<table> resolves to s3a://viewfs-bucket/s3_data/<table>
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.viewFsLocation(VIEWFS_S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n2,b\n");

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadTableWithNonDataFilesOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_non_data_files";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n2,b\n");
            // Files written by other engines and tools next to the data, which must not appear in the results
            env.putS3Object(objectPrefix + "_SUCCESS", "");
            env.putS3Object(objectPrefix + ".hidden", "9,hidden\n");
            env.putS3Object(objectPrefix + "subdirectory_$folder$", "");
            env.putS3Object(objectPrefix + "subdirectory/", "");
            env.putS3Object(objectPrefix + "subdirectory/nested.csv", "8,nested\n");
            // An empty directory, represented only by a directory marker object
            env.putS3Object(objectPrefix + "empty_subdirectory/", "");

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadTableWithRecursiveDirectoriesOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_recursive_directories";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,top\n");
            env.putS3Object(objectPrefix + "subdirectory/nested.csv", "2,nested\n");
            env.putS3Object(objectPrefix + "subdirectory/deeper/deep.csv", "3,deep\n");
            // An empty directory, represented only by a directory marker object, and files that are not data
            env.putS3Object(objectPrefix + "empty_subdirectory/", "");
            env.putS3Object(objectPrefix + "_SUCCESS", "");
            env.putS3Object(objectPrefix + ".hidden", "9,hidden\n");

            // With recursive directories, files in all subdirectories are read through ViewFS
            assertThat(env.executeTrino("SELECT id, name FROM %s.default.%s".formatted(CATALOG_WITH_RECURSIVE_DIRECTORIES, tableName)))
                    .containsOnly(row(1, "top"), row(2, "nested"), row(3, "deep"));
            // Without, only the files directly in the table directory are read
            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "top"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadTableOnViewFsMountLinkToS3WithoutNativeS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_without_native_s3";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n");

            // Without native S3 in the catalog, nothing provides a Hadoop file system for s3a://
            assertThatThrownBy(() -> env.executeTrino("SELECT id, name FROM %s.default.%s".formatted(CATALOG_WITHOUT_NATIVE_S3, tableName)))
                    .hasMessageContaining("Class org.apache.hadoop.fs.s3a.S3AFileSystem not found");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testCatalogsDoNotShareViewFsMountLinkFileSystems(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_two_catalogs";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n");

            // Both catalogs read the same mount link as the same user. Each must use its own native S3
            // settings: a file system shared with the first catalog would make the second read succeed.
            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"));
            // The S3 connection error is not part of the query error message, only the listing that failed
            assertThatThrownBy(() -> env.executeTrino("SELECT id, name FROM %s.default.%s".formatted(CATALOG_WITH_UNREACHABLE_S3, tableName)))
                    .hasMessageContaining("Failed to list directory: " + env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName));
            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testInsertWithoutStagingIntoTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_insert_without_staging";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n");

            // This catalog writes directly to the table directory, without a temporary staging directory
            env.executeTrinoUpdate("INSERT INTO %s.default.%s VALUES (2, 'b')".formatted(CATALOG_WITHOUT_STAGING, tableName));

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"));
            Assertions.assertThat(env.listS3Objects(objectPrefix)).hasSizeGreaterThan(1);
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testInsertWithoutStagingIntoEmptyTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_insert_without_staging_empty";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            // No objects exist under the table location yet
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));

            env.executeTrinoUpdate("INSERT INTO %s.default.%s VALUES (1, 'a')".formatted(CATALOG_WITHOUT_STAGING, tableName));

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"));
            Assertions.assertThat(env.listS3Objects(objectPrefix)).isNotEmpty();
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testCreateTableAsSelectOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_ctas";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            // CREATE TABLE AS SELECT with an external location writes directly to the target directory
            env.executeTrinoUpdate(
                    """
                    CREATE TABLE hive.default.%s
                    WITH (format = 'TEXTFILE', external_location = '%s')
                    AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"));
            Assertions.assertThat(env.listS3Objects(objectPrefix)).isNotEmpty();
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testExtendedSettingsPartitionedAvroTable(HiveViewFsEnvironment env)
    {
        String writerTableName = "test_viewfs_extended_avro_writer";
        String tableName = "test_viewfs_extended_avro";
        String location = env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName);
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, writerTableName));
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            // Write Avro data files through the mount link
            env.executeTrinoUpdate(
                    """
                    CREATE TABLE %s.default.%s
                    WITH (format = 'AVRO', external_location = '%s', partitioned_by = ARRAY['region', 'category', 'event_date'])
                    AS SELECT CAST(id AS integer) id, CAST(name AS varchar) name, CAST(region AS varchar) region, CAST(category AS varchar) category, CAST(event_date AS varchar) event_date
                    FROM (VALUES (1, 'a', 'eu', 'x', '2026-11-01'), (2, 'b', 'eu', 'x', '2026-11-02'), (3, 'c', 'eu', 'y', '2026-11-01'), (4, 'd', 'us', 'x', '2026-11-01'), (5, 'e', 'us', 'x', '2026-11-01'))
                        t(id, name, region, category, event_date)
                    """.formatted(EXTENDED_SETTINGS_CATALOG, writerTableName, location));
            env.executeTrinoUpdate("DROP TABLE %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, writerTableName));

            // Define the table in Hive, discover its partitions, and read it
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    PARTITIONED BY (region STRING, category STRING, event_date STRING)
                    STORED AS AVRO
                    LOCATION '%s'
                    """.formatted(tableName, location));
            env.executeTrinoUpdate("CALL %s.system.sync_partition_metadata('default', '%s', 'FULL')".formatted(EXTENDED_SETTINGS_CATALOG, tableName));

            assertThat(env.executeTrino("SELECT count(*) FROM %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, tableName)))
                    .containsOnly(row(5L));
            assertThat(env.executeTrino("SELECT region, category, event_date, count(*) FROM %s.default.%s GROUP BY 1, 2, 3".formatted(EXTENDED_SETTINGS_CATALOG, tableName)))
                    .containsOnly(
                            row("eu", "x", "2026-11-01", 1L),
                            row("eu", "x", "2026-11-02", 1L),
                            row("eu", "y", "2026-11-01", 1L),
                            row("us", "x", "2026-11-01", 2L));
            assertThat(env.executeTrino("SELECT id, name FROM %s.default.%s WHERE region = 'us'".formatted(EXTENDED_SETTINGS_CATALOG, tableName)))
                    .containsOnly(row(4, "d"), row(5, "e"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, writerTableName));
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testExtendedSettingsParquetColumnNames(HiveViewFsEnvironment env)
    {
        String writerTableName = "test_viewfs_extended_parquet_writer";
        String tableName = "test_viewfs_extended_parquet";
        String location = env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName);
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, writerTableName));
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeTrinoUpdate(
                    """
                    CREATE TABLE %s.default.%s
                    WITH (format = 'PARQUET', external_location = '%s')
                    AS SELECT CAST(id AS integer) id, CAST(name AS varchar) name FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)
                    """.formatted(EXTENDED_SETTINGS_CATALOG, writerTableName, location));
            env.executeTrinoUpdate("DROP TABLE %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, writerTableName));

            // Columns declared in a different order than in the files are matched by name
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (name STRING, id INT)
                    STORED AS PARQUET
                    LOCATION '%s'
                    """.formatted(tableName, location));

            assertThat(env.executeTrino("SELECT id, name FROM %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, tableName)))
                    .containsOnly(row(1, "a"), row(2, "b"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, writerTableName));
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testExtendedSettingsIgnoreAbsentPartitions(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_extended_absent_partitions";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, tableName));
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeTrinoUpdate(
                    """
                    CREATE TABLE %s.default.%s
                    WITH (format = 'TEXTFILE', external_location = '%s', partitioned_by = ARRAY['event_date'])
                    AS SELECT CAST(id AS integer) id, CAST(event_date AS varchar) event_date FROM (VALUES (1, '2026-11-01'), (2, '2026-11-02')) t(id, event_date)
                    """.formatted(EXTENDED_SETTINGS_CATALOG, tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            // Remove the data of one partition, which stays registered in the metastore
            env.deleteS3Objects(objectPrefix + "event_date=2026-11-02/");

            assertThat(env.executeTrino("SELECT id FROM %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, tableName)))
                    .containsOnly(row(1));
            assertThatThrownBy(() -> env.executeTrino("SELECT id FROM hive.default." + tableName))
                    .hasMessageContaining("Partition location does not exist");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, tableName));
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testExtendedSettingsHiveView(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_extended_view_table";
        String viewName = "test_viewfs_extended_view";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP VIEW IF EXISTS default." + viewName);
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n2,b\n");
            env.executeHiveUpdate("CREATE VIEW default.%s AS SELECT id, name FROM default.%s WHERE id > 1".formatted(viewName, tableName));

            assertThat(env.executeTrino("SELECT id, name FROM %s.default.%s".formatted(EXTENDED_SETTINGS_CATALOG, viewName)))
                    .containsOnly(row(2, "b"));
        }
        finally {
            env.executeHiveUpdate("DROP VIEW IF EXISTS default." + viewName);
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }
}
