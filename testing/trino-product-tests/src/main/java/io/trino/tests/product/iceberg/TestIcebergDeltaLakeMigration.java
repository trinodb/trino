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
package io.trino.tests.product.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.tests.product.deltalake.BaseTestDeltaLakeS3Storage;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.ICEBERG_DELTA_LAKE_MIGRATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestIcebergDeltaLakeMigration
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {ICEBERG_DELTA_LAKE_MIGRATION, PROFILE_SPECIFIC_TESTS})
    public void testMigrateDeltaLakeTable()
    {
        String tableName = "test_migrate_delta_lake_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "default." + tableName;

        onDelta().executeQuery(format("CREATE TABLE " + sparkTableName + " USING DELTA LOCATION 's3://%s/%s' AS SELECT " +
                "    true a_boolean," +
                "     67 a_tinyint," +
                "     35 a_smallint," +
                "    CAST(-1546831166 AS INT) an_integer," +
                "    1544323431676534245 a_bigint," +
                "    12345.67F a_real," +
                "    12345.678901234D a_double," +
                "    CAST('1234567.8901' AS decimal(11, 4)) a_short_decimal," +
                "    CAST('1234567890123456789.0123456' AS decimal(26, 7)) a_long_decimal," +
                "    'some longer string' an_unbounded_varchar," +
                "    X'65683F' a_varbinary," +
                "    DATE '2005-09-10' a_date," +
                "    ARRAY(1, 10, 100, 1000) an_array_of_ints," +
                "    map('map_key', ARRAY(1, 2, 3)) a_map_with_int_array," +
                "    named_struct('struct_name', 42) a_row_with_int", bucketName, tableName));

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");
        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .containsOnly(row(
                        true,
                        67,
                        35,
                        -1546831166,
                        1544323431676534245L,
                        12345.67F,
                        12345.678901234D,
                        new BigDecimal("1234567.8901"),
                        new BigDecimal("1234567890123456789.0123456"),
                        "some longer string",
                        new byte[]{(byte) 0x65, (byte) 0x68, (byte) 0x3F},
                        Date.valueOf(LocalDate.of(2005, 9, 10)),
                        ImmutableList.of(1, 10, 100, 1000),
                        ImmutableMap.of("map_key", ImmutableList.of(1, 2, 3)),
                        io.trino.jdbc.Row.builder().addField("struct_name", 42).build()));

        onTrino().executeQuery("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test(groups = {ICEBERG_DELTA_LAKE_MIGRATION, PROFILE_SPECIFIC_TESTS})
    public void testMigratePartitionedDeltaLakeTable()
    {
        String tableName = "test_migrate_partitioned_delta_lake_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "default." + tableName;

        onDelta().executeQuery(format("CREATE TABLE " + sparkTableName + " USING DELTA LOCATION 's3://%s/%s' PARTITIONED BY " +
                "   (a_boolean, a_tinyint, a_smallint, an_integer, a_bigint, a_real, a_double, a_short_decimal, a_long_decimal, an_unbounded_varchar, a_date)" +
                " AS SELECT " +
                "    true a_boolean," +
                "     67 a_tinyint," +
                "     35 a_smallint," +
                "    CAST(-1546831166 AS INT) an_integer," +
                "    1544323431676534245 a_bigint," +
                "    12345.67F a_real," +
                "    12345.678901234D a_double," +
                "    CAST('1234567.8901' AS decimal(11, 4)) a_short_decimal," +
                "    CAST('1234567890123456789.0123456' AS decimal(26, 7)) a_long_decimal," +
                "    'some longer string' an_unbounded_varchar," +
                "    X'65683F' a_varbinary," +
                "    DATE '2005-09-10' a_date," +
                "    ARRAY(1, 10, 100, 1000) an_array_of_ints," +
                "    map('map_key', ARRAY(1, 2, 3)) a_map_with_int_array," +
                "    named_struct('struct_name', 42) a_row_with_int", bucketName, tableName));

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");
        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .containsOnly(row(
                        true,
                        67,
                        35,
                        -1546831166,
                        1544323431676534245L,
                        12345.67F,
                        12345.678901234D,
                        new BigDecimal("1234567.8901"),
                        new BigDecimal("1234567890123456789.0123456"),
                        "some longer string",
                        new byte[]{(byte) 0x65, (byte) 0x68, (byte) 0x3F},
                        Date.valueOf(LocalDate.of(2005, 9, 10)),
                        ImmutableList.of(1, 10, 100, 1000),
                        ImmutableMap.of("map_key", ImmutableList.of(1, 2, 3)),
                        io.trino.jdbc.Row.builder().addField("struct_name", 42).build()));

        onTrino().executeQuery("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test(groups = {ICEBERG_DELTA_LAKE_MIGRATION, PROFILE_SPECIFIC_TESTS})
    public void testMigratedTableStats()
    {
        String tableName = "test_migrate_table_stats_" + randomNameSuffix();
        String sparkTableName = "default." + tableName;
        String deltaTableName = "delta.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        onDelta().executeQuery(format("CREATE TABLE " + sparkTableName + "(id INT, a_double DOUBLE) USING DELTA LOCATION 's3://%s/%s'", bucketName, tableName));
        onDelta().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 1.0), (10, 10.123), (3, null)");
        onDelta().executeQuery("OPTIMIZE " + sparkTableName);

        onTrino().executeQuery("ANALYZE " + deltaTableName);

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + deltaTableName))
                .containsOnly(
                        row("id", null, 3.0, 0.0, null, "1", "10"),
                        row("a_double", null, 2.0, 1.0 / 3, null, "1.0", "10.123"),
                        row(null, null, null, null, 3.0, null, null));

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");
        // Migrating NDV stats is not yet supported
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + icebergTableName))
                .containsOnly(
                        row("id", null, null, 0.0, null, "1", "10"),
                        row("a_double", null, null, 1.0 / 3, null, "1.0", "10.123"),
                        row(null, null, null, null, 3.0, null, null));

        String icebergFilesTable = "iceberg.default.\"" + tableName + "$files\"";
        assertThat(onTrino().executeQuery("SELECT record_count, value_counts, null_value_counts, nan_value_counts, lower_bounds, upper_bounds FROM " + icebergFilesTable))
                .contains(row(
                        3,
                        ImmutableMap.of(1, 3L, 2, 3L),
                        ImmutableMap.of(1, 0L, 2, 1L),
                        ImmutableMap.of(),
                        ImmutableMap.of(1, "1", 2, "1.0"),
                        ImmutableMap.of(1, "10", 2, "10.123")));
    }

    @Test(groups = {ICEBERG_DELTA_LAKE_MIGRATION, PROFILE_SPECIFIC_TESTS})
    public void testMigrateUnsupportedWriterVersion()
    {
        String tableName = "test_migrate_unsupported_writer_version_" + randomNameSuffix();
        onDelta().executeQuery(format("CREATE TABLE default." + tableName + " USING DELTA LOCATION 's3://%s/%s' TBLPROPERTIES ('delta.minWriterVersion' = 3) AS SELECT 1 x", bucketName, tableName));
        assertQueryFailure(() -> onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')"))
                .hasMessageContaining("Migration of Delta Lake tables using writer versions above 2 is not supported");
        onTrino().executeQuery("DROP TABLE IF EXISTS delta.default." + tableName);
    }

    @Test(groups = {ICEBERG_DELTA_LAKE_MIGRATION, PROFILE_SPECIFIC_TESTS})
    public void testMigrateUnsupportedReaderVersion()
    {
        String tableName = "test_migrate_unsupported_reader_version_" + randomNameSuffix();
        onDelta().executeQuery(format("CREATE TABLE default." + tableName + " USING DELTA LOCATION 's3://%s/%s' TBLPROPERTIES ('delta.minReaderVersion' = 2) AS SELECT 1 x", bucketName, tableName));
        assertQueryFailure(() -> onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')"))
                .hasMessageContaining("Migration of Delta Lake tables using reader versions above 1 is not supported");
        onTrino().executeQuery("DROP TABLE IF EXISTS delta.default." + tableName);
    }
}
