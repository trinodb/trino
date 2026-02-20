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
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Iceberg stored procedure calls (migrate, rollback_to_snapshot, etc).
 * <p>
 * Ported from the Tempto-based TestIcebergProcedureCalls.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergEnvironment.class)
@TestGroup.Iceberg
class TestIcebergProcedureCalls
{
    @Test
    void testMigrateHiveTable(SparkIcebergEnvironment env)
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + hiveTableName);
        env.executeTrinoUpdate("CREATE TABLE " + hiveTableName + " AS SELECT 1 x");

        env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(env.executeTrino("SELECT * FROM " + icebergTableName))
                .containsOnly(row(1));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1));

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test
    void testMigrateTimestampHiveTableWithOrc(SparkIcebergEnvironment env)
    {
        testMigrateTimestampHiveTable(
                env,
                "MILLISECONDS",
                "2021-01-01 10:11:12.123",
                "2021-01-01 10:11:12.123000",
                "2021-01-01 10:11:12.123",
                "ORC");

        testMigrateTimestampHiveTable(
                env,
                "MICROSECONDS",
                "2021-01-01 10:11:12.123456",
                "2021-01-01 10:11:12.123456",
                "2021-01-01 10:11:12.123456",
                "ORC");

        testMigrateTimestampHiveTable(
                env,
                "NANOSECONDS",
                "2021-01-01 10:11:12.123456789",
                "2021-01-01 10:11:12.123457",
                "2021-01-01 10:11:12.123456",
                "ORC");
    }

    @Test
    void testMigrateTimestampHiveTableWithParquet(SparkIcebergEnvironment env)
    {
        testMigrateTimestampHiveTable(
                env,
                "MILLISECONDS",
                "2021-01-01 10:11:12.123",
                "2021-01-01 10:11:12.123000 UTC",
                "2021-01-01 10:11:12.123",
                "PARQUET");

        testMigrateTimestampHiveTable(
                env,
                "MICROSECONDS",
                "2021-01-01 10:11:12.123456",
                "2021-01-01 10:11:12.123456 UTC",
                "2021-01-01 10:11:12.123456",
                "PARQUET");

        testMigrateTimestampHiveTable(
                env,
                "NANOSECONDS",
                "2021-01-01 10:11:12.123456789",
                "2021-01-01 10:11:12.123457 UTC",
                "2021-01-01 10:11:12.123456",
                "PARQUET");
    }

    private void testMigrateTimestampHiveTable(SparkIcebergEnvironment env, String precisionName, String inputValue, String expectedValueInTrino, String expectedValueInSpark, String format)
    {
        String tableName = "test_migrate_timestamp_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;
        String timestampType = switch (precisionName) {
            case "MILLISECONDS" -> "TIMESTAMP(3)";
            case "MICROSECONDS" -> "TIMESTAMP(6)";
            case "NANOSECONDS" -> "TIMESTAMP(9)";
            default -> throw new IllegalArgumentException("Unsupported precision: " + precisionName);
        };

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + hiveTableName);
        // Session properties are connection-scoped; set and create in one Trino session.
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.timestamp_precision = '" + precisionName + "'");
            session.executeUpdate("CREATE TABLE " + hiveTableName + " WITH (format='" + format + "') AS SELECT CAST('" + inputValue + "' AS " + timestampType + ") x ");
        });

        assertThat(env.executeHive("SELECT CAST(x AS string) FROM default." + tableName))
                .containsOnly(row(inputValue));
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.timestamp_precision = '" + precisionName + "'");
            assertThat(session.executeQuery("SELECT CAST(x AS varchar) FROM " + hiveTableName))
                    .containsOnly(row(inputValue));
        });

        env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(env.executeTrino("SELECT CAST(x AS varchar) FROM " + icebergTableName))
                .containsOnly(row(expectedValueInTrino));
        assertThat(env.executeSpark("SELECT CAST(x AS string) FROM " + sparkTableName))
                .containsOnly(row(expectedValueInSpark));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    static Stream<String> fileFormats()
    {
        return Stream.of("ORC", "PARQUET", "AVRO");
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    void testMigrateHiveTableWithTinyintType(String fileFormat, SparkIcebergEnvironment env)
    {
        String tableName = "test_migrate_tinyint" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        String createTable = "CREATE TABLE " + hiveTableName + "(col TINYINT) WITH (format = '" + fileFormat + "')";
        if (fileFormat.equals("AVRO")) {
            assertThatThrownBy(() -> env.executeTrinoUpdate(createTable))
                    .hasMessageContaining("Column 'col' is tinyint, which is not supported by Avro. Use integer instead.");
            return;
        }
        env.executeTrinoUpdate(createTable);
        env.executeTrinoUpdate("INSERT INTO " + hiveTableName + " VALUES -128, 127");

        env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')");

        List<Row> expected = ImmutableList.of(row(-128), row(127));
        assertThat(env.executeTrino("SELECT * FROM " + icebergTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    void testMigrateHiveTableWithSmallintType(String fileFormat, SparkIcebergEnvironment env)
    {
        String tableName = "test_migrate_smallint" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        String createTable = "CREATE TABLE " + hiveTableName + "(col SMALLINT) WITH (format = '" + fileFormat + "')";
        if (fileFormat.equals("AVRO")) {
            assertThatThrownBy(() -> env.executeTrinoUpdate(createTable))
                    .hasMessageContaining("Column 'col' is smallint, which is not supported by Avro. Use integer instead.");
            return;
        }
        env.executeTrinoUpdate(createTable);
        env.executeTrinoUpdate("INSERT INTO " + hiveTableName + " VALUES -32768, 32767");

        env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')");

        List<Row> expected = ImmutableList.of(row(-32768), row(32767));
        assertThat(env.executeTrino("SELECT * FROM " + icebergTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @ParameterizedTest
    @MethodSource("fileFormats")
    void testMigrateHiveTableWithComplexType(String fileFormat, SparkIcebergEnvironment env)
    {
        String tableName = "test_migrate_complex_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + hiveTableName);
        env.executeTrinoUpdate("CREATE TABLE " + hiveTableName + " WITH (format='" + fileFormat + "') AS " +
                "SELECT 1 x, " +
                "array[2, 3] a, " +
                "CAST(map(array['key'], array['value']) AS map(varchar, varchar)) b, " +
                "CAST(row(1) AS row(d integer)) c");

        env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(env.executeTrino("SELECT x, a[1], a[2], b['key'], c.d FROM " + icebergTableName))
                .containsOnly(row(1, 2, 3, "value", 1));
        assertThat(env.executeSpark("SELECT x, element_at(a, 1), element_at(a, 2), element_at(b, 'key'), c.d FROM " + sparkTableName))
                .containsOnly(row(1, 2, 3, "value", 1));

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test
    void testMigrateHivePartitionedTable(SparkIcebergEnvironment env)
    {
        String tableName = "test_migrate_partitioned_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + hiveTableName);
        env.executeTrinoUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT 1 x, 'test' part");

        env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(env.executeTrino("SELECT * FROM " + icebergTableName))
                .containsOnly(row(1, "test"));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1, "test"));

        assertThat((String) env.executeTrino("SHOW CREATE TABLE " + icebergTableName).getOnlyValue())
                .contains("partitioning = ARRAY['part']");

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test
    void testMigrateHiveBucketedTable(SparkIcebergEnvironment env)
    {
        String tableName = "test_migrate_bucketed_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + hiveTableName);
        env.executeTrinoUpdate("" +
                "CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket'], bucket_count = 10)" +
                "AS SELECT 1 bucket, 'test' part");

        env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(env.executeTrino("SELECT * FROM " + icebergTableName))
                .containsOnly(row(1, "test"));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1, "test"));

        assertThat((String) env.executeTrino("SHOW CREATE TABLE " + icebergTableName).getOnlyValue())
                .contains("partitioning = ARRAY['part']");

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test
    void testMigrateHiveBucketedOnMultipleColumns(SparkIcebergEnvironment env)
    {
        String tableName = "test_migrate_bucketed_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + hiveTableName);
        env.executeTrinoUpdate("" +
                "CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket', 'another_bucket'], bucket_count = 10)" +
                "AS SELECT 1 bucket, 'a' another_bucket, 'test' part");

        env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(env.executeTrino("SELECT * FROM " + icebergTableName))
                .containsOnly(row(1, "a", "test"));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1, "a", "test"));

        assertThat((String) env.executeTrino("SHOW CREATE TABLE " + icebergTableName).getOnlyValue())
                .contains("partitioning = ARRAY['part']");

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test
    void testTrinoMigrateExternalTable(SparkIcebergEnvironment env)
    {
        migrateExternalTable(env, tableName -> env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "', 'true')"));
    }

    @Test
    void testSparkMigrateExternalTable(SparkIcebergEnvironment env)
    {
        migrateExternalTable(env, tableName -> env.executeSparkUpdate("CALL iceberg_test.system.migrate('default." + tableName + "', map('recursive_directory', 'true'))"));
    }

    private void migrateExternalTable(SparkIcebergEnvironment env, Consumer<String> migrateTable)
    {
        String managedTableName = "test_migrate_managed_" + randomNameSuffix();
        String externalTableName = "test_migrate_external_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + externalTableName;
        String sparkTableName = "iceberg_test.default." + externalTableName;

        // Previous test cases may remove the default warehouse root; recreate it for deterministic setup.
        env.createHdfsClient().createDirectory(env.getWarehouseDirectory());
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + managedTableName);
        env.executeHiveUpdate("CREATE TABLE default." + managedTableName + " (x int) STORED AS ORC");
        env.executeHiveUpdate("INSERT INTO default." + managedTableName + " VALUES (1)");
        String tableLocation = env.getWarehouseDirectory() + "/" + managedTableName;
        env.executeHiveUpdate("CREATE EXTERNAL TABLE default." + externalTableName + " (x int) STORED AS ORC LOCATION '" + tableLocation + "'");

        // Migrate an external table
        migrateTable.accept(externalTableName);

        assertThat(env.executeTrino("SELECT * FROM " + icebergTableName)).containsOnly(row(1));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(row(1));

        // The migrated table behaves like managed tables because Iceberg doesn't have an external table concept
        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);

        assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.default." + managedTableName))
                .hasMessageContaining("Partition location does not exist");
        assertThat(env.executeHive("SELECT * FROM default." + managedTableName)).hasNoRows();

        env.executeTrinoUpdate("DROP TABLE hive.default." + managedTableName);
    }

    @Test
    void testMigrateUnsupportedTransactionalTable(SparkIcebergEnvironment env)
    {
        String tableName = "test_migrate_unsupported_transactional_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        env.executeTrinoUpdate("CREATE TABLE " + hiveTableName + " WITH (transactional = true) AS SELECT 1 x");

        assertThatThrownBy(() -> env.executeTrinoUpdate("CALL iceberg.system.migrate('default', '" + tableName + "')"))
                .hasMessageContaining("Migrating transactional tables is unsupported");

        assertThat(env.executeTrino("SELECT * FROM " + hiveTableName)).containsOnly(row(1));
        assertThatThrownBy(() -> env.executeTrino("SELECT * FROM " + icebergTableName))
                .hasMessageContaining("Not an Iceberg table");

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testRollbackToSnapshot(SparkIcebergEnvironment env)
            throws InterruptedException
    {
        String tableName = "test_rollback_to_snapshot_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + tableName;

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", icebergTableName));
        env.executeTrinoUpdate(format("CREATE TABLE %s (a INTEGER)", icebergTableName));
        Thread.sleep(1);
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES 1", icebergTableName));
        Thread.sleep(1);
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES 2", icebergTableName));
        long snapshotId = getSecondOldestTableSnapshot(env, tableName);
        env.executeTrinoUpdate(format("ALTER TABLE %s EXECUTE rollback_to_snapshot(%d)", icebergTableName, snapshotId));
        assertThat(env.executeTrino(format("SELECT * FROM %s", icebergTableName)))
                .containsOnly(row(1));
        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", icebergTableName));
    }

    private long getSecondOldestTableSnapshot(SparkIcebergEnvironment env, String tableName)
    {
        return (Long) env.executeTrino(
                format("SELECT snapshot_id FROM iceberg.default.\"%s$snapshots\" WHERE parent_id IS NOT NULL ORDER BY committed_at FETCH FIRST 1 ROW WITH TIES", tableName))
                .getOnlyValue();
    }
}
