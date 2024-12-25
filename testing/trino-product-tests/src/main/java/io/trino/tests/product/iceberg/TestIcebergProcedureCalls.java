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
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert.Row;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Consumer;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TableLocationUtils.getTableLocation;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergProcedureCalls
        extends ProductTest
{
    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMigrateHiveTable()
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        onTrino().executeQuery("CREATE TABLE " + hiveTableName + " AS SELECT 1 x");

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .containsOnly(row(1));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1));

        onTrino().executeQuery("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMigrateTimestampHiveTableWithOrc()
    {
        testMigrateTimestampHiveTable(
                "MILLISECONDS",
                "2021-01-01 10:11:12.123",
                "2021-01-01 10:11:12.123000",
                "2021-01-01 10:11:12.123",
                "ORC");

        testMigrateTimestampHiveTable(
                "MICROSECONDS",
                "2021-01-01 10:11:12.123456",
                "2021-01-01 10:11:12.123456",
                "2021-01-01 10:11:12.123456",
                "ORC");

        testMigrateTimestampHiveTable(
                "NANOSECONDS",
                "2021-01-01 10:11:12.123456789",
                "2021-01-01 10:11:12.123457",
                "2021-01-01 10:11:12.123456",
                "ORC");
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMigrateTimestampHiveTableWithParquet()
    {
        testMigrateTimestampHiveTable(
                "MILLISECONDS",
                "2021-01-01 10:11:12.123",
                "2021-01-01 10:11:12.123000 UTC",
                "2021-01-01 10:11:12.123",
                "PARQUET");

        testMigrateTimestampHiveTable(
                "MICROSECONDS",
                "2021-01-01 10:11:12.123456",
                "2021-01-01 10:11:12.123456 UTC",
                "2021-01-01 10:11:12.123456",
                "PARQUET");

        testMigrateTimestampHiveTable(
                "NANOSECONDS",
                "2021-01-01 10:11:12.123456789",
                "2021-01-01 10:11:12.123457 UTC",
                "2021-01-01 10:11:12.123456",
                "PARQUET");
    }

    private void testMigrateTimestampHiveTable(String precisionName, String inputValue, String expectedValueInTrino, String expectedValueInSpark, String format)
    {
        String tableName = "test_migrate_timestamp_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        onTrino().executeQuery("DROP TABLE IF EXISTS default." + tableName);
        onTrino().executeQuery("SET SESSION hive.timestamp_precision = '" + precisionName + "'");
        onTrino().executeQuery("CREATE TABLE " + hiveTableName + " WITH (format='" + format + "') AS SELECT TIMESTAMP '" + inputValue + "' x ");

        assertThat(onHive().executeQuery("SELECT CAST(x AS string) FROM default." + tableName))
                .containsOnly(row(inputValue));
        assertThat(onTrino().executeQuery("SELECT CAST(x AS varchar) FROM " + hiveTableName))
                .containsOnly(row(inputValue));

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(onTrino().executeQuery("SELECT CAST(x AS varchar) FROM " + icebergTableName))
                .containsOnly(row(expectedValueInTrino));
        assertThat(onSpark().executeQuery("SELECT CAST(x AS string) FROM " + sparkTableName))
                .containsOnly(row(expectedValueInSpark));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "fileFormats")
    public void testMigrateHiveTableWithTinyintType(String fileFormat)
    {
        String tableName = "test_migrate_tinyint" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        String createTable = "CREATE TABLE " + hiveTableName + "(col TINYINT) WITH (format = '" + fileFormat + "')";
        if (fileFormat.equals("AVRO")) {
            assertQueryFailure(() -> onTrino().executeQuery(createTable))
                    .hasMessageContaining("Column 'col' is tinyint, which is not supported by Avro. Use integer instead.");
            return;
        }
        onTrino().executeQuery(createTable);
        onTrino().executeQuery("INSERT INTO " + hiveTableName + " VALUES -128, 127");

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");

        List<Row> expected = ImmutableList.of(row(-128), row(127));
        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "fileFormats")
    public void testMigrateHiveTableWithSmallintType(String fileFormat)
    {
        String tableName = "test_migrate_smallint" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        String createTable = "CREATE TABLE " + hiveTableName + "(col SMALLINT) WITH (format = '" + fileFormat + "')";
        if (fileFormat.equals("AVRO")) {
            assertQueryFailure(() -> onTrino().executeQuery(createTable))
                    .hasMessageContaining("Column 'col' is smallint, which is not supported by Avro. Use integer instead.");
            return;
        }
        onTrino().executeQuery(createTable);
        onTrino().executeQuery("INSERT INTO " + hiveTableName + " VALUES -32768, 32767");

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");

        List<Row> expected = ImmutableList.of(row(-32768), row(32767));
        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "fileFormats")
    public void testMigrateHiveTableWithComplexType(String fileFormat)
    {
        String tableName = "test_migrate_complex_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;
        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        onTrino().executeQuery("CREATE TABLE " + hiveTableName + " WITH (format='" + fileFormat + "') AS " +
                "SELECT 1 x, " +
                "array[2, 3] a, " +
                "CAST(map(array['key'], array['value']) AS map(varchar, varchar)) b, " +
                "CAST(row(1) AS row(d integer)) c");

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(onTrino().executeQuery("SELECT x, a[1], a[2], b['key'], c.d FROM " + icebergTableName))
                .containsOnly(row(1, 2, 3, "value", 1));
        assertThat(onSpark().executeQuery("SELECT x, element_at(a, 1), element_at(a, 2), element_at(b, 'key'), c.d FROM " + sparkTableName))
                .containsOnly(row(1, 2, 3, "value", 1));

        onTrino().executeQuery("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMigrateHivePartitionedTable()
    {
        String tableName = "test_migrate_partitioned_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        onTrino().executeQuery("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT 1 x, 'test' part");

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .containsOnly(row(1, "test"));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1, "test"));

        assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE " + icebergTableName).getOnlyValue())
                        .contains("partitioning = ARRAY['part']");

        onTrino().executeQuery("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMigrateHiveBucketedTable()
    {
        String tableName = "test_migrate_bucketed_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        onTrino().executeQuery("" +
                "CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket'], bucket_count = 10)" +
                "AS SELECT 1 bucket, 'test' part");

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .containsOnly(row(1, "test"));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1, "test"));

        assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE " + icebergTableName).getOnlyValue())
                .contains("partitioning = ARRAY['part']");

        onTrino().executeQuery("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMigrateHiveBucketedOnMultipleColumns()
    {
        String tableName = "test_migrate_bucketed_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        onTrino().executeQuery("" +
                "CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket', 'another_bucket'], bucket_count = 10)" +
                "AS SELECT 1 bucket, 'a' another_bucket, 'test' part");

        onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')");

        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .containsOnly(row(1, "a", "test"));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1, "a", "test"));

        assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE " + icebergTableName).getOnlyValue())
                .contains("partitioning = ARRAY['part']");

        onTrino().executeQuery("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoMigrateExternalTable()
    {
        migrateExternalTable(tableName -> onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')"));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkMigrateExternalTable()
    {
        migrateExternalTable(tableName -> onSpark().executeQuery("CALL iceberg_test.system.migrate('default." + tableName + "')"));
    }

    private void migrateExternalTable(Consumer<String> migrateTable)
    {
        String managedTableName = "test_migrate_managed_" + randomNameSuffix();
        String externalTableName = "test_migrate_external_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + externalTableName;
        String sparkTableName = "iceberg_test.default." + externalTableName;

        onTrino().executeQuery("DROP TABLE IF EXISTS hive.default." + managedTableName);
        onTrino().executeQuery("CREATE TABLE hive.default." + managedTableName + " AS SELECT 1 x");
        String tableLocation = getTableLocation("hive.default." + managedTableName);
        onTrino().executeQuery("CREATE TABLE hive.default." + externalTableName + "(x integer) WITH (external_location = '" + tableLocation + "')");

        // Migrate an external table
        migrateTable.accept(externalTableName);

        assertThat(onTrino().executeQuery("SELECT * FROM " + icebergTableName)).containsOnly(row(1));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(row(1));

        // The migrated table behaves like managed tables because Iceberg doesn't have an external table concept
        onTrino().executeQuery("DROP TABLE " + icebergTableName);

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hive.default." + managedTableName))
                .hasMessageContaining("Partition location does not exist");
        assertThat(onHive().executeQuery("SELECT * FROM default." + managedTableName)).hasNoRows();

        onTrino().executeQuery("DROP TABLE hive.default." + managedTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMigrateUnsupportedTransactionalTable()
    {
        String tableName = "test_migrate_unsupported_transactional_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        onTrino().executeQuery("CREATE TABLE " + hiveTableName + " WITH (transactional = true) AS SELECT 1 x");

        assertThatThrownBy(() -> onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')"))
                .hasMessageContaining("Migrating transactional tables is unsupported");

        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveTableName)).containsOnly(row(1));
        assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .hasMessageContaining("Not an Iceberg table");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testRollbackToSnapshot()
            throws InterruptedException
    {
        String tableName = "test_rollback_to_snapshot_" + randomNameSuffix();

        onTrino().executeQuery("USE iceberg.default");
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onTrino().executeQuery(format("CREATE TABLE %s (a INTEGER)", tableName));
        Thread.sleep(1);
        onTrino().executeQuery(format("INSERT INTO %s VALUES 1", tableName));
        Thread.sleep(1);
        onTrino().executeQuery(format("INSERT INTO %s VALUES 2", tableName));
        long snapshotId = getSecondOldestTableSnapshot(tableName);
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE rollback_to_snapshot(%d)", tableName, snapshotId));
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", tableName)))
                .containsOnly(row(1));
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
    }

    private long getSecondOldestTableSnapshot(String tableName)
    {
        return (Long) onTrino().executeQuery(
                format("SELECT snapshot_id FROM iceberg.default.\"%s$snapshots\" WHERE parent_id IS NOT NULL ORDER BY committed_at FETCH FIRST 1 ROW WITH TIES", tableName))
                .getOnlyValue();
    }

    @DataProvider
    public static Object[][] fileFormats()
    {
        return new Object[][] {{"ORC"}, {"PARQUET"}, {"AVRO"}};
    }
}
