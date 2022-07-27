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

import io.trino.tempto.ProductTest;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
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

        Assertions.assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE " + icebergTableName).getOnlyValue())
                        .contains("partitioning = ARRAY['part']");

        onTrino().executeQuery("DROP TABLE IF EXISTS " + icebergTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMigrateHiveBucketedTable()
    {
        String tableName = "test_migrate_bucketed_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        onTrino().executeQuery("" +
                "CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket'], bucket_count = 10)" +
                "AS SELECT 1 bucket, 'test' part");

        assertThatThrownBy(() -> onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')"))
                .hasStackTraceContaining("Cannot migrate bucketed table");

        assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .hasMessageContaining("Not an Iceberg table: default." + tableName);
        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveTableName))
                .containsOnly(row(1, "test"));

        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
    }

    @Test
    public void testMigrateHiveBucketedOnMultipleColumns()
    {
        String tableName = "test_migrate_bucketed_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        onTrino().executeQuery("" +
                "CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part'], bucketed_by = ARRAY['bucket', 'another_bucket'], bucket_count = 10)" +
                "AS SELECT 1 bucket, 'a' another_bucket, 'test' part");

        assertThatThrownBy(() -> onTrino().executeQuery("CALL iceberg.system.migrate('default', '" + tableName + "')"))
                .hasStackTraceContaining("Cannot migrate bucketed table");

        assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM " + icebergTableName))
                .hasMessageContaining("Not an Iceberg table: default." + tableName);
        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveTableName))
                .containsOnly(row(1, "a", "test"));

        onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
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
        onTrino().executeQuery(format("call system.rollback_to_snapshot('default', '%s', %d)", tableName, snapshotId));
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", tableName)))
                .containsOnly(row(1));
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testRollbackToSnapshotWithNullArgument()
    {
        onTrino().executeQuery("USE iceberg.default");
        assertQueryFailure(() -> onTrino().executeQuery("CALL system.rollback_to_snapshot(NULL, 'customer_orders', 8954597067493422955)"))
                .hasMessageMatching(".*schema cannot be null.*");
        assertQueryFailure(() -> onTrino().executeQuery("CALL system.rollback_to_snapshot('testdb', NULL, 8954597067493422955)"))
                .hasMessageMatching(".*table cannot be null.*");
        assertQueryFailure(() -> onTrino().executeQuery("CALL system.rollback_to_snapshot('testdb', 'customer_orders', NULL)"))
                .hasMessageMatching(".*snapshot_id cannot be null.*");
    }

    private long getSecondOldestTableSnapshot(String tableName)
    {
        return (Long) onTrino().executeQuery(
                format("SELECT snapshot_id FROM iceberg.default.\"%s$snapshots\" WHERE parent_id IS NOT NULL ORDER BY committed_at FETCH FIRST 1 ROW WITH TIES", tableName))
                .getOnlyValue();
    }
}
