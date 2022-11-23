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
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestIcebergProcedureCalls
        extends ProductTest
{
    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testRollbackToSnapshot()
            throws InterruptedException
    {
        String tableName = "test_rollback_to_snapshot_" + randomTableSuffix();

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

    private long getSecondOldestTableSnapshot(String tableName)
    {
        return (Long) onTrino().executeQuery(
                format("SELECT snapshot_id FROM iceberg.default.\"%s$snapshots\" WHERE parent_id IS NOT NULL ORDER BY committed_at FETCH FIRST 1 ROW WITH TIES", tableName))
                .getOnlyValue();
    }
}
