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
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.ICEBERG_FORMAT_VERSION_COMPATIBILITY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onCompatibilityTestServer;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergFormatVersionCompatibility
        extends ProductTest
{
    @Test(groups = {ICEBERG_FORMAT_VERSION_COMPATIBILITY, PROFILE_SPECIFIC_TESTS})
    public void testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino()
    {
        String baseTableName = "test_trino_time_travel_read_table_created_by_early_version_trino_" + randomNameSuffix();
        String tableName = "iceberg.default.%s".formatted(baseTableName);
        String snapshotsTableName = "iceberg.default.\"%s$snapshots\"".formatted(baseTableName);

        onCompatibilityTestServer().executeQuery("CREATE TABLE %s (c VARCHAR)".formatted(tableName));
        onCompatibilityTestServer().executeQuery("INSERT INTO %s VALUES 'a', 'b', 'c';".formatted(tableName));

        long latestSnapshotId = (long) onCompatibilityTestServer()
                .executeQuery("SELECT snapshot_id FROM %s ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES".formatted(snapshotsTableName))
                .getOnlyValue();
        assertThat(onTrino().executeQuery("SELECT snapshot_id FROM %s ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES".formatted(snapshotsTableName)))
                .containsOnly(row(latestSnapshotId));

        List<QueryAssert.Row> expected = onCompatibilityTestServer().executeQuery("SELECT * FROM %s".formatted(tableName)).rows().stream()
                .map(row -> row(row.toArray()))
                .collect(toImmutableList());
        assertThat(expected).hasSize(3);
        assertThat(onTrino().executeQuery("SELECT * FROM %s FOR VERSION AS OF %d".formatted(tableName, latestSnapshotId))).containsOnly(expected);

        onCompatibilityTestServer().executeQuery("DROP TABLE %s".formatted(tableName));
    }
}
