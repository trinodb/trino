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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Iceberg format version compatibility with time travel.
 * <p>
 * Ported from the Tempto-based TestIcebergFormatVersionCompatibility.
 * <p>
 * This test runs DDL/DML on an older compatibility Trino and validates reads on current Trino.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergCompatibilityEnvironment.class)
@TestGroup.Iceberg
@TestGroup.IcebergFormatVersionCompatibility
class TestIcebergFormatVersionCompatibility
{
    @Test
    void testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino(SparkIcebergCompatibilityEnvironment env)
    {
        String baseTableName = "test_trino_time_travel_read_table_created_by_early_version_trino_" + randomNameSuffix();
        String tableName = "iceberg.default." + baseTableName;
        String snapshotsTableName = "iceberg.default.\"" + baseTableName + "$snapshots\"";

        try {
            env.executeCompatibilityTrinoUpdate("CREATE TABLE " + tableName + " (c VARCHAR)");
            env.executeCompatibilityTrinoUpdate("INSERT INTO " + tableName + " VALUES 'a', 'b', 'c'");

            long latestSnapshotId = (long) env.executeCompatibilityTrino(
                    "SELECT snapshot_id FROM " + snapshotsTableName + " ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES")
                    .getOnlyValue();

            assertThat(env.executeTrino(
                    "SELECT snapshot_id FROM " + snapshotsTableName + " ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES"))
                    .containsOnly(row(latestSnapshotId));

            QueryResult result = env.executeCompatibilityTrino("SELECT * FROM " + tableName);
            List<Row> expected = result.getRows().stream()
                    .map(r -> row(r.getValues().toArray()))
                    .collect(toImmutableList());

            assertThat(expected).hasSize(3);
            assertThat(env.executeTrino("SELECT * FROM " + tableName + " FOR VERSION AS OF " + latestSnapshotId))
                    .containsOnly(expected.toArray(new Row[0]));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
