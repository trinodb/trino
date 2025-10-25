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
package io.trino.plugin.lakehouse;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.lakehouse.TableType.ICEBERG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLakehouseIcebergConnectorSmokeTest
        extends BaseLakehouseConnectorSmokeTest
{
    protected TestLakehouseIcebergConnectorSmokeTest()
    {
        super(ICEBERG);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.region (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   format = 'PARQUET',
                   format_version = 2,
                   location = \\E's3://test-bucket-.*/tpch/region-.*'\\Q,
                   type = 'ICEBERG'
                )\\E""");
    }

    @Test
    public void testOptimize()
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar)");
        try {
            assertThat(query("ALTER TABLE " + tableName + " EXECUTE optimize(file_size_threshold => '10kB')")).succeeds();

            assertThat(query("ALTER TABLE " + tableName + " EXECUTE optimize_manifests")).succeeds();

            assertThat(query("ALTER TABLE " + tableName + " EXECUTE drop_extended_stats")).succeeds();

            long currentSnapshotId = getCurrentSnapshotId(tableName);
            assertThat(currentSnapshotId).isGreaterThan(0);
            assertThat(query("ALTER TABLE " + tableName + " EXECUTE rollback_to_snapshot(" + currentSnapshotId + ")")).succeeds();

            assertThat(query("ALTER TABLE " + tableName + " EXECUTE expire_snapshots(retention_threshold => '7d')")).succeeds();

            assertThat(query("ALTER TABLE " + tableName + " EXECUTE remove_orphan_files(retention_threshold => '7d')")).succeeds();

            assertThat(query("ALTER TABLE " + tableName + " EXECUTE add_files(" +
                    " location => 's3://my-bucket/a/path'," +
                    " format => 'ORC')"))
                    .failure().hasMessage("Failed to add files: Failed to list location: s3://my-bucket/a/path");

            String tableName2 = "test_optimize2_" + randomNameSuffix();
            assertUpdate("CREATE TABLE " + tableName2 + " (key integer, value varchar)");
            assertThat(query("ALTER TABLE " + tableName + " EXECUTE add_files_from_table(" +
                    " schema_name => CURRENT_SCHEMA," +
                    " table_name => '" + tableName2 + "')"))
                    .failure().hasMessage("Adding files from non-Hive tables is unsupported");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private long getCurrentSnapshotId(String tableName)
    {
        return (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES");
    }
}
