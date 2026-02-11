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

import com.google.common.collect.Iterables;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static io.trino.plugin.iceberg.TableType.ALL_ENTRIES;
import static io.trino.plugin.iceberg.TableType.ALL_MANIFESTS;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TableType.ENTRIES;
import static io.trino.plugin.iceberg.TableType.FILES;
import static io.trino.plugin.iceberg.TableType.HISTORY;
import static io.trino.plugin.iceberg.TableType.MANIFESTS;
import static io.trino.plugin.iceberg.TableType.MATERIALIZED_VIEW_STORAGE;
import static io.trino.plugin.iceberg.TableType.METADATA_LOG_ENTRIES;
import static io.trino.plugin.iceberg.TableType.PARTITIONS;
import static io.trino.plugin.iceberg.TableType.PROPERTIES;
import static io.trino.plugin.iceberg.TableType.REFS;
import static io.trino.plugin.iceberg.TableType.SNAPSHOTS;
import static io.trino.plugin.lakehouse.TableType.ICEBERG;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
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
    void testSelectMetadataTable()
    {
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$history\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$metadata_log_entries\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$snapshots\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$all_manifests\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$manifests\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$partitions\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$files\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$all_entries\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$entries\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$properties\"")).matches("VALUES (CAST(6 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$refs\"")).matches("VALUES (CAST(1 AS BIGINT))");

        // This test should get updated if a new system table is added
        assertThat(io.trino.plugin.iceberg.TableType.values())
                .containsExactly(
                        DATA,
                        HISTORY,
                        METADATA_LOG_ENTRIES,
                        SNAPSHOTS,
                        ALL_MANIFESTS,
                        MANIFESTS,
                        PARTITIONS,
                        FILES,
                        ALL_ENTRIES,
                        ENTRIES,
                        PROPERTIES,
                        REFS,
                        MATERIALIZED_VIEW_STORAGE);

        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$timeline\""))
                .failure().hasMessageMatching(".* Table .* does not exist");
    }

    @Test
    void testTableChangesFunction()
    {
        DateTimeFormatter instantMillisFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSVV").withZone(UTC);

        try (TestTable table = newTrinoTable(
                "test_table_changes_function_",
                "AS SELECT nationkey, name FROM tpch.tiny.nation WITH NO DATA")) {
            long initialSnapshot = getMostRecentSnapshotId(table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, name FROM nation", 25);
            long snapshotAfterInsert = getMostRecentSnapshotId(table.getName());
            String snapshotAfterInsertTime = getSnapshotTime(table.getName(), snapshotAfterInsert).format(instantMillisFormatter);

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterInsert),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation".formatted(snapshotAfterInsert, snapshotAfterInsertTime));

            // Run with named arguments
            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(schema_name => CURRENT_SCHEMA, table_name => '%s', start_snapshot_id => %s, end_snapshot_id => %s))"
                                    .formatted(table.getName(), initialSnapshot, snapshotAfterInsert),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation".formatted(snapshotAfterInsert, snapshotAfterInsertTime));

            assertUpdate("DELETE FROM " + table.getName(), 25);
            long snapshotAfterDelete = getMostRecentSnapshotId(table.getName());
            String snapshotAfterDeleteTime = getSnapshotTime(table.getName(), snapshotAfterDelete).format(instantMillisFormatter);

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), snapshotAfterInsert, snapshotAfterDelete),
                    "SELECT nationkey, name, 'delete', %s, '%s', 0 FROM nation".formatted(snapshotAfterDelete, snapshotAfterDeleteTime));

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))".formatted(table.getName(), initialSnapshot, snapshotAfterDelete),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation UNION SELECT nationkey, name, 'delete', %s, '%s', 1 FROM nation".formatted(
                            snapshotAfterInsert, snapshotAfterInsertTime, snapshotAfterDelete, snapshotAfterDeleteTime));
        }
    }

    private long getMostRecentSnapshotId(String tableName)
    {
        return (long) Iterables.getOnlyElement(getQueryRunner().execute(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyColumnAsSet());
    }

    private ZonedDateTime getSnapshotTime(String tableName, long snapshotId)
    {
        return (ZonedDateTime) Iterables.getOnlyElement(getQueryRunner().execute(format("SELECT committed_at FROM \"%s$snapshots\" WHERE snapshot_id = %s", tableName, snapshotId))
                .getOnlyColumnAsSet());
    }

    @Test
    void testTableChangesFunctionFailures()
    {
        try (TestTable table = newTrinoTable(
                "test_table_changes_function_",
                "AS SELECT nationkey, name FROM tpch.tiny.nation WITH NO DATA")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey, name FROM nation", 25);

            assertThat(query("SELECT * FROM TABLE(system.table_changes())"))
                    .failure().hasMessageMatching("line 1:21: Missing argument: SCHEMA_NAME");

            assertThat(query("SELECT * FROM TABLE(system.table_changes(NOSCHEMA))"))
                    .failure().hasMessageMatching("line 1:42: Column 'noschema' cannot be resolved");

            assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA))"))
                    .failure().hasMessageMatching("line 1:42: Missing argument: TABLE_NAME");

            assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + table.getName() + "'))"))
                    .failure().hasMessageMatching("table_changes arguments may not be null");

            assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + table.getName() + "', 'not-a-number', null, null))"))
                    .failure().hasMessage("line 1:100: Cannot cast type varchar(12) to bigint");

            assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + table.getName() + "', null, 'not-a-number', null))"))
                    .failure().hasMessage("line 1:106: Cannot cast type varchar(12) to bigint");

            assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + table.getName() + "', null, null, 'not-a-number'))"))
                    .failure().hasMessage("line 1:112: Cannot cast type varchar(12) to bigint");

            assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + table.getName() + "', 100))"))
                    .failure().hasMessageMatching("table_changes arguments may not be null");

            assertThat(query("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, '" + table.getName() + "', 100, 200))"))
                    .failure().hasMessageMatching("Snapshot not found in Iceberg table history: 100");
        }
    }
}
