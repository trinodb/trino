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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;

public final class IcebergMaterializedViewSummary
{
    // Snapshot summary properties tracking the tables/functions a materialized view depends on, and its snapshot ids.
    public static final String DEPENDS_ON_TABLES = "dependsOnTables";
    public static final String DEPENDS_ON_TABLE_FUNCTIONS = "dependsOnTableFunctions";
    public static final String DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS = "dependsOnNonDeterministicFunctions";
    // Value should be ISO-8601 formatted time instant
    public static final String TRINO_QUERY_START_TIME = "trino-query-start-time";

    private static final List<String> DEPENDENCY_SUMMARY_PROPERTIES = ImmutableList.of(
            DEPENDS_ON_TABLES,
            DEPENDS_ON_TABLE_FUNCTIONS,
            DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS,
            TRINO_QUERY_START_TIME);

    private IcebergMaterializedViewSummary() {}

    /**
     * Carries forward the materialized view dependency summary properties from the table's current snapshot onto the
     * given snapshot update. Maintenance operations that commit a new snapshot on a materialized view storage table
     * (OPTIMIZE, optimize_manifests) would otherwise drop these properties, which would
     * break freshness computation and demote the next incremental refresh to a full refresh. This is a no-op for
     * ordinary tables, which do not carry these properties.
     */
    public static void carryForwardMaterializedViewDependencies(Table table, SnapshotUpdate<?> snapshotUpdate)
    {
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            return;
        }
        Map<String, String> summary = currentSnapshot.summary();
        for (String key : DEPENDENCY_SUMMARY_PROPERTIES) {
            String value = summary.get(key);
            if (value != null) {
                snapshotUpdate.set(key, value);
            }
        }
    }
}
