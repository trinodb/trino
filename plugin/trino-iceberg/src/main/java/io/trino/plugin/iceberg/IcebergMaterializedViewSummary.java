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
import com.google.common.collect.Iterables;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotAncestryValidator;
import org.apache.iceberg.SnapshotUpdate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * Carries forward the materialized view dependency summary properties onto the given snapshot update.
     * Maintenance operations that commit a new snapshot on a materialized view storage table (OPTIMIZE,
     * optimize_manifests) would otherwise drop these properties, which would break freshness computation and
     * demote the next incremental refresh to a full refresh. This is a no-op for ordinary tables, which do not
     * carry these properties.
     * <p>
     * The properties are copied from the commit's actual parent snapshot at validation time, once table metadata
     * has been refreshed, rather than from whatever snapshot the operation originally scanned. That way a
     * concurrent commit that lands between the scan and this commit's own compare-and-swap, such as a
     * {@code REFRESH MATERIALIZED VIEW}, cannot cause a stale summary to be carried onto the new snapshot: the
     * validator reruns on every commit retry and always sees the parent the commit is about to attach to.
     */
    public static void carryForwardMaterializedViewDependencies(SnapshotUpdate<?> snapshotUpdate)
    {
        snapshotUpdate.validateWith(new DependencySummaryAncestryValidator(snapshotUpdate));
    }

    private static final class DependencySummaryAncestryValidator
            implements SnapshotAncestryValidator
    {
        private final SnapshotUpdate<?> snapshotUpdate;
        // Properties set by a previous validation attempt (a previous commit retry). SnapshotUpdate.set cannot
        // unset a property, so if the parent snapshot seen on a later attempt no longer carries one of these,
        // committing would resurrect the stale value instead of dropping it.
        private final Set<String> propertiesSetSoFar = new HashSet<>();

        private DependencySummaryAncestryValidator(SnapshotUpdate<?> snapshotUpdate)
        {
            this.snapshotUpdate = snapshotUpdate;
        }

        @Override
        public boolean validate(Iterable<Snapshot> baseSnapshots)
        {
            // The first element of the ancestry is the parent this commit is attaching to; empty ancestry means
            // there is no parent, i.e. this is the table's first snapshot.
            Snapshot parent = Iterables.getFirst(baseSnapshots, null);
            Map<String, String> summary = parent == null ? Map.of() : parent.summary();
            for (String key : DEPENDENCY_SUMMARY_PROPERTIES) {
                String value = summary.get(key);
                if (value != null) {
                    snapshotUpdate.set(key, value);
                    propertiesSetSoFar.add(key);
                }
                else if (propertiesSetSoFar.contains(key)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String errorMessage()
        {
            return "Materialized view dependency metadata changed concurrently";
        }
    }
}
