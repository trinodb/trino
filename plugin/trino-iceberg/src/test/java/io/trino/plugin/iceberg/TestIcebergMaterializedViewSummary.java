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

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotAncestryValidator;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static io.trino.plugin.iceberg.IcebergMaterializedViewSummary.DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS;
import static io.trino.plugin.iceberg.IcebergMaterializedViewSummary.DEPENDS_ON_TABLES;
import static io.trino.plugin.iceberg.IcebergMaterializedViewSummary.DEPENDS_ON_TABLE_FUNCTIONS;
import static io.trino.plugin.iceberg.IcebergMaterializedViewSummary.TRINO_QUERY_START_TIME;
import static io.trino.plugin.iceberg.IcebergMaterializedViewSummary.carryForwardMaterializedViewDependencies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergMaterializedViewSummary
{
    @Test
    public void testAllDependencyPropertiesAreCopiedFromParent()
    {
        FakeSnapshotUpdate snapshotUpdate = new FakeSnapshotUpdate();
        carryForwardMaterializedViewDependencies(snapshotUpdate);

        Map<String, String> parentSummary = ImmutableMap.of(
                DEPENDS_ON_TABLES, "catalog.schema.table=1",
                DEPENDS_ON_TABLE_FUNCTIONS, "false",
                DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS, "false",
                TRINO_QUERY_START_TIME, "2026-01-01T00:00:00Z");

        assertThat(snapshotUpdate.validateAncestry(List.of(new FakeSnapshot(parentSummary)))).isTrue();
        assertThat(snapshotUpdate.properties()).containsExactlyInAnyOrderEntriesOf(parentSummary);
    }

    @Test
    public void testOnlyPropertiesPresentOnParentAreCopied()
    {
        FakeSnapshotUpdate snapshotUpdate = new FakeSnapshotUpdate();
        carryForwardMaterializedViewDependencies(snapshotUpdate);

        Map<String, String> parentSummary = ImmutableMap.of(DEPENDS_ON_TABLES, "catalog.schema.table=1");

        assertThat(snapshotUpdate.validateAncestry(List.of(new FakeSnapshot(parentSummary)))).isTrue();
        assertThat(snapshotUpdate.properties()).containsExactlyInAnyOrderEntriesOf(parentSummary);
    }

    @Test
    public void testParentWithNoDependencyPropertiesIsANoOp()
    {
        FakeSnapshotUpdate snapshotUpdate = new FakeSnapshotUpdate();
        carryForwardMaterializedViewDependencies(snapshotUpdate);

        assertThat(snapshotUpdate.validateAncestry(List.of(new FakeSnapshot(ImmutableMap.of())))).isTrue();
        assertThat(snapshotUpdate.properties()).isEmpty();
    }

    @Test
    public void testEmptyAncestryIsANoOp()
    {
        // Empty ancestry means there is no parent snapshot, i.e. this is the table's first snapshot.
        FakeSnapshotUpdate snapshotUpdate = new FakeSnapshotUpdate();
        carryForwardMaterializedViewDependencies(snapshotUpdate);

        assertThat(snapshotUpdate.validateAncestry(List.of())).isTrue();
        assertThat(snapshotUpdate.properties()).isEmpty();
    }

    @Test
    public void testCommitIsRejectedWhenRetryParentLostAPreviouslySetProperty()
    {
        // Simulates a commit retry: the validator runs once per attempt, and set() cannot unset a property
        // that an earlier attempt already applied. If the parent seen on a later attempt no longer carries a
        // property that was copied from an earlier attempt's parent, the commit must be rejected rather than
        // resurrecting the stale value.
        FakeSnapshotUpdate snapshotUpdate = new FakeSnapshotUpdate();
        carryForwardMaterializedViewDependencies(snapshotUpdate);

        Snapshot firstAttemptParent = new FakeSnapshot(ImmutableMap.of(DEPENDS_ON_TABLES, "catalog.schema.table=1"));
        assertThat(snapshotUpdate.validateAncestry(List.of(firstAttemptParent))).isTrue();
        assertThat(snapshotUpdate.properties()).containsEntry(DEPENDS_ON_TABLES, "catalog.schema.table=1");

        Snapshot retryParent = new FakeSnapshot(ImmutableMap.of());
        assertThat(snapshotUpdate.validateAncestry(List.of(retryParent))).isFalse();
    }

    private static final class FakeSnapshotUpdate
            implements SnapshotUpdate<FakeSnapshotUpdate>
    {
        private final Map<String, String> properties = new HashMap<>();
        private SnapshotAncestryValidator validator = SnapshotAncestryValidator.NON_VALIDATING;

        Map<String, String> properties()
        {
            return properties;
        }

        boolean validateAncestry(List<Snapshot> ancestry)
        {
            return validator.validate(ancestry);
        }

        @Override
        public FakeSnapshotUpdate set(String property, String value)
        {
            properties.put(property, value);
            return this;
        }

        @Override
        public FakeSnapshotUpdate deleteWith(Consumer<String> deleteFunc)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FakeSnapshotUpdate stageOnly()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FakeSnapshotUpdate scanManifestsWith(ExecutorService executorService)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FakeSnapshotUpdate validateWith(SnapshotAncestryValidator validator)
        {
            this.validator = validator;
            return this;
        }

        @Override
        public Snapshot apply()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commit()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FakeSnapshot
            implements Snapshot
    {
        private final Map<String, String> summary;

        FakeSnapshot(Map<String, String> summary)
        {
            this.summary = summary;
        }

        @Override
        public Map<String, String> summary()
        {
            return summary;
        }

        @Override
        public long sequenceNumber()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long snapshotId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long parentId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long timestampMillis()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ManifestFile> allManifests(FileIO io)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ManifestFile> dataManifests(FileIO io)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ManifestFile> deleteManifests(FileIO io)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String operation()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<DataFile> addedDataFiles(FileIO io)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<DataFile> removedDataFiles(FileIO io)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String manifestListLocation()
        {
            throw new UnsupportedOperationException();
        }
    }
}
