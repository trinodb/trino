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
package io.trino.plugin.iceberg.system.entries;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.plugin.iceberg.system.files.TrinoManifestFile;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.iceberg.IcebergManifestUtils;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.ParallelIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;

/**
 * Emits one split per manifest for {@code $entries}/{@code $all_entries}.
 * <p>
 * {@code ENTRIES} enumerates the current snapshot's manifests. {@code ALL_ENTRIES} enumerates the manifests of every
 * snapshot and deduplicates by path - but it does so by <em>streaming</em>: manifest lists are read concurrently
 * through a bounded {@link org.apache.iceberg.util.ParallelIterable} and each {@link ManifestFile} is dropped as soon
 * as it has been deduplicated and turned into a split. This avoids Iceberg's
 * {@code BaseAllMetadataTableScan.reachableManifests}, which materializes the union of every snapshot's manifests into
 * a single {@code HashSet} on the coordinator (O(snapshots^2) live {@code ManifestFile} objects -> OOM). Here the live
 * footprint is the {@code seen} path set plus the bounded in-flight window, and the heavy per-manifest entry reads are
 * distributed across workers.
 * <p>
 * Crucially the per-snapshot manifest lists are read via {@link IcebergManifestUtils#read} rather than
 * {@code Snapshot.allManifests()}: the latter memoizes the list inside the (retained) {@code BaseSnapshot}, which is the
 * actual source of the quadratic retention.
 */
public final class EntriesTableSplitSource
        implements ConnectorSplitSource
{
    private final Table icebergTable;
    private final MetadataTableType metadataTableType;
    private final ExecutorService executor;
    private final String schemaJson;
    private final String metadataSchemaJson;
    private final Map<Integer, String> partitionSpecsByIdJson;

    private final Set<String> seenManifestPaths = new HashSet<>();
    private FileIO fileIO;
    private CloseableIterable<ManifestFile> manifests;
    private CloseableIterator<ManifestFile> manifestIterator;
    private boolean started;
    private boolean finished;

    public EntriesTableSplitSource(
            Table icebergTable,
            MetadataTableType metadataTableType,
            ExecutorService executor,
            String schemaJson,
            String metadataSchemaJson,
            Map<Integer, String> partitionSpecsByIdJson)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.metadataTableType = requireNonNull(metadataTableType, "metadataTableType is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.schemaJson = requireNonNull(schemaJson, "schemaJson is null");
        this.metadataSchemaJson = requireNonNull(metadataSchemaJson, "metadataSchemaJson is null");
        this.partitionSpecsByIdJson = ImmutableMap.copyOf(partitionSpecsByIdJson);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        ensureStarted();

        List<ConnectorSplit> splits = new ArrayList<>();
        while (splits.size() < maxSize && manifestIterator.hasNext()) {
            ManifestFile manifest = manifestIterator.next();
            // Dedup by path - the same identity Sets.newHashSet(ManifestFile) uses in reachableManifests, so every
            // distinct manifest is visited exactly once and $all_entries output is preserved. Harmless no-op for ENTRIES.
            if (seenManifestPaths.add(manifest.path())) {
                splits.add(new EntriesTableSplit(
                        TrinoManifestFile.from(manifest),
                        schemaJson,
                        metadataSchemaJson,
                        partitionSpecsByIdJson));
            }
        }

        if (!manifestIterator.hasNext()) {
            finished = true;
        }
        return completedFuture(new ConnectorSplitBatch(splits, finished));
    }

    private void ensureStarted()
    {
        if (started) {
            return;
        }
        started = true;
        fileIO = icebergTable.io();
        manifests = manifestList(fileIO);
        manifestIterator = manifests.iterator();
    }

    private CloseableIterable<ManifestFile> manifestList(FileIO io)
    {
        if (metadataTableType == ALL_ENTRIES) {
            // Read every snapshot's manifest list concurrently (bounded by ParallelIterable's internal queue),
            // never holding more than the in-flight window live at once.
            Iterable<Iterable<ManifestFile>> perSnapshotManifests = Iterables.transform(
                    icebergTable.snapshots(),
                    snapshot -> () -> IcebergManifestUtils.read(io, snapshot.manifestListLocation()).iterator());
            return new ParallelIterable<>(perSnapshotManifests, executor);
        }

        Snapshot snapshot = icebergTable.currentSnapshot();
        if (snapshot == null) {
            return CloseableIterable.empty();
        }
        return CloseableIterable.withNoopClose(IcebergManifestUtils.read(io, snapshot.manifestListLocation()));
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close()
    {
        try {
            if (manifestIterator != null) {
                manifestIterator.close();
            }
            if (manifests != null) {
                manifests.close();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            if (fileIO != null) {
                fileIO.close();
            }
        }
    }
}
