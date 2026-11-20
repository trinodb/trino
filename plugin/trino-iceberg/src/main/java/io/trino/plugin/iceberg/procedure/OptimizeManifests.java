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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.CommitMetricsReporter;
import io.trino.spi.TrinoException;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReporters;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.trino.plugin.iceberg.IcebergMaterializedViewSummary.carryForwardMaterializedViewDependencies;
import static io.trino.plugin.iceberg.IcebergUtil.loadDataManifestsFromSnapshot;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNullElse;
import static org.apache.iceberg.IcebergManifestUtils.liveEntries;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;

public final class OptimizeManifests
{
    private static final Logger log = Logger.get(OptimizeManifests.class);

    private OptimizeManifests() {}

    public static Map<String, Long> optimizeManifests(BaseTable table, ExecutorService icebergScanExecutor)
    {
        // org.apache.iceberg.BaseRewriteManifests currently rewrites only data manifests
        Snapshot snapshot = table.currentSnapshot();
        List<ManifestFile> manifests = loadDataManifestsFromSnapshot(table, snapshot);
        if (manifests.isEmpty()) {
            return ImmutableMap.of();
        }
        long manifestTargetSizeBytes = table.operations().current().propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        if (manifests.size() == 1 && manifests.getFirst().length() < manifestTargetSizeBytes) {
            return ImmutableMap.of();
        }
        long totalManifestsSize = manifests.stream().mapToLong(ManifestFile::length).sum();
        // Having too many open manifest writers can potentially cause OOM on the coordinator
        int targetManifestClusters = toIntExact(Math.min(((totalManifestsSize + manifestTargetSizeBytes - 1) / manifestTargetSizeBytes), 200));

        // Cluster manifests by partitioning fields for better manifest pruning when reading data files with filters
        Map<StructLike, Integer> clusteredPartitions;
        if (table.spec().isPartitioned() && targetManifestClusters > 1) {
            clusteredPartitions = clusterPartitions(table, manifests, targetManifestClusters, icebergScanExecutor);
        }
        else {
            clusteredPartitions = ImmutableMap.of();
        }

        Types.StructType partitionType = table.spec().partitionType();
        Map<Integer, PartitionSpec> specs = table.specs();
        CommitMetricsReporter reporter = new CommitMetricsReporter();
        table = new BaseTable(table.operations(), table.name(), MetricsReporters.combine(table.reporter(), reporter));
        RewriteManifests rewriteManifests = table.rewriteManifests();
        // commit.manifest.target-size-bytes is enforced by RewriteManifests,
        // so we don't have to worry about any manifest file getting too big
        // because of the clustering logic
        rewriteManifests
                .clusterBy(dataFile -> {
                    if (clusteredPartitions.isEmpty()) {
                        return 0;
                    }
                    return clusteredPartitions.getOrDefault(partitionTuple(dataFile, partitionType, specs), 0);
                })
                .scanManifestsWith(icebergScanExecutor);
        carryForwardMaterializedViewDependencies(rewriteManifests);
        try {
            rewriteManifests.commit();
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(NOT_SUPPORTED, "Failed to optimize manifests: " + requireNonNullElse(e.getMessage(), e), e);
        }

        CommitReport report = reporter.commitReport();
        if (report == null) {
            return ImmutableMap.of();
        }
        log.info("optimize_manifests on table %s, commit report %s", table.name(), report);

        CommitMetricsResult metrics = report.commitMetrics();
        if (metrics == null) {
            return ImmutableMap.of();
        }
        return ImmutableMap.of(
                "rewritten_manifests_count", metrics.manifestsReplaced().value(),
                "added_manifests_count", metrics.manifestsCreated().value(),
                "kept_manifests_count", metrics.manifestsKept().value(),
                "processed_manifest_entries_count", metrics.manifestEntriesProcessed().value());
    }

    private static Map<StructLike, Integer> clusterPartitions(
            BaseTable icebergTable,
            List<ManifestFile> dataManifests,
            int targetClusters,
            ExecutorService icebergScanExecutor)
    {
        Types.StructType partitionType = icebergTable.spec().partitionType();
        Map<Integer, PartitionSpec> specs = icebergTable.specs();
        Iterable<CloseableIterable<ContentFile<DataFile>>> dataFileIterables = Iterables.transform(
                dataManifests,
                manifestFile ->
                        CloseableIterable.transform(
                                liveEntries(
                                        ManifestFiles.read(manifestFile, icebergTable.io(), icebergTable.specs()).select(ImmutableList.of("partition"))),
                                ContentFile::copyWithoutStats));
        Set<StructLike> uniquePartitions = StructLikeSet.create(partitionType);
        try (CloseableIterable<ContentFile<DataFile>> dataFiles = new ParallelIterable<>(dataFileIterables, icebergScanExecutor)) {
            dataFiles.forEach(dataFile -> uniquePartitions.add(partitionTuple(dataFile, partitionType, specs)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        StructLike[] sortedPartitions = uniquePartitions.toArray(new StructLike[uniquePartitions.size()]);
        ObjectArrays.quickSort(sortedPartitions, Comparators.forType(partitionType));

        // Keep adjacent partition tuples together while bounding the number of clusters.
        Map<StructLike, Integer> partitionToBucket = StructLikeMap.create(partitionType);
        for (int i = 0; i < sortedPartitions.length; i++) {
            int bucket = toIntExact((long) i * targetClusters / sortedPartitions.length);
            partitionToBucket.put(sortedPartitions[i], bucket);
        }

        return partitionToBucket;
    }

    private static StructLike partitionTuple(ContentFile<DataFile> dataFile, Types.StructType partitionType, Map<Integer, PartitionSpec> specs)
    {
        PartitionSpec fileSpec = specs.get(dataFile.specId());
        if (fileSpec.partitionType().equals(partitionType)) {
            return dataFile.partition();
        }
        return PartitionUtil.coercePartition(partitionType, fileSpec, dataFile.partition());
    }
}
