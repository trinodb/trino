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
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReporters;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.JavaHash;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.PartitionUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.IcebergUtil.loadDataManifestsFromSnapshot;
import static java.lang.Math.toIntExact;
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
        List<ManifestFile> manifests = loadDataManifestsFromSnapshot(table, table.currentSnapshot());
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
        Map<Object, Integer> clusteredPartitionValues;
        if (table.spec().isPartitioned() && targetManifestClusters > 1) {
            clusteredPartitionValues = getClusteredPartitionValues(table, manifests, targetManifestClusters, icebergScanExecutor);
        }
        else {
            clusteredPartitionValues = ImmutableMap.of();
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
                    if (clusteredPartitionValues.isEmpty()) {
                        return 0;
                    }
                    StructLike partition = PartitionUtil.coercePartition(partitionType, specs.get(dataFile.specId()), dataFile.partition());
                    Object value = partition.get(0, Object.class);
                    if (value == null) {
                        return 0;
                    }
                    return clusteredPartitionValues.get(value);
                })
                .scanManifestsWith(icebergScanExecutor)
                .commit();

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

    private static Map<Object, Integer> getClusteredPartitionValues(
            BaseTable icebergTable,
            List<ManifestFile> dataManifests,
            int targetClusters,
            ExecutorService icebergScanExecutor)
    {
        checkArgument(icebergTable.spec().isPartitioned(), "Table %s must be partitioned", icebergTable);

        // Clustering is limited to the top-level partitioning column as that is likely
        // to be the most effective for filters during reads
        Type.PrimitiveType firstPartitionFieldType = icebergTable.spec().partitionType()
                .fields().getFirst()
                .type().asPrimitiveType();
        Hash.Strategy<Object> icebergHashStrategy = new IcebergHashStrategy(firstPartitionFieldType);

        Iterable<CloseableIterable<ContentFile<DataFile>>> dataFileIterables = Iterables.transform(
                dataManifests,
                manifestFile ->
                        CloseableIterable.transform(
                                liveEntries(
                                        ManifestFiles.read(manifestFile, icebergTable.io(), icebergTable.specs()).select(ImmutableList.of("partition"))),
                                ContentFile::copyWithoutStats));
        // Collect unique values of top-level partitioning column
        Set<Object> uniqueValues = new ObjectOpenCustomHashSet<>(icebergHashStrategy);
        try (CloseableIterable<ContentFile<DataFile>> dataFiles = new ParallelIterable<>(dataFileIterables, icebergScanExecutor)) {
            Types.StructType partitionType = icebergTable.spec().partitionType();
            Map<Integer, PartitionSpec> specs = icebergTable.specs();
            dataFiles.forEach(dataFile -> {
                StructLike partition = PartitionUtil.coercePartition(partitionType, specs.get(dataFile.specId()), dataFile.partition());
                Object value = partition.get(0, Object.class);
                if (value != null) {
                    uniqueValues.add(value);
                }
            });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (uniqueValues.isEmpty()) {
            return ImmutableMap.of();
        }
        // Sort unique values to cluster nearby partitions together
        Object[] sortedValues = uniqueValues.toArray();
        ObjectArrays.quickSort(sortedValues, Comparators.forType(firstPartitionFieldType));

        // Assign a bucket in range [0, targetClusters) to the partition values, while also grouping
        // adjacent values into same bucket
        Map<Object, Integer> valueToBucket = new Object2IntOpenCustomHashMap<>(icebergHashStrategy);
        for (int i = 0; i < sortedValues.length; i++) {
            int bucket = (i * targetClusters / sortedValues.length);
            valueToBucket.put(sortedValues[i], bucket);
        }

        return valueToBucket;
    }

    /**
     * Bridges Iceberg's JavaHash and Comparator into FastUtil's Hash.Strategy.
     */
    private static final class IcebergHashStrategy
            implements Hash.Strategy<Object>
    {
        private final JavaHash<Object> hasher;
        private final Comparator<Object> comparator;

        IcebergHashStrategy(Type.PrimitiveType type)
        {
            this.hasher = JavaHash.forType(type);
            this.comparator = Comparators.forType(type);
        }

        @Override
        public int hashCode(Object o)
        {
            return o == null ? 0 : hasher.hash(o);
        }

        @Override
        public boolean equals(Object a, Object b)
        {
            if (a == b) {
                return true;
            }
            if (a == null || b == null) {
                return false;
            }
            return comparator.compare(a, b) == 0;
        }
    }
}
