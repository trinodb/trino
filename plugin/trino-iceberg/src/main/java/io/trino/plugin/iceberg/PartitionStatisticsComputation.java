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
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IcebergManifestUtils;
import org.apache.iceberg.IcebergManifestUtils.EntryStatus;
import org.apache.iceberg.IcebergManifestUtils.FileEntryWithStatus;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructProjection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.base.util.ExecutorUtil.processWithAdditionalThreads;
import static io.trino.plugin.iceberg.PartitionStatisticsReader.readRecords;
import static org.apache.iceberg.TableUtil.formatVersion;

/**
 * Computes Iceberg partition statistics. It ports {@code PartitionStatsHandler} of the Iceberg library, so that the
 * rows match the library's value for value, with one difference: statistics are always keyed by the spec id and the
 * partition in that spec's own partition type, so that partition specs that are not a prefix of the unified partition
 * type are not mixed up.
 */
final class PartitionStatisticsComputation
{
    private PartitionStatisticsComputation() {}

    /**
     * Returns the rows of the partition statistics file of a snapshot, sorted by partition, with the schema
     * {@link PartitionStatistics#schema}. The list is empty when the snapshot has no partitions. With a base, the
     * rows are computed from the base file and the manifests added by the snapshots after the base's snapshot, which
     * must be an ancestor of the snapshot.
     *
     * @throws UnreadableBaseException when the base file cannot be read or its snapshot is not an ancestor
     */
    static List<GenericRecord> compute(Table table, long snapshotId, Optional<PartitionStatisticsFile> base, TypeManager typeManager, ExecutorService executor)
    {
        Snapshot snapshot = table.snapshot(snapshotId);
        checkArgument(snapshot != null, "Snapshot not found: %s", snapshotId);
        Types.StructType partitionType = Partitioning.partitionType(table);
        Schema schema = PartitionStatistics.schema(partitionType, formatVersion(table));
        PartitionMap<GenericRecord> statistics;
        List<ManifestFile> manifests;
        if (base.isPresent()) {
            // otherwise the manifests of the whole history, or of the part that is left of it, would be added to the base
            if (!SnapshotUtil.isAncestorOf(snapshotId, base.get().snapshotId(), table::snapshot)) {
                throw new UnreadableBaseException(new IllegalArgumentException("Snapshot %s of the base is not an ancestor of snapshot %s".formatted(base.get().snapshotId(), snapshotId)));
            }
            statistics = readBase(table, base.get(), schema, partitionType, typeManager);
            // DELETED entries are not carried over to later snapshots, so the manifests each snapshot added are read
            manifests = stream(SnapshotUtil.ancestorsBetween(snapshotId, base.get().snapshotId(), table::snapshot))
                    .flatMap(ancestor -> ancestor.allManifests(table.io()).stream()
                            .filter(manifest -> manifest.snapshotId() != null && manifest.snapshotId() == ancestor.snapshotId()))
                    .collect(toImmutableList());
        }
        else {
            statistics = PartitionMap.create(table.specs());
            manifests = snapshot.allManifests(table.io());
        }
        collectStatistics(table, manifests, base.isPresent(), schema, partitionType, executor, statistics);
        return sortByPartition(new ArrayList<>(statistics.values()), partitionType);
    }

    /**
     * Statistics of a partition are keyed by the spec id and the partition in that spec's own partition type, but the
     * base file has the partitions in the unified partition type. The Iceberg library keys them by the unified one,
     * which mixes up the rows of a spec that is not a prefix of the unified partition type.
     */
    private static PartitionMap<GenericRecord> readBase(Table table, PartitionStatisticsFile base, Schema schema, Types.StructType partitionType, TypeManager typeManager)
    {
        PartitionMap<GenericRecord> statistics = PartitionMap.create(table.specs());
        Map<Integer, StructProjection> projections = new HashMap<>();
        // a file written before an upgrade to format version 3 has no dv_count, which reads as null
        Schema readSchema = allOptional(PartitionStatistics.schema(partitionType, 3));
        try (CloseableIterable<StructLike> records = readRecords(table.io().newInputFile(base.path(), base.fileSizeInBytes()), readSchema, typeManager)) {
            for (StructLike record : records) {
                int specId = record.get(PartitionStatistics.SPEC_ID_POSITION, Integer.class);
                PartitionSpec spec = table.specs().get(specId);
                checkArgument(spec != null, "Unknown partition spec: %s", specId);
                GenericRecord partition = copy(partitionType, record.get(PartitionStatistics.PARTITION_POSITION, StructLike.class));
                StructLike key = projections.computeIfAbsent(specId, _ -> StructProjection.create(partitionType, spec.partitionType())).copyFor(partition);
                // the last row of a partition wins
                statistics.put(specId, key, fromBase(schema, record, partition, specId));
            }
        }
        catch (IOException | RuntimeException e) {
            throw new UnreadableBaseException(e);
        }
        return statistics;
    }

    private static Schema allOptional(Schema schema)
    {
        return new Schema(schema.columns().stream()
                .map(field -> Types.NestedField.optional(field.fieldId(), field.name(), field.type(), field.doc()))
                .collect(toImmutableList()));
    }

    private static GenericRecord fromBase(Schema schema, StructLike record, StructLike partition, int specId)
    {
        GenericRecord statistics = newStatistics(schema, partition, specId);
        for (int position = PartitionStatistics.DATA_RECORD_COUNT_POSITION; position < schema.columns().size(); position++) {
            // total_record_count is not computed, so it is not carried forward
            if (position == PartitionStatistics.TOTAL_RECORD_COUNT_POSITION) {
                continue;
            }
            // counts missing in the base are 0
            Object value = record.get(position, Object.class);
            if (value != null) {
                statistics.set(position, value);
            }
        }
        return statistics;
    }

    private static void collectStatistics(
            Table table,
            List<ManifestFile> manifests,
            boolean incremental,
            Schema schema,
            Types.StructType partitionType,
            ExecutorService executor,
            PartitionMap<GenericRecord> target)
    {
        List<Callable<PartitionMap<GenericRecord>>> tasks = manifests.stream()
                .map(manifest -> (Callable<PartitionMap<GenericRecord>>) () -> collectStatistics(table, manifest, incremental, schema, partitionType))
                .collect(toImmutableList());
        List<PartitionMap<GenericRecord>> partial;
        try {
            partial = processWithAdditionalThreads(tasks, executor);
        }
        catch (ExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
        for (PartitionMap<GenericRecord> statistics : partial) {
            statistics.forEach((key, value) -> target.merge(key, value, (existing, added) -> {
                append(existing, added);
                return existing;
            }));
        }
    }

    private static PartitionMap<GenericRecord> collectStatistics(Table table, ManifestFile manifest, boolean incremental, Schema schema, Types.StructType partitionType)
    {
        int specId = manifest.partitionSpecId();
        PartitionSpec spec = table.specs().get(specId);
        PartitionMap<GenericRecord> statisticsByPartition = PartitionMap.create(table.specs());
        try (CloseableIterable<FileEntryWithStatus> entries = IcebergManifestUtils.allEntries(
                IcebergUtil.readerForManifest(manifest, table.io(), table.specs())
                        .select(ImmutableList.of("content", "file_format", "partition", "record_count", "file_size_in_bytes")))) {
            for (FileEntryWithStatus entry : entries) {
                ContentFile<?> file = entry.file();
                // the key is in the spec's own partition type, the row's partition in the unified partition type
                GenericRecord statistics = statisticsByPartition.computeIfAbsent(
                        specId,
                        ((PartitionData) file.partition()).copy(),
                        () -> newStatistics(schema, copy(partitionType, PartitionUtil.coercePartition(partitionType, spec, file.partition())), specId));
                // an existing entry is in the base already
                if (incremental && entry.status() == EntryStatus.EXISTING) {
                    continue;
                }
                Snapshot snapshot = entry.snapshotId() == null ? null : table.snapshot(entry.snapshotId());
                if (entry.status() != EntryStatus.DELETED) {
                    addFile(statistics, file, 1);
                }
                else if (incremental) {
                    // the base counts the file
                    addFile(statistics, file, -1);
                }
                if (snapshot != null) {
                    updateSnapshotInfo(statistics, snapshot.snapshotId(), snapshot.timestampMillis());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return statisticsByPartition;
    }

    private static GenericRecord newStatistics(Schema schema, StructLike partition, int specId)
    {
        GenericRecord statistics = GenericRecord.create(schema);
        statistics.set(PartitionStatistics.PARTITION_POSITION, partition);
        statistics.set(PartitionStatistics.SPEC_ID_POSITION, specId);
        statistics.set(PartitionStatistics.DATA_RECORD_COUNT_POSITION, 0L);
        statistics.set(PartitionStatistics.DATA_FILE_COUNT_POSITION, 0);
        statistics.set(PartitionStatistics.TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, 0L);
        statistics.set(PartitionStatistics.POSITION_DELETE_RECORD_COUNT_POSITION, 0L);
        statistics.set(PartitionStatistics.POSITION_DELETE_FILE_COUNT_POSITION, 0);
        statistics.set(PartitionStatistics.EQUALITY_DELETE_RECORD_COUNT_POSITION, 0L);
        statistics.set(PartitionStatistics.EQUALITY_DELETE_FILE_COUNT_POSITION, 0);
        if (hasDvCount(statistics)) {
            statistics.set(PartitionStatistics.DV_COUNT_POSITION, 0);
        }
        return statistics;
    }

    // copied, because the manifest reader reuses its containers
    private static GenericRecord copy(Types.StructType partitionType, StructLike partition)
    {
        GenericRecord copy = GenericRecord.create(partitionType);
        for (int i = 0; i < partitionType.fields().size(); i++) {
            copy.set(i, partition.get(i, Object.class));
        }
        return copy;
    }

    private static void addFile(GenericRecord statistics, ContentFile<?> file, int sign)
    {
        switch (file.content()) {
            case DATA -> {
                addLong(statistics, PartitionStatistics.DATA_RECORD_COUNT_POSITION, sign * file.recordCount());
                addInt(statistics, PartitionStatistics.DATA_FILE_COUNT_POSITION, sign);
                addLong(statistics, PartitionStatistics.TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, sign * file.fileSizeInBytes());
            }
            case POSITION_DELETES -> {
                addLong(statistics, PartitionStatistics.POSITION_DELETE_RECORD_COUNT_POSITION, sign * file.recordCount());
                if (file.format() == FileFormat.PUFFIN) {
                    addInt(statistics, PartitionStatistics.DV_COUNT_POSITION, sign);
                }
                else {
                    addInt(statistics, PartitionStatistics.POSITION_DELETE_FILE_COUNT_POSITION, sign);
                }
            }
            case EQUALITY_DELETES -> {
                addLong(statistics, PartitionStatistics.EQUALITY_DELETE_RECORD_COUNT_POSITION, sign * file.recordCount());
                addInt(statistics, PartitionStatistics.EQUALITY_DELETE_FILE_COUNT_POSITION, sign);
            }
            default -> throw new UnsupportedOperationException("Unsupported file content type: " + file.content());
        }
    }

    // The statistics of one partition found in different manifests, or of the base file and of new manifests
    private static void append(GenericRecord target, GenericRecord added)
    {
        addLong(target, PartitionStatistics.DATA_RECORD_COUNT_POSITION, added.get(PartitionStatistics.DATA_RECORD_COUNT_POSITION, Long.class));
        addInt(target, PartitionStatistics.DATA_FILE_COUNT_POSITION, added.get(PartitionStatistics.DATA_FILE_COUNT_POSITION, Integer.class));
        addLong(target, PartitionStatistics.TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, added.get(PartitionStatistics.TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, Long.class));
        addLong(target, PartitionStatistics.POSITION_DELETE_RECORD_COUNT_POSITION, added.get(PartitionStatistics.POSITION_DELETE_RECORD_COUNT_POSITION, Long.class));
        addInt(target, PartitionStatistics.POSITION_DELETE_FILE_COUNT_POSITION, added.get(PartitionStatistics.POSITION_DELETE_FILE_COUNT_POSITION, Integer.class));
        addLong(target, PartitionStatistics.EQUALITY_DELETE_RECORD_COUNT_POSITION, added.get(PartitionStatistics.EQUALITY_DELETE_RECORD_COUNT_POSITION, Long.class));
        addInt(target, PartitionStatistics.EQUALITY_DELETE_FILE_COUNT_POSITION, added.get(PartitionStatistics.EQUALITY_DELETE_FILE_COUNT_POSITION, Integer.class));
        // a statistics file written before an upgrade to format version 3 has no deletion vector count
        if (hasDvCount(added) && hasDvCount(target)) {
            Integer dvCount = added.get(PartitionStatistics.DV_COUNT_POSITION, Integer.class);
            if (dvCount != null) {
                Integer existing = target.get(PartitionStatistics.DV_COUNT_POSITION, Integer.class);
                target.set(PartitionStatistics.DV_COUNT_POSITION, existing == null ? dvCount : existing + dvCount);
            }
        }
        Long lastUpdatedAt = added.get(PartitionStatistics.LAST_UPDATED_AT_POSITION, Long.class);
        if (lastUpdatedAt != null) {
            updateSnapshotInfo(target, added.get(PartitionStatistics.LAST_UPDATED_SNAPSHOT_ID_POSITION, Long.class), lastUpdatedAt);
        }
    }

    private static boolean hasDvCount(GenericRecord statistics)
    {
        return statistics.size() > PartitionStatistics.DV_COUNT_POSITION;
    }

    private static void updateSnapshotInfo(GenericRecord statistics, long snapshotId, long updatedAt)
    {
        Long lastUpdatedAt = statistics.get(PartitionStatistics.LAST_UPDATED_AT_POSITION, Long.class);
        if (lastUpdatedAt == null || lastUpdatedAt < updatedAt) {
            statistics.set(PartitionStatistics.LAST_UPDATED_AT_POSITION, updatedAt);
            statistics.set(PartitionStatistics.LAST_UPDATED_SNAPSHOT_ID_POSITION, snapshotId);
        }
    }

    private static void addLong(GenericRecord statistics, int position, long value)
    {
        statistics.set(position, statistics.get(position, Long.class) + value);
    }

    private static void addInt(GenericRecord statistics, int position, int value)
    {
        statistics.set(position, statistics.get(position, Integer.class) + value);
    }

    private static List<GenericRecord> sortByPartition(List<GenericRecord> statistics, Types.StructType partitionType)
    {
        Comparator<StructLike> comparator = Comparators.forType(partitionType);
        statistics.sort(Comparator.comparing(row -> row.get(PartitionStatistics.PARTITION_POSITION, StructLike.class), comparator));
        return statistics;
    }

    /**
     * Thrown when the base statistics file cannot be used, because it is missing, corrupt, has an unexpected schema or
     * belongs to a snapshot that is not an ancestor.
     */
    static final class UnreadableBaseException
            extends RuntimeException
    {
        UnreadableBaseException(Throwable cause)
        {
            super(cause);
        }
    }
}
