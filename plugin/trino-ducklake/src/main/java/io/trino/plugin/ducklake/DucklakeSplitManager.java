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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeDataFile;
import io.trino.plugin.ducklake.catalog.DucklakeFilePartitionValue;
import io.trino.plugin.ducklake.catalog.DucklakeInlinedDataInfo;
import io.trino.plugin.ducklake.catalog.DucklakePartitionField;
import io.trino.plugin.ducklake.catalog.DucklakePartitionSpec;
import io.trino.plugin.ducklake.catalog.DucklakePartitionTransform;
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

/**
 * Split manager for Ducklake connector.
 * Discovers data files from SQL catalog and creates splits for each Parquet file.
 */
public class DucklakeSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(DucklakeSplitManager.class);

    private final DucklakeCatalog catalog;
    private final DucklakeConfig config;

    @Inject
    public DucklakeSplitManager(DucklakeCatalog catalog, DucklakeConfig config)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DucklakeTableHandle tableHandle = (DucklakeTableHandle) table;

        log.debug("Getting splits for table %s at snapshot %d", tableHandle.tableName(), tableHandle.snapshotId());

        // Get all data files for this table at the snapshot
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(
                tableHandle.tableId(),
                tableHandle.snapshotId());

        log.debug("Found %d data files for table %s", dataFiles.size(), tableHandle.tableName());

        // If no data files exist at all, check for inlined data in the metadata catalog.
        // Only check when the raw (unpruned) file list is empty — this means the table
        // genuinely has no Parquet files, not that pruning eliminated them.
        if (dataFiles.isEmpty()) {
            Optional<DucklakeInlinedDataInfo> inlinedInfo = catalog.getInlinedDataInfo(
                    tableHandle.tableId(), tableHandle.snapshotId());
            if (inlinedInfo.isPresent()) {
                DucklakeInlinedDataInfo info = inlinedInfo.get();
                log.debug("Found inlined data for table %s (tableId=%d, schemaVersion=%d)",
                        tableHandle.tableName(), info.tableId(), info.schemaVersion());
                return new FixedSplitSource(List.of(new DucklakeInlinedSplit(
                        info.tableId(), info.schemaVersion(), tableHandle.snapshotId())));
            }
        }

        DucklakeTable tableMetadata = catalog.getTableById(tableHandle.tableId(), tableHandle.snapshotId())
                .orElseThrow(() -> new IllegalStateException("Table metadata missing for table ID: " + tableHandle.tableId()));
        DucklakeSchema schemaMetadata = catalog.getSchema(tableHandle.schemaName(), tableHandle.snapshotId())
                .orElseThrow(() -> new IllegalStateException("Schema metadata missing for schema: " + tableHandle.schemaName()));
        String tableDataPath = resolveTableDataPath(schemaMetadata, tableMetadata);

        TupleDomain<DucklakeColumnHandle> fileStatisticsDomain = buildFileStatisticsDomain(constraint)
                .intersect(tableHandle.unenforcedPredicate());
        dataFiles = pruneDataFiles(dataFiles, tableHandle, constraint);
        dataFiles = pruneByPartitionValues(dataFiles, tableHandle);

        // Convert data files to splits
        List<DucklakeSplit> splits = dataFiles.stream()
                .map(dataFile -> createSplit(dataFile, tableDataPath, fileStatisticsDomain))
                .collect(toImmutableList());

        log.debug("Created %d splits for table %s", splits.size(), tableHandle.tableName());

        return new FixedSplitSource(splits);
    }

    private List<DucklakeDataFile> pruneDataFiles(List<DucklakeDataFile> dataFiles, DucklakeTableHandle tableHandle, Constraint constraint)
    {
        if (dataFiles.isEmpty()) {
            return dataFiles;
        }

        if (constraint == null || constraint.getSummary().isAll()) {
            return dataFiles;
        }

        if (constraint.getSummary().isNone()) {
            return List.of();
        }

        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        if (domains.isEmpty() || domains.get().isEmpty()) {
            return dataFiles;
        }

        Set<Long> candidateFileIds = dataFiles.stream()
                .map(DucklakeDataFile::dataFileId)
                .collect(toCollection(LinkedHashSet::new));
        boolean pruningApplied = false;

        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (!(entry.getKey() instanceof DucklakeColumnHandle columnHandle)) {
                continue;
            }

            Domain domain = entry.getValue();
            if (domain.isNone()) {
                return List.of();
            }

            Optional<PredicateBounds> predicateBounds = extractPredicateBounds(domain);
            if (predicateBounds.isEmpty()) {
                continue;
            }

            PredicateBounds bounds = predicateBounds.get();
            List<Long> matchingFileIds = catalog.getDataFileIdsForPredicate(
                    tableHandle.tableId(),
                    columnHandle.columnId(),
                    tableHandle.snapshotId(),
                    bounds.minValue(),
                    bounds.maxValue());

            pruningApplied = true;
            candidateFileIds.retainAll(matchingFileIds);

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files for table %s using column %s", tableHandle.tableName(), columnHandle.columnName());
                return List.of();
            }
        }

        if (!pruningApplied) {
            return dataFiles;
        }

        List<DucklakeDataFile> prunedDataFiles = dataFiles.stream()
                .filter(file -> candidateFileIds.contains(file.dataFileId()))
                .collect(toImmutableList());

        log.debug("Pruned data files from %d to %d for table %s", dataFiles.size(), prunedDataFiles.size(), tableHandle.tableName());
        return prunedDataFiles;
    }

    private TupleDomain<DucklakeColumnHandle> buildFileStatisticsDomain(Constraint constraint)
    {
        if (constraint == null) {
            return TupleDomain.all();
        }

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            return TupleDomain.all();
        }
        if (summary.isNone()) {
            return TupleDomain.none();
        }

        Optional<Map<ColumnHandle, Domain>> domains = summary.getDomains();
        if (domains.isEmpty() || domains.get().isEmpty()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> ducklakeDomains = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (entry.getKey() instanceof DucklakeColumnHandle columnHandle) {
                ducklakeDomains.put(columnHandle, entry.getValue());
            }
        }

        Map<DucklakeColumnHandle, Domain> result = ducklakeDomains.buildOrThrow();
        if (result.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(result);
    }

    private Optional<PredicateBounds> extractPredicateBounds(Domain domain)
    {
        if (domain.isOnlyNull() || domain.getValues().isAll()) {
            return Optional.empty();
        }

        return domain.getValues().getValuesProcessor().transform(
                ranges -> {
                    if (ranges.getRangeCount() == 0) {
                        return Optional.empty();
                    }

                    Range span = ranges.getSpan();
                    Object minValue = span.getLowValue()
                            .map(value -> normalizePredicateValue(domain.getType(), value))
                            .orElse(null);
                    Object maxValue = span.getHighValue()
                            .map(value -> normalizePredicateValue(domain.getType(), value))
                            .orElse(null);

                    if (minValue == null && maxValue == null) {
                        return Optional.empty();
                    }
                    return Optional.of(new PredicateBounds(minValue, maxValue));
                },
                discreteValues -> extractDiscreteValueBounds(domain.getType(), discreteValues),
                allOrNone -> Optional.empty());
    }

    private Optional<PredicateBounds> extractDiscreteValueBounds(Type type, io.trino.spi.predicate.DiscreteValues discreteValues)
    {
        if (discreteValues.getValuesCount() == 0) {
            return Optional.empty();
        }

        Object minValue = null;
        Object maxValue = null;
        for (Object value : discreteValues.getValues()) {
            Object normalized = normalizePredicateValue(type, value);
            if (minValue == null || compareNormalized(normalized, minValue) < 0) {
                minValue = normalized;
            }
            if (maxValue == null || compareNormalized(normalized, maxValue) > 0) {
                maxValue = normalized;
            }
        }
        return Optional.of(new PredicateBounds(minValue, maxValue));
    }

    @SuppressWarnings("unchecked")
    private static int compareNormalized(Object left, Object right)
    {
        return ((Comparable<Object>) left).compareTo(right);
    }

    private Object normalizePredicateValue(Type type, Object value)
    {
        if (value instanceof io.airlift.slice.Slice slice) {
            return slice.toStringUtf8();
        }
        if (type.equals(DATE) && value instanceof Long daysSinceEpoch) {
            return LocalDate.ofEpochDay(daysSinceEpoch).toString();
        }
        return value;
    }

    private List<DucklakeDataFile> pruneByPartitionValues(
            List<DucklakeDataFile> dataFiles,
            DucklakeTableHandle tableHandle)
    {
        TupleDomain<DucklakeColumnHandle> enforced = tableHandle.enforcedPredicate();
        if (enforced.isAll()) {
            return dataFiles;
        }
        if (enforced.isNone()) {
            return List.of();
        }
        if (dataFiles.isEmpty()) {
            return dataFiles;
        }

        List<DucklakePartitionSpec> specs = catalog.getPartitionSpecs(
                tableHandle.tableId(), tableHandle.snapshotId());
        if (specs.isEmpty()) {
            return dataFiles;
        }

        Map<Long, List<DucklakeFilePartitionValue>> filePartValues =
                catalog.getFilePartitionValues(tableHandle.tableId(), tableHandle.snapshotId());

        // Build columnId -> list of (partitionKeyIndex, transform) for all fields
        // A single column can have multiple transforms (e.g., year + month on the same date column)
        Map<Long, List<PartitionKeyMapping>> columnToPartKeys = new HashMap<>();
        for (DucklakePartitionSpec spec : specs) {
            for (DucklakePartitionField field : spec.fields()) {
                columnToPartKeys.computeIfAbsent(field.columnId(), _ -> new ArrayList<>())
                        .add(new PartitionKeyMapping(field.partitionKeyIndex(), field.transform()));
            }
        }

        Set<Long> candidateFileIds = dataFiles.stream()
                .map(DucklakeDataFile::dataFileId)
                .collect(toCollection(LinkedHashSet::new));

        for (Map.Entry<DucklakeColumnHandle, Domain> entry : enforced.getDomains().orElse(Map.of()).entrySet()) {
            DucklakeColumnHandle column = entry.getKey();
            Domain domain = entry.getValue();
            List<PartitionKeyMapping> mappings = columnToPartKeys.get(column.columnId());
            if (mappings == null) {
                continue;
            }

            candidateFileIds.removeIf(fileId -> {
                List<DucklakeFilePartitionValue> values = filePartValues.getOrDefault(fileId, List.of());
                // A file is pruned if ANY partition transform definitively excludes it
                for (PartitionKeyMapping mapping : mappings) {
                    Optional<String> partValue = values.stream()
                            .filter(v -> v.partitionKeyIndex() == mapping.keyIndex())
                            .map(DucklakeFilePartitionValue::partitionValue)
                            .findFirst();
                    if (partValue.isEmpty()) {
                        continue;
                    }
                    if (!partitionValueMatchesDomain(column.columnType(), partValue.get(), domain, mapping.transform())) {
                        return true; // this transform excludes the file
                    }
                }
                return false; // no transform excluded the file
            });

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files by partition values for table %s", tableHandle.tableName());
                return List.of();
            }
        }

        List<DucklakeDataFile> result = dataFiles.stream()
                .filter(f -> candidateFileIds.contains(f.dataFileId()))
                .collect(toImmutableList());
        log.debug("Partition pruning: %d -> %d files for table %s", dataFiles.size(), result.size(), tableHandle.tableName());
        return result;
    }

    private static boolean partitionValueMatchesDomain(Type columnType, String partitionValue, Domain domain, DucklakePartitionTransform transform)
    {
        try {
            if (transform.isIdentity()) {
                Object nativeValue = parsePartitionValue(columnType, partitionValue);
                return domain.includesNullableValue(nativeValue);
            }
            if (transform.isTemporal()) {
                return temporalPartitionMatchesDomain(columnType, partitionValue, domain, transform);
            }
            return true; // unknown transform — don't prune
        }
        catch (RuntimeException _) {
            return true; // parse failure — don't prune to avoid false negatives
        }
    }

    private static boolean temporalPartitionMatchesDomain(Type columnType, String partitionValue, Domain domain, DucklakePartitionTransform transform)
    {
        // Partition value for temporal transforms is the transformed integer (e.g., years from epoch)
        long transformedValue = Long.parseLong(partitionValue);

        // Convert domain bounds to transformed values and check containment
        if (domain.isNone()) {
            return false;
        }
        if (domain.getValues().isAll()) {
            return true;
        }

        // For each range in the domain, apply the transform to the bounds and check
        // if the partition's transformed value could overlap
        return domain.getValues().getValuesProcessor().transform(
                ranges -> {
                    for (Range range : ranges.getOrderedRanges()) {
                        if (temporalRangeContainsTransformedValue(columnType, range, transformedValue, transform)) {
                            return true;
                        }
                    }
                    return false;
                },
                discreteValues -> {
                    for (Object value : discreteValues.getValues()) {
                        long transformed = applyTemporalTransform(columnType, value, transform);
                        if (transformed == transformedValue) {
                            return true;
                        }
                    }
                    return false;
                },
                allOrNone -> true);
    }

    private static boolean temporalRangeContainsTransformedValue(Type columnType, Range range, long transformedValue, DucklakePartitionTransform transform)
    {
        long lowTransformed = range.getLowValue()
                .map(v -> applyTemporalTransform(columnType, v, transform))
                .orElse(Long.MIN_VALUE);
        long highTransformed = range.getHighValue()
                .map(v -> applyTemporalTransform(columnType, v, transform))
                .orElse(Long.MAX_VALUE);

        // For month/day/hour transforms, values can wrap (e.g., a date range from Nov to Feb
        // produces month values 11..2). In wrapping cases, skip pruning to avoid false negatives.
        if (transform.isTemporal() && !transform.equals(DucklakePartitionTransform.YEAR) && lowTransformed > highTransformed) {
            return true; // wrapping range — don't prune
        }
        return transformedValue >= lowTransformed && transformedValue <= highTransformed;
    }

    private static long applyTemporalTransform(Type columnType, Object value, DucklakePartitionTransform transform)
    {
        // DuckDB's Ducklake extension writes calendar values for temporal transforms:
        // year -> calendar year (e.g., 2023), month -> calendar month (1-12),
        // day -> day of month (1-31), hour -> hour of day (0-23)
        LocalDate date;
        int hour = 0;
        if (columnType.equals(DATE)) {
            date = LocalDate.ofEpochDay((long) value);
        }
        else {
            // Timestamp types: value is micros since epoch
            long microsSinceEpoch = (long) value;
            long secondsSinceEpoch = Math.floorDiv(microsSinceEpoch, 1_000_000);
            date = LocalDate.ofEpochDay(Math.floorDiv(secondsSinceEpoch, 86400));
            hour = (int) (Math.floorMod(secondsSinceEpoch, 86400) / 3600);
        }
        return switch (transform) {
            case YEAR -> date.getYear();
            case MONTH -> date.getMonthValue();
            case DAY -> date.getDayOfMonth();
            case HOUR -> hour;
            default -> throw new IllegalArgumentException("Unsupported transform: " + transform);
        };
    }

    private static Object parsePartitionValue(Type type, String value)
    {
        if (type.equals(VARCHAR) || type instanceof io.trino.spi.type.VarcharType) {
            return Slices.utf8Slice(value);
        }
        if (type.equals(BIGINT)) {
            return Long.parseLong(value);
        }
        if (type.equals(INTEGER)) {
            return (long) Integer.parseInt(value);
        }
        if (type.equals(SMALLINT)) {
            return (long) Short.parseShort(value);
        }
        if (type.equals(TINYINT)) {
            return (long) Byte.parseByte(value);
        }
        if (type.equals(DOUBLE)) {
            return Double.parseDouble(value);
        }
        if (type.equals(REAL)) {
            return (long) Float.floatToIntBits(Float.parseFloat(value));
        }
        if (type.equals(DATE)) {
            return LocalDate.parse(value).toEpochDay();
        }
        if (type.equals(BOOLEAN)) {
            return Boolean.parseBoolean(value);
        }
        throw new IllegalArgumentException("Unsupported partition value type: " + type);
    }

    private record PartitionKeyMapping(int keyIndex, DucklakePartitionTransform transform) {}

    private DucklakeSplit createSplit(DucklakeDataFile dataFile, String tableDataPath, TupleDomain<DucklakeColumnHandle> fileStatisticsDomain)
    {
        // Resolve the full path for the data file
        String dataFilePath = resolveFilePath(dataFile.path(), dataFile.pathIsRelative(), tableDataPath);

        // Resolve delete file path if present
        Optional<String> deleteFilePath = dataFile.deleteFilePath()
                .map(path -> resolveFilePath(path, dataFile.deleteFilePathIsRelative().orElse(false), tableDataPath));

        return new DucklakeSplit(
                dataFilePath,
                deleteFilePath,
                dataFile.rowIdStart(),
                dataFile.recordCount(),
                dataFile.fileSizeBytes(),
                dataFile.fileFormat(),
                fileStatisticsDomain);
    }

    private String resolveFilePath(String path, boolean isRelative, String tableDataPath)
    {
        if (!isRelative) {
            return path;
        }

        return Location.of(tableDataPath).appendPath(path).toString();
    }

    private String resolveTableDataPath(DucklakeSchema schema, DucklakeTable table)
    {
        Optional<String> catalogDataPath = catalog.getDataPath();
        String rootDataPath = catalogDataPath.orElseGet(config::getDataPath);

        if (rootDataPath == null) {
            throw new IllegalStateException("No data path configured for relative file paths");
        }

        String schemaDataPath = resolveScopedPath(schema.path(), schema.pathIsRelative(), rootDataPath);
        return resolveScopedPath(table.path(), table.pathIsRelative(), schemaDataPath);
    }

    private String resolveScopedPath(Optional<String> path, Optional<Boolean> isRelative, String parentPath)
    {
        if (path.isEmpty() || path.get().isBlank()) {
            return parentPath;
        }

        if (isRelative.orElse(false)) {
            return Location.of(parentPath).appendPath(path.get()).toString();
        }
        return path.get();
    }

    private record PredicateBounds(Object minValue, Object maxValue) {}
}
