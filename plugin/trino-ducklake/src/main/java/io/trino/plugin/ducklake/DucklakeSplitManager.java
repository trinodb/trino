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
import io.trino.filesystem.Location;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeDataFile;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.DateType.DATE;
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

        DucklakeTable tableMetadata = catalog.getTableById(tableHandle.tableId(), tableHandle.snapshotId())
                .orElseThrow(() -> new IllegalStateException("Table metadata missing for table ID: " + tableHandle.tableId()));
        DucklakeSchema schemaMetadata = catalog.getSchema(tableHandle.schemaName(), tableHandle.snapshotId())
                .orElseThrow(() -> new IllegalStateException("Schema metadata missing for schema: " + tableHandle.schemaName()));
        String tableDataPath = resolveTableDataPath(schemaMetadata, tableMetadata);

        log.debug("Found %d data files for table %s", dataFiles.size(), tableHandle.tableName());

        TupleDomain<DucklakeColumnHandle> fileStatisticsDomain = buildFileStatisticsDomain(constraint);
        dataFiles = pruneDataFiles(dataFiles, tableHandle, constraint);

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
                discreteValues -> Optional.empty(),
                allOrNone -> Optional.empty());
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
