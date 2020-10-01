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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.TupleDomainOrcPredicate;
import io.prestosql.orc.TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder;
import io.prestosql.orc.metadata.OrcType.OrcTypeKind;
import io.prestosql.plugin.hive.AcidInfo;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveColumnProjectionInfo;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.ReaderProjections;
import io.prestosql.plugin.hive.orc.OrcPageSource.ColumnAdaptation;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.OrcReader.ProjectedLayout.createProjectedLayout;
import static io.prestosql.orc.OrcReader.ProjectedLayout.fullyProjectedLayout;
import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.INT;
import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HivePageSourceFactory.ReaderPageSourceWithProjections.noProjectionAdaptation;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcNestedLazy;
import static io.prestosql.plugin.hive.HiveSessionProperties.isUseOrcColumnNames;
import static io.prestosql.plugin.hive.ReaderProjections.projectBaseColumns;
import static io.prestosql.plugin.hive.orc.OrcPageSource.handleException;
import static io.prestosql.plugin.hive.util.HiveUtil.isDeserializerClass;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;

public class OrcPageSourceFactory
        implements HivePageSourceFactory
{
    // ACID format column names
    public static final String ACID_COLUMN_OPERATION = "operation";
    public static final String ACID_COLUMN_ORIGINAL_TRANSACTION = "originalTransaction";
    public static final String ACID_COLUMN_BUCKET = "bucket";
    public static final String ACID_COLUMN_ROW_ID = "rowId";
    public static final String ACID_COLUMN_CURRENT_TRANSACTION = "currentTransaction";
    public static final String ACID_COLUMN_ROW_STRUCT = "row";

    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");
    private final OrcReaderOptions orcReaderOptions;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final DateTimeZone legacyTimeZone;

    @Inject
    public OrcPageSourceFactory(OrcReaderConfig config, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, HiveConfig hiveConfig)
    {
        this(config.toOrcReaderOptions(), hdfsEnvironment, stats, requireNonNull(hiveConfig, "hiveConfig is null").getOrcLegacyDateTimeZone());
    }

    public OrcPageSourceFactory(
            OrcReaderOptions orcReaderOptions,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            DateTimeZone legacyTimeZone)
    {
        this.orcReaderOptions = requireNonNull(orcReaderOptions, "orcReaderOptions is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.legacyTimeZone = legacyTimeZone;
    }

    @Override
    public Optional<ReaderPageSourceWithProjections> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo)
    {
        if (!isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (estimatedFileSize == 0) {
            ReaderPageSourceWithProjections context = noProjectionAdaptation(new FixedPageSource(ImmutableList.of()));
            return Optional.of(context);
        }

        Optional<ReaderProjections> projectedReaderColumns = projectBaseColumns(columns);

        ConnectorPageSource orcPageSource = createOrcPageSource(
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                estimatedFileSize,
                projectedReaderColumns
                        .map(ReaderProjections::getReaderColumns)
                        .orElse(columns),
                columns,
                isUseOrcColumnNames(session),
                isFullAcidTable(Maps.fromProperties(schema)),
                effectivePredicate,
                legacyTimeZone,
                orcReaderOptions
                        .withMaxMergeDistance(getOrcMaxMergeDistance(session))
                        .withMaxBufferSize(getOrcMaxBufferSize(session))
                        .withStreamBufferSize(getOrcStreamBufferSize(session))
                        .withTinyStripeThreshold(getOrcTinyStripeThreshold(session))
                        .withMaxReadBlockSize(getOrcMaxReadBlockSize(session))
                        .withLazyReadSmallRanges(getOrcLazyReadSmallRanges(session))
                        .withNestedLazy(isOrcNestedLazy(session))
                        .withBloomFiltersEnabled(isOrcBloomFiltersEnabled(session)),
                acidInfo,
                stats);

        return Optional.of(new ReaderPageSourceWithProjections(orcPageSource, projectedReaderColumns));
    }

    private static ConnectorPageSource createOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            List<HiveColumnHandle> columns,
            List<HiveColumnHandle> projections,
            boolean useOrcColumnNames,
            boolean isFullAcid,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone legacyFileTimeZone,
            OrcReaderOptions options,
            Optional<AcidInfo> acidInfo,
            FileFormatDataSourceStats stats)
    {
        for (HiveColumnHandle column : columns) {
            checkArgument(column.getColumnType() == REGULAR, "column type must be regular: %s", column);
        }
        checkArgument(!effectivePredicate.isNone());

        OrcDataSource orcDataSource;

        boolean originalFilesPresent = acidInfo.isPresent() && !acidInfo.get().getOriginalFiles().isEmpty();
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(sessionUser, () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    estimatedFileSize,
                    options,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        try {
            Optional<OrcReader> optionalOrcReader = OrcReader.createOrcReader(orcDataSource, options);
            if (optionalOrcReader.isEmpty()) {
                return new FixedPageSource(ImmutableList.of());
            }
            OrcReader reader = optionalOrcReader.get();

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.size() + (isFullAcid ? 2 : 0));
            List<Type> fileReadTypes = new ArrayList<>(columns.size() + (isFullAcid ? 2 : 0));
            List<OrcReader.ProjectedLayout> fileReadLayouts = new ArrayList<>(columns.size() + (isFullAcid ? 2 : 0));
            if (isFullAcid && !originalFilesPresent) {
                verifyAcidSchema(reader, path);
                Map<String, OrcColumn> acidColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName().toLowerCase(ENGLISH));
                fileColumns = acidColumnsByName.get(ACID_COLUMN_ROW_STRUCT.toLowerCase(ENGLISH)).getNestedColumns();

                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_ORIGINAL_TRANSACTION.toLowerCase(ENGLISH)));
                fileReadTypes.add(BIGINT);
                fileReadLayouts.add(fullyProjectedLayout());

                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_ROW_ID.toLowerCase(ENGLISH)));
                fileReadTypes.add(BIGINT);
                fileReadLayouts.add(fullyProjectedLayout());
            }

            Map<String, OrcColumn> fileColumnsByName = ImmutableMap.of();
            if (useOrcColumnNames || isFullAcid) {
                verifyFileHasColumnNames(fileColumns, path);

                // Convert column names read from ORC files to lower case to be consistent with those stored in Hive Metastore
                fileColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName().toLowerCase(ENGLISH));
            }

            Map<String, List<List<String>>> projectionsByColumnName = ImmutableMap.of();
            Map<Integer, List<List<String>>> projectionsByColumnIndex = ImmutableMap.of();
            if (useOrcColumnNames || isFullAcid) {
                projectionsByColumnName = projections.stream()
                        .collect(Collectors.groupingBy(
                                HiveColumnHandle::getBaseColumnName,
                                mapping(
                                        column -> column.getHiveColumnProjectionInfo().map(HiveColumnProjectionInfo::getDereferenceNames).orElse(ImmutableList.<String>of()),
                                        toList())));
            }
            else {
                projectionsByColumnIndex = projections.stream()
                        .collect(Collectors.groupingBy(
                                HiveColumnHandle::getBaseHiveColumnIndex,
                                mapping(
                                        column -> column.getHiveColumnProjectionInfo().map(HiveColumnProjectionInfo::getDereferenceNames).orElse(ImmutableList.<String>of()),
                                        toList())));
            }

            TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder()
                    .setBloomFiltersEnabled(options.isBloomFiltersEnabled());
            Map<HiveColumnHandle, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                    .orElseThrow(() -> new IllegalArgumentException("Effective predicate is none"));
            List<ColumnAdaptation> columnAdaptations = new ArrayList<>(columns.size());
            for (HiveColumnHandle column : columns) {
                OrcColumn orcColumn = null;
                OrcReader.ProjectedLayout projectedLayout = null;
                Map<Optional<HiveColumnProjectionInfo>, Domain> columnDomains = null;

                if (useOrcColumnNames || isFullAcid) {
                    String columnName = column.getName().toLowerCase(ENGLISH);
                    orcColumn = fileColumnsByName.get(columnName);
                    if (orcColumn != null) {
                        projectedLayout = createProjectedLayout(orcColumn, projectionsByColumnName.get(columnName));
                        columnDomains = effectivePredicateDomains.entrySet().stream()
                                .filter(columnDomain -> columnDomain.getKey().getBaseColumnName().toLowerCase(ENGLISH).equals(columnName))
                                .collect(toImmutableMap(columnDomain -> columnDomain.getKey().getHiveColumnProjectionInfo(), Map.Entry::getValue));
                    }
                }
                else if (column.getBaseHiveColumnIndex() < fileColumns.size()) {
                    orcColumn = fileColumns.get(column.getBaseHiveColumnIndex());
                    if (orcColumn != null) {
                        projectedLayout = createProjectedLayout(orcColumn, projectionsByColumnIndex.get(column.getBaseHiveColumnIndex()));
                        columnDomains = effectivePredicateDomains.entrySet().stream()
                                .filter(columnDomain -> columnDomain.getKey().getBaseHiveColumnIndex() == column.getBaseHiveColumnIndex())
                                .collect(toImmutableMap(columnDomain -> columnDomain.getKey().getHiveColumnProjectionInfo(), Map.Entry::getValue));
                    }
                }

                Type readType = column.getType();
                if (orcColumn != null) {
                    int sourceIndex = fileReadColumns.size();
                    columnAdaptations.add(ColumnAdaptation.sourceColumn(sourceIndex));
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(readType);
                    fileReadLayouts.add(projectedLayout);

                    // Add predicates on top-level and nested columns
                    for (Map.Entry<Optional<HiveColumnProjectionInfo>, Domain> columnDomain : columnDomains.entrySet()) {
                        OrcColumn nestedColumn = getNestedColumn(orcColumn, columnDomain.getKey());
                        if (nestedColumn != null) {
                            predicateBuilder.addColumn(nestedColumn.getColumnId(), columnDomain.getValue());
                        }
                    }
                }
                else {
                    columnAdaptations.add(ColumnAdaptation.nullColumn(readType));
                }
            }

            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    fileReadLayouts,
                    predicateBuilder.build(),
                    start,
                    length,
                    legacyFileTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleException(orcDataSource.getId(), exception));

            Optional<OrcDeletedRows> deletedRows = acidInfo.map(info ->
                    new OrcDeletedRows(
                            path.getName(),
                            new OrcDeleteDeltaPageSourceFactory(options, sessionUser, configuration, hdfsEnvironment, stats),
                            sessionUser,
                            configuration,
                            hdfsEnvironment,
                            info));

            Optional<Long> originalFileRowId = acidInfo
                    .filter(OrcPageSourceFactory::hasOriginalFilesAndDeleteDeltas)
                    // TODO reduce number of file footer accesses. Currently this is quadratic to the number of original files.
                    .map(info -> OriginalFilesUtils.getPrecedingRowCount(
                            acidInfo.get().getOriginalFiles(),
                            path,
                            hdfsEnvironment,
                            sessionUser,
                            options,
                            configuration,
                            stats));

            return new OrcPageSource(
                    recordReader,
                    columnAdaptations,
                    orcDataSource,
                    deletedRows,
                    originalFileRowId,
                    systemMemoryUsage,
                    stats);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e instanceof BlockMissingException) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static boolean hasOriginalFilesAndDeleteDeltas(AcidInfo acidInfo)
    {
        return !acidInfo.getDeleteDeltas().isEmpty() && !acidInfo.getOriginalFiles().isEmpty();
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    private static void verifyFileHasColumnNames(List<OrcColumn> columns, Path path)
    {
        if (!columns.isEmpty() && columns.stream().map(OrcColumn::getColumnName).allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
            throw new PrestoException(
                    HIVE_FILE_MISSING_COLUMN_NAMES,
                    "ORC file does not contain column names in the footer: " + path);
        }
    }

    static void verifyAcidSchema(OrcReader orcReader, Path path)
    {
        OrcColumn rootColumn = orcReader.getRootColumn();
        if (rootColumn.getNestedColumns().size() != 6) {
            throw new PrestoException(HIVE_BAD_DATA, "ORC ACID file should have 6 columns: " + path);
        }
        verifyAcidColumn(orcReader, 0, ACID_COLUMN_OPERATION, INT, path);
        verifyAcidColumn(orcReader, 1, ACID_COLUMN_ORIGINAL_TRANSACTION, LONG, path);
        verifyAcidColumn(orcReader, 2, ACID_COLUMN_BUCKET, INT, path);
        verifyAcidColumn(orcReader, 3, ACID_COLUMN_ROW_ID, LONG, path);
        verifyAcidColumn(orcReader, 4, ACID_COLUMN_CURRENT_TRANSACTION, LONG, path);
        verifyAcidColumn(orcReader, 5, ACID_COLUMN_ROW_STRUCT, STRUCT, path);
    }

    private static void verifyAcidColumn(OrcReader orcReader, int columnIndex, String columnName, OrcTypeKind columnType, Path path)
    {
        OrcColumn column = orcReader.getRootColumn().getNestedColumns().get(columnIndex);
        if (!column.getColumnName().toLowerCase(ENGLISH).equals(columnName.toLowerCase(ENGLISH))) {
            throw new PrestoException(HIVE_BAD_DATA, format("ORC ACID file column %s should be named %s: %s", columnIndex, columnName, path));
        }
        if (column.getColumnType() != columnType) {
            throw new PrestoException(HIVE_BAD_DATA, format("ORC ACID file %s column should be type %s: %s", columnName, columnType, path));
        }
    }

    private static OrcColumn getNestedColumn(OrcColumn baseColumn, Optional<HiveColumnProjectionInfo> projectionInfo)
    {
        if (projectionInfo.isEmpty()) {
            return baseColumn;
        }

        OrcColumn current = baseColumn;
        for (String field : projectionInfo.get().getDereferenceNames()) {
            Optional<OrcColumn> orcColumn = current.getNestedColumns().stream()
                    .filter(column -> column.getColumnName().toLowerCase(ENGLISH).equals(field))
                    .findFirst();

            if (orcColumn.isEmpty()) {
                return null;
            }
            current = orcColumn.get();
        }
        return current;
    }
}
