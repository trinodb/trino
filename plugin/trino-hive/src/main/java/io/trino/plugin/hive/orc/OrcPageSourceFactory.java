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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.NameBasedFieldMapper;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.TupleDomainOrcPredicate;
import io.trino.orc.TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.acid.AcidSchema;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.hive.formats.HiveClassNames.ORC_SERDE_CLASS;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcReader.NameBasedProjectedLayout.createProjectedLayout;
import static io.trino.orc.OrcReader.fullyProjectedLayout;
import static io.trino.orc.metadata.OrcMetadataWriter.PRESTO_WRITER_ID;
import static io.trino.orc.metadata.OrcMetadataWriter.TRINO_WRITER_ID;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.INT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static io.trino.plugin.hive.HivePageSourceProvider.BUCKET_CHANNEL;
import static io.trino.plugin.hive.HivePageSourceProvider.ORIGINAL_TRANSACTION_CHANNEL;
import static io.trino.plugin.hive.HivePageSourceProvider.ROW_ID_CHANNEL;
import static io.trino.plugin.hive.HivePageSourceProvider.getProjection;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static io.trino.plugin.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isOrcNestedLazy;
import static io.trino.plugin.hive.HiveSessionProperties.isUseOrcColumnNames;
import static io.trino.plugin.hive.orc.OrcFileWriter.computeBucketValue;
import static io.trino.plugin.hive.orc.OrcPageSource.handleException;
import static io.trino.plugin.hive.orc.OrcTypeTranslator.createCoercer;
import static io.trino.plugin.hive.util.HiveUtil.splitError;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public class OrcPageSourceFactory
        implements HivePageSourceFactory
{
    private static final Block ORIGINAL_FILE_TRANSACTION_ID_BLOCK = nativeValueToBlock(BIGINT, 0L);
    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");
    private final OrcReaderOptions orcReaderOptions;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats stats;
    private final DateTimeZone legacyTimeZone;
    private final int domainCompactionThreshold;

    @Inject
    public OrcPageSourceFactory(
            OrcReaderConfig config,
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats stats,
            HiveConfig hiveConfig)
    {
        this(
                config.toOrcReaderOptions(),
                fileSystemFactory,
                stats,
                hiveConfig.getOrcLegacyDateTimeZone(),
                hiveConfig.getDomainCompactionThreshold());
    }

    public OrcPageSourceFactory(
            OrcReaderOptions orcReaderOptions,
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats stats,
            DateTimeZone legacyTimeZone)
    {
        this(orcReaderOptions, fileSystemFactory, stats, legacyTimeZone, 0);
    }

    public OrcPageSourceFactory(
            OrcReaderOptions orcReaderOptions,
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats stats,
            DateTimeZone legacyTimeZone,
            int domainCompactionThreshold)
    {
        this.orcReaderOptions = requireNonNull(orcReaderOptions, "orcReaderOptions is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.legacyTimeZone = legacyTimeZone;
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    public static boolean stripUnnecessaryProperties(String serializationLibraryName)
    {
        return ORC_SERDE_CLASS.equals(serializationLibraryName);
    }

    @Override
    public Optional<ConnectorPageSource> createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            Schema schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!ORC_SERDE_CLASS.equals(schema.serializationLibraryName())) {
            return Optional.empty();
        }

        return Optional.of(createOrcPageSource(
                session,
                path,
                start,
                length,
                estimatedFileSize,
                fileModifiedTime,
                columns,
                isUseOrcColumnNames(session) || schema.isFullAcidTable(),
                schema.isFullAcidTable(),
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
                bucketNumber,
                originalFile,
                transaction,
                stats));
    }

    private ConnectorPageSource createOrcPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            boolean isFullAcid,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone legacyFileTimeZone,
            OrcReaderOptions options,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction,
            FileFormatDataSourceStats stats)
    {
        for (HiveColumnHandle column : columns) {
            checkArgument(column.getColumnType() == REGULAR, "column type must be regular: %s", column);
        }
        checkArgument(!effectivePredicate.isNone());

        OrcDataSource orcDataSource;

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            TrinoInputFile inputFile = fileSystem.newInputFile(path, estimatedFileSize, Instant.ofEpochMilli(fileModifiedTime));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    estimatedFileSize,
                    options,
                    inputFile,
                    stats);
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
        try {
            Optional<OrcReader> optionalOrcReader = OrcReader.createOrcReader(orcDataSource, options);
            if (optionalOrcReader.isEmpty()) {
                return new EmptyPageSource();
            }
            OrcReader reader = optionalOrcReader.get();
            if (!originalFile && acidInfo.isPresent() && !acidInfo.get().isOrcAcidVersionValidated()) {
                validateOrcAcidVersion(path, reader);
            }

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            List<OrcColumn> fileReadColumns = new ArrayList<>();
            List<Type> fileReadTypes = new ArrayList<>();
            List<OrcReader.ProjectedLayout> fileReadLayouts = new ArrayList<>();
            boolean originalFilesPresent = acidInfo.isPresent() && hasOriginalFiles(acidInfo.get());
            if (isFullAcid && !originalFilesPresent) {
                verifyAcidSchema(reader, path);
                Map<String, OrcColumn> acidColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName().toLowerCase(ENGLISH));

                fileColumns = ensureColumnNameConsistency(
                        requireNonNull(acidColumnsByName.get(AcidSchema.ACID_COLUMN_ROW_STRUCT.toLowerCase(ENGLISH))).getNestedColumns(),
                        columns);

                fileReadColumns.add(requireNonNull(acidColumnsByName.get(AcidSchema.ACID_COLUMN_ORIGINAL_TRANSACTION.toLowerCase(ENGLISH))));
                fileReadTypes.add(BIGINT);
                fileReadLayouts.add(fullyProjectedLayout());

                fileReadColumns.add(requireNonNull(acidColumnsByName.get(AcidSchema.ACID_COLUMN_BUCKET.toLowerCase(ENGLISH))));
                fileReadTypes.add(INTEGER);
                fileReadLayouts.add(fullyProjectedLayout());

                fileReadColumns.add(requireNonNull(acidColumnsByName.get(AcidSchema.ACID_COLUMN_ROW_ID.toLowerCase(ENGLISH))));
                fileReadTypes.add(BIGINT);
                fileReadLayouts.add(fullyProjectedLayout());
            }

            Map<String, OrcColumn> fileColumnsByName = ImmutableMap.of();
            if (useOrcColumnNames) {
                verifyFileHasColumnNames(fileColumns, path);

                // Convert column names read from ORC files to lower case to be consistent with those stored in Hive Metastore
                fileColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName().toLowerCase(ENGLISH));
            }

            // Mapping from the base column key (name or index) to the dereferences paths (fields) requested by the caller
            Map<Object, List<List<String>>> projectionsByBaseColumnKey = columns.stream()
                    .collect(Collectors.groupingBy(
                            useOrcColumnNames ? HiveColumnHandle::getBaseColumnName : HiveColumnHandle::getBaseHiveColumnIndex,
                            mapping(
                                    OrcPageSourceFactory::getDereferencesAsList, toList())));

            TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder()
                    .setBloomFiltersEnabled(options.isBloomFiltersEnabled())
                    .setDomainCompactionThreshold(domainCompactionThreshold);
            Map<HiveColumnHandle, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                    .orElseThrow(() -> new IllegalArgumentException("Effective predicate is none"));
            TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
            Map<Object, Integer> baseColumnKeyToOrdinal = new HashMap<>();
            for (HiveColumnHandle column : columns) {
                HiveColumnHandle baseColumn = column.getBaseColumn();
                Integer ordinal = baseColumnKeyToOrdinal.get(useOrcColumnNames ? column.getBaseColumnName() : column.getBaseHiveColumnIndex());
                if (ordinal == null) {
                    OrcColumn orcBaseColumn = null;
                    OrcReader.ProjectedLayout projectedLayout = null;
                    Map<Optional<HiveColumnProjectionInfo>, Domain> columnDomains = null;
                    if (useOrcColumnNames) {
                        String columnName = baseColumn.getName().toLowerCase(ENGLISH);
                        orcBaseColumn = fileColumnsByName.get(columnName);
                        if (orcBaseColumn != null) {
                            projectedLayout = createProjectedLayout(orcBaseColumn, projectionsByBaseColumnKey.get(columnName));
                            columnDomains = effectivePredicateDomains.entrySet().stream()
                                    .filter(columnDomain -> columnDomain.getKey().getBaseColumnName().toLowerCase(ENGLISH).equals(columnName))
                                    .collect(toImmutableMap(columnDomain -> columnDomain.getKey().getHiveColumnProjectionInfo(), Map.Entry::getValue));
                        }
                    }
                    else if (baseColumn.getBaseHiveColumnIndex() < fileColumns.size()) {
                        orcBaseColumn = fileColumns.get(baseColumn.getBaseHiveColumnIndex());
                        if (orcBaseColumn != null) {
                            projectedLayout = createProjectedLayout(orcBaseColumn, projectionsByBaseColumnKey.get(baseColumn.getBaseHiveColumnIndex()));
                            columnDomains = effectivePredicateDomains.entrySet().stream()
                                    .filter(columnDomain -> columnDomain.getKey().getBaseHiveColumnIndex() == baseColumn.getBaseHiveColumnIndex())
                                    .collect(toImmutableMap(columnDomain -> columnDomain.getKey().getHiveColumnProjectionInfo(), Map.Entry::getValue));
                        }
                    }

                    if (orcBaseColumn == null) {
                        transforms.constantValue(column.getType().createNullBlock());
                        continue;
                    }

                    ordinal = fileReadColumns.size();
                    baseColumnKeyToOrdinal.put(useOrcColumnNames ? column.getBaseColumnName() : column.getBaseHiveColumnIndex(), ordinal);
                    fileReadColumns.add(orcBaseColumn);
                    fileReadLayouts.add(projectedLayout);
                    // todo it should be possible to compute fileReadType without creating the coercer
                    fileReadTypes.add(createCoercer(orcBaseColumn.getColumnType(), orcBaseColumn.getNestedColumns(), baseColumn.getType())
                            .map(TypeCoercer::getFromType)
                            .orElse(baseColumn.getType()));

                    // Add predicates on top-level and nested columns
                    for (Map.Entry<Optional<HiveColumnProjectionInfo>, Domain> columnDomain : columnDomains.entrySet()) {
                        OrcColumn nestedColumn = getNestedColumn(orcBaseColumn, columnDomain.getKey());
                        if (nestedColumn != null) {
                            predicateBuilder.addColumn(nestedColumn.getColumnId(), columnDomain.getValue());
                        }
                    }
                }

                OrcColumn orcBaseColumn = fileReadColumns.get(ordinal);
                if (column.isBaseColumn()) {
                    Optional<TypeCoercer<?, ?>> coercer = createCoercer(orcBaseColumn.getColumnType(), orcBaseColumn.getNestedColumns(), column.getType());
                    transforms.column(ordinal, coercer.map(identity()));
                }
                else {
                    // Dereference the nested column by name
                    OrcColumn orcFieldColumn = orcBaseColumn;
                    for (String fieldName : column.getHiveColumnProjectionInfo().orElseThrow().getDereferenceNames()) {
                        Optional<OrcColumn> nestedField = orcFieldColumn.getNestedColumns().stream()
                                .filter(field -> field.getColumnName().equalsIgnoreCase(fieldName))
                                .collect(toOptional());
                        if (nestedField.isEmpty()) {
                            orcFieldColumn = null;
                            break;
                        }
                        orcFieldColumn = nestedField.get();
                    }

                    if (orcFieldColumn == null) {
                        transforms.constantValue(column.getType().createNullBlock());
                    }
                    else {
                        Optional<TypeCoercer<?, ?>> coercer = createCoercer(orcFieldColumn.getColumnType(), orcFieldColumn.getNestedColumns(), column.getType());
                        transforms.dereferenceField(
                                ImmutableList.<Integer>builder()
                                        .add(ordinal)
                                        .addAll(getProjection(column, baseColumn))
                                        .build(),
                                coercer.map(identity()));
                    }
                }
            }

            Optional<Long> originalFileRowId = acidInfo
                    .filter(OrcPageSourceFactory::hasOriginalFiles)
                    // TODO reduce number of file footer accesses. Currently this is quadratic to the number of original files.
                    .map(_ -> OriginalFilesUtils.getPrecedingRowCount(
                            acidInfo.get().getOriginalFiles(),
                            path,
                            fileSystemFactory,
                            session.getIdentity(),
                            options,
                            stats));

            boolean appendRowNumberColumn = false;
            if (transaction.isMerge()) {
                if (originalFile) {
                    transforms.transform(new MergedRowAdaptationWithOriginalFiles(originalFileRowId.orElse(0L), bucketNumber.orElse(0)));
                    appendRowNumberColumn = true;
                }
                else {
                    transforms.transform(new MergedRowPageFunction());
                }
            }

            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    fileReadLayouts,
                    appendRowNumberColumn,
                    predicateBuilder.build(),
                    start,
                    length,
                    legacyFileTimeZone,
                    memoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleException(orcDataSource.getId(), exception),
                    NameBasedFieldMapper::create);

            Optional<OrcDeletedRows> deletedRows = acidInfo.map(info ->
                    new OrcDeletedRows(
                            path.fileName(),
                            new OrcDeleteDeltaPageSourceFactory(options, stats),
                            session.getIdentity(),
                            fileSystemFactory,
                            info,
                            bucketNumber,
                            memoryUsage));

            ConnectorPageSource pageSource = new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    deletedRows,
                    originalFileRowId,
                    memoryUsage,
                    stats,
                    reader.getCompressionKind());
            return transforms.build(pageSource);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException _) {
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            if (e instanceof OrcCorruptionException) {
                throw new TrinoException(HIVE_BAD_DATA, e);
            }
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }
    }

    private static void validateOrcAcidVersion(Location path, OrcReader reader)
    {
        // Trino cannot read ORC ACID tables with a version < 2 (written by Hive older than 3.0)
        // See https://github.com/trinodb/trino/issues/2790#issuecomment-591901728 for more context

        // If we did not manage to validate if ORC ACID version used by table is supported based on _orc_acid_version metadata file,
        // we check the data file footer.

        if (reader.getFooter().getNumberOfRows() == 0) {
            // If the file is empty, assume the version is good. We do not want to depend on metadata in such a case
            // as some hadoop distributions do not write ORC ACID metadata for empty ORC files
            return;
        }

        int writerId = reader.getFooter().getWriterId().orElseThrow(() -> new TrinoException(HIVE_BAD_DATA, "writerId not set in ORC metadata in " + path));
        if (writerId == TRINO_WRITER_ID || writerId == PRESTO_WRITER_ID) {
            // The file was written by Trino, so it is ok
            return;
        }

        Optional<Integer> hiveAcidVersion = getHiveAcidVersion(reader);
        if (hiveAcidVersion.isEmpty() || hiveAcidVersion.get() < 2) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    format("Hive transactional tables are supported since Hive 3.0. Expected `hive.acid.version` in ORC metadata in %s to be >=2 but was %s. " +
                                    "If you have upgraded from an older version of Hive, make sure a major compaction has been run at least once after the upgrade.",
                            path,
                            hiveAcidVersion.map(String::valueOf).orElse("<empty>")));
        }
    }

    private static Optional<Integer> getHiveAcidVersion(OrcReader reader)
    {
        Slice slice = reader.getFooter().getUserMetadata().get("hive.acid.version");
        if (slice == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.valueOf(slice.toString(UTF_8)));
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    /**
     * Recreate the list of fileColumns, updating the names of any whose names have changed in the
     * corresponding elements of the desiredColumns list.  NOTE: this renaming is only applied to
     * top-level columns, not nested columns.
     *
     * @param fileColumns All OrcColumns nested in the root column of the table.
     * @param columns Columns from the Hive metastore that are being used
     * @return Return the fileColumns list with any OrcColumn corresponding to a desiredColumn renamed if
     * the names differ from those specified in the desiredColumns.
     */
    private static List<OrcColumn> ensureColumnNameConsistency(List<OrcColumn> fileColumns, List<HiveColumnHandle> columns)
    {
        int columnCount = fileColumns.size();
        ImmutableList.Builder<OrcColumn> builder = ImmutableList.builderWithExpectedSize(columnCount);

        Map<Integer, HiveColumnHandle> baseColumnsByColumnIndex = columns.stream()
                .map(HiveColumnHandle::getBaseColumn)
                .collect(toImmutableMap(HiveColumnHandle::getBaseHiveColumnIndex, identity()));

        for (int index = 0; index < columnCount; index++) {
            OrcColumn column = fileColumns.get(index);
            HiveColumnHandle handle = baseColumnsByColumnIndex.get(index);
            if (handle != null && !column.getColumnName().equals(handle.getName())) {
                column = new OrcColumn(column.getPath(), column.getColumnId(), handle.getName(), column.getColumnType(), column.getOrcDataSourceId(), column.getNestedColumns(), column.getAttributes());
            }
            builder.add(column);
        }
        return builder.build();
    }

    private static boolean hasOriginalFiles(AcidInfo acidInfo)
    {
        return !acidInfo.getOriginalFiles().isEmpty();
    }

    private static void verifyFileHasColumnNames(List<OrcColumn> columns, Location path)
    {
        if (!columns.isEmpty() && columns.stream().map(OrcColumn::getColumnName).allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
            throw new TrinoException(
                    HIVE_FILE_MISSING_COLUMN_NAMES,
                    "ORC file does not contain column names in the footer: " + path);
        }
    }

    /*
     * The rowId contains the ACID columns - - originalTransaction, rowId, bucket
     */
    private static final class MergedRowPageFunction
            implements Function<SourcePage, Block>
    {
        @Override
        public Block apply(SourcePage page)
        {
            return fromFieldBlocks(
                    page.getPositionCount(),
                    new Block[] {
                            page.getBlock(ORIGINAL_TRANSACTION_CHANNEL),
                            page.getBlock(BUCKET_CHANNEL),
                            page.getBlock(ROW_ID_CHANNEL)
                    });
        }
    }

    /**
     * The rowId contains the ACID columns - - originalTransaction, rowId, bucket,
     * derived from the original file.  The transactionId is always zero,
     * and the rowIds count up from the startingRowId.
     */
    private static final class MergedRowAdaptationWithOriginalFiles
            implements Function<SourcePage, Block>
    {
        private final long startingRowId;
        private final Block bucketBlock;

        public MergedRowAdaptationWithOriginalFiles(long startingRowId, int bucketId)
        {
            this.startingRowId = startingRowId;
            this.bucketBlock = nativeValueToBlock(INTEGER, (long) computeBucketValue(bucketId, 0));
        }

        @Override
        public Block apply(SourcePage sourcePage)
        {
            int positionCount = sourcePage.getPositionCount();

            LongArrayBlock rowNumberBlock = (LongArrayBlock) sourcePage.getBlock(sourcePage.getChannelCount() - 1);
            if (startingRowId != 0) {
                long[] newRowNumbers = new long[rowNumberBlock.getPositionCount()];
                for (int index = 0; index < rowNumberBlock.getPositionCount(); index++) {
                    newRowNumbers[index] = startingRowId + rowNumberBlock.getLong(index);
                }
                rowNumberBlock = new LongArrayBlock(rowNumberBlock.getPositionCount(), Optional.empty(), newRowNumbers);
            }

            return fromFieldBlocks(
                    positionCount,
                    new Block[] {
                            RunLengthEncodedBlock.create(ORIGINAL_FILE_TRANSACTION_ID_BLOCK, positionCount),
                            RunLengthEncodedBlock.create(bucketBlock, positionCount),
                            rowNumberBlock
                    });
        }
    }

    static void verifyAcidSchema(OrcReader orcReader, Location path)
    {
        OrcColumn rootColumn = orcReader.getRootColumn();
        List<OrcColumn> nestedColumns = rootColumn.getNestedColumns();
        if (nestedColumns.size() != 6) {
            throw new TrinoException(
                    HIVE_BAD_DATA,
                    format(
                            "ORC ACID file should have 6 columns, found %s %s in %s",
                            nestedColumns.size(),
                            nestedColumns.stream()
                                    .map(column -> format("%s (%s)", column.getColumnName(), column.getColumnType()))
                                    .collect(toImmutableList()),
                            path));
        }
        verifyAcidColumn(orcReader, 0, AcidSchema.ACID_COLUMN_OPERATION, INT, path);
        verifyAcidColumn(orcReader, 1, AcidSchema.ACID_COLUMN_ORIGINAL_TRANSACTION, LONG, path);
        verifyAcidColumn(orcReader, 2, AcidSchema.ACID_COLUMN_BUCKET, INT, path);
        verifyAcidColumn(orcReader, 3, AcidSchema.ACID_COLUMN_ROW_ID, LONG, path);
        verifyAcidColumn(orcReader, 4, AcidSchema.ACID_COLUMN_CURRENT_TRANSACTION, LONG, path);
        verifyAcidColumn(orcReader, 5, AcidSchema.ACID_COLUMN_ROW_STRUCT, STRUCT, path);
    }

    private static void verifyAcidColumn(OrcReader orcReader, int columnIndex, String columnName, OrcTypeKind columnType, Location path)
    {
        OrcColumn column = orcReader.getRootColumn().getNestedColumns().get(columnIndex);
        if (!column.getColumnName().toLowerCase(ENGLISH).equals(columnName.toLowerCase(ENGLISH))) {
            throw new TrinoException(HIVE_BAD_DATA, format("ORC ACID file column %s should be named %s: %s", columnIndex, columnName, path));
        }
        if (column.getColumnType().getOrcTypeKind() != columnType) {
            throw new TrinoException(HIVE_BAD_DATA, format("ORC ACID file %s column should be type %s: %s", columnName, columnType, path));
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

    private static List<String> getDereferencesAsList(HiveColumnHandle column)
    {
        return column.getHiveColumnProjectionInfo()
                .map(info -> info.getDereferenceNames().stream()
                        .map(dereference -> dereference.toLowerCase(ENGLISH))
                        .collect(toImmutableList()))
                .orElse(ImmutableList.of());
    }
}
