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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.graph.Traverser;
import io.airlift.json.JsonCodec;
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
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.parquet.predicate.Predicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.plugin.hive.orc.HdfsOrcDataSource;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.orc.OrcPageSource.ColumnAdaptation;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.HdfsParquetDataSource;
import io.trino.plugin.hive.parquet.HiveParquetColumnIOConverter;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.iceberg.IcebergParquetColumnIOConverter.FieldContext;
import io.trino.plugin.iceberg.delete.IcebergPositionDeletePageSink;
import io.trino.plugin.iceberg.delete.TrinoDeleteFilter;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructProjection;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcReader.ProjectedLayout;
import static io.trino.orc.OrcReader.fullyProjectedLayout;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
import static io.trino.plugin.iceberg.IcebergColumnHandle.ROW_POSITION_HANDLE;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CURSOR_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_DATA;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcLazyReadSmallRanges;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcMaxBufferSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcMaxMergeDistance;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcMaxReadBlockSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcStreamBufferSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcTinyStripeThreshold;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isOrcBloomFiltersEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isOrcNestedLazy;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.TypeConverter.ICEBERG_BINARY_TYPE;
import static io.trino.plugin.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.joda.time.DateTimeZone.UTC;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final FileIoProvider fileIoProvider;
    private final int maxOpenPartitions;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            FileIoProvider fileIoProvider,
            IcebergConfig config)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.orcReaderOptions = requireNonNull(orcReaderConfig, "orcReaderConfig is null").toOrcReaderOptions();
        this.parquetReaderOptions = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
        requireNonNull(config, "config is null");
        this.maxOpenPartitions = config.getMaxPartitionsPerWriter();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        IcebergSplit split = (IcebergSplit) connectorSplit;
        IcebergTableHandle table = (IcebergTableHandle) connectorTable;
        Table icebergTable = table.getTable();
        FileScanTask task = split.getTask();
        List<IcebergColumnHandle> updateColumns = table.getUpdateColumns();
        List<IcebergColumnHandle> queriedColumns = columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .collect(toImmutableList());

        Map<Integer, Optional<String>> partitionKeys = split.getPartitionKeys();
        Optional<StructLike> partition = task.spec().isUnpartitioned() ? Optional.empty() : Optional.of(task.file().partition());
        Optional<PartitionData> partitionDataForWrite = coercePartitionData(icebergTable.spec(), task.spec(), partition);

        TupleDomain<IcebergColumnHandle> effectivePredicate = table.getUnenforcedPredicate()
                .intersect(dynamicFilter.getCurrentPredicate().transformKeys(IcebergColumnHandle.class::cast))
                .simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);

        // construct the columns that needs to be read by the file reader
        List<Integer> fileReadColumnIds = new ArrayList<>();
        List<IcebergColumnHandle> fileReadColumns = new ArrayList<>();
        List<Boolean> isRowPositionChannel = new ArrayList<>();

        // 1. non-partition queried columns
        Block[] queriedColumnPrefillValues = new Block[queriedColumns.size()];
        int[] queriedColumnFileReadChannels = new int[queriedColumns.size()];
        boolean isDeleteOrUpdateQuery = false;
        for (int idx = 0; idx < queriedColumns.size(); idx++) {
            IcebergColumnHandle column = queriedColumns.get(idx);
            if (column.isTrinoRowIdColumn()) {
                // TODO: it's a bit late to fail here, but failing earlier would cause metadata delete to also fail
                if (ORC == getFileFormat(table.getTable())) {
                    throw new TrinoException(GENERIC_USER_ERROR, "Row level delete and update are not supported for ORC type");
                }
                isDeleteOrUpdateQuery = true;
                queriedColumnPrefillValues[idx] = null;
                queriedColumnFileReadChannels[idx] = -2; // use -2 to indicate $rowid column
            }
            else {
                prefillPartitionValuesAndCompleteFileReadChannels(idx, column, fileReadColumnIds, fileReadColumns,
                        queriedColumnPrefillValues, queriedColumnFileReadChannels, isRowPositionChannel, partitionKeys, false);
            }
        }

        // 2. non-partition equality delete columns
        // make sure the order of delete columns are the same as the schema required by the delete filter
        HdfsContext hdfsContext = new HdfsContext(session);
        FileIO fileIO = fileIoProvider.createFileIo(hdfsContext, session.getQueryId());
        TrinoDeleteFilter deleteFilter = new TrinoDeleteFilter(fileIO, task, icebergTable.schema());
        List<IcebergColumnHandle> deleteColumns = getColumns(deleteFilter.requiredSchema(), typeManager);
        Block[] deleteColumnPrefillValues = new Block[deleteColumns.size()];
        int[] deleteColumnFileReadChannels = new int[deleteColumns.size()];
        Type[] deleteColumnTypes = new Type[deleteColumns.size()];
        int rowPositionChannel = -1;
        for (int idx = 0; idx < deleteColumns.size(); idx++) {
            IcebergColumnHandle column = deleteColumns.get(idx);
            deleteColumnTypes[idx] = column.getType();
            if (column.isIcebergIsDeletedMetadataColumn()) {
                deleteColumnPrefillValues[idx] = nativeValueToBlock(BOOLEAN, false);
                deleteColumnFileReadChannels[idx] = -1;
            }
            else {
                prefillPartitionValuesAndCompleteFileReadChannels(idx, column, fileReadColumnIds, fileReadColumns,
                        deleteColumnPrefillValues, deleteColumnFileReadChannels, isRowPositionChannel, partitionKeys,
                        column.isIcebergRowPositionMetadataColumn());
                if (column.isIcebergRowPositionMetadataColumn()) {
                    rowPositionChannel = deleteColumnFileReadChannels[idx];
                }
            }
        }

        // for delete or update query, if read of delete query does not have position deletes, add that column separately
        if (isDeleteOrUpdateQuery && rowPositionChannel == -1) {
            rowPositionChannel = fileReadColumns.size();
            fileReadColumns.add(ROW_POSITION_HANDLE);
            fileReadColumnIds.add(ROW_POSITION.fieldId());
            isRowPositionChannel.add(true);
        }

        // for update, all table columns are needed and in the order of the table schema
        // the channels for update pages are [update-col1, update-col2, ..., rowId]
        // the rowId channels are [file_path, pos, non-update-col1, non-update-col2, ...]
        // use (-1-idx) to save update channel index, and (+idx) for non-update to avoid using 2 different arrays
        List<Integer> updateColumnIds = updateColumns.stream().map(IcebergColumnHandle::getId).collect(Collectors.toList());
        List<IcebergColumnHandle> allTableColumns = getColumns(icebergTable.schema(), typeManager);
        int[] allTableColumnChannels = new int[allTableColumns.size()];
        List<IcebergColumnHandle> nonUpdateColumns = new ArrayList<>();
        boolean isUpdateQuery = !updateColumnIds.isEmpty();
        if (isUpdateQuery) {
            for (int idx = 0; idx < allTableColumns.size(); idx++) {
                IcebergColumnHandle column = allTableColumns.get(idx);
                int updateChannel = updateColumnIds.indexOf(column.getId());
                if (updateChannel > -1) {
                    allTableColumnChannels[idx] = (-1) - updateChannel;
                }
                else {
                    nonUpdateColumns.add(column);
                    allTableColumnChannels[idx] = nonUpdateColumns.size() - 1;
                }
            }
        }

        // 3. non-partition non-update columns
        // if there is no update column, then non-update column is also empty and it's a no-op
        // otherwise we need to read and pass all the non-update column values into the rowid block
        Block[] nonUpdateColumnPrefillValues = new Block[nonUpdateColumns.size()];
        int[] nonUpdateColumnFileReadChannels = new int[nonUpdateColumns.size()];
        for (int idx = 0; idx < nonUpdateColumns.size(); idx++) {
            prefillPartitionValuesAndCompleteFileReadChannels(idx, nonUpdateColumns.get(idx), fileReadColumnIds, fileReadColumns,
                    nonUpdateColumnPrefillValues, nonUpdateColumnFileReadChannels, isRowPositionChannel, partitionKeys, false);
        }

        // create file page source
        String filePath = task.file().path().toString();
        ReaderPageSource dataPageSource = createDataPageSource(
                session,
                hdfsContext,
                new Path(split.getPath()),
                split.getStart(),
                split.getLength(),
                split.getFileSize(),
                split.getFileFormat(),
                fileReadColumns,
                effectivePredicate,
                isRowPositionChannel);

        IcebergPositionDeletePageSink posDeleteSink = isDeleteOrUpdateQuery ? new IcebergPositionDeletePageSink(
                icebergTable.spec(),
                partitionDataForWrite,
                icebergTable.locationProvider(),
                fileWriterFactory,
                hdfsEnvironment,
                hdfsContext,
                jsonCodec,
                session,
                split.getFileFormat()) : null;

        IcebergPageSink updateRowSink = isUpdateQuery ? new IcebergPageSink(
                icebergTable.schema(),
                icebergTable.spec(),
                icebergTable.locationProvider(),
                fileWriterFactory,
                pageIndexerFactory,
                hdfsEnvironment,
                hdfsContext,
                allTableColumns,
                jsonCodec,
                session,
                task.file().format(),
                maxOpenPartitions) : null;

        Optional<ReaderProjectionsAdapter> projectionsAdapter = dataPageSource.getReaderColumns().map(readerColumns ->
                new ReaderProjectionsAdapter(
                        fileReadColumns,
                        readerColumns,
                        column -> ((IcebergColumnHandle) column).getType(),
                        IcebergPageSourceProvider::applyProjection));

        return new IcebergPageSource(
                filePath, deleteFilter, dataPageSource.get(),
                queriedColumnPrefillValues, queriedColumnFileReadChannels,
                deleteColumnPrefillValues, deleteColumnFileReadChannels, deleteColumnTypes,
                nonUpdateColumnPrefillValues, nonUpdateColumnFileReadChannels, rowPositionChannel,
                allTableColumnChannels, posDeleteSink, updateRowSink, projectionsAdapter,
                isDeleteOrUpdateQuery, isUpdateQuery);
    }

    private void prefillPartitionValuesAndCompleteFileReadChannels(
            int idx,
            IcebergColumnHandle column,
            List<Integer> fileReadColumnIds,
            List<IcebergColumnHandle> fileReadColumns,
            Object[] prefillValues,
            int[] fileReadChannels,
            List<Boolean> isRowPositionChannel,
            Map<Integer, Optional<String>> partitionKeys,
            boolean isRowPosition)
    {
        if (partitionKeys.containsKey(column.getId())) {
            String partitionValue = partitionKeys.get(column.getId()).orElse(null);
            Type type = column.getType();
            Object nativeValue = deserializePartitionValue(type, partitionValue, column.getName());
            prefillValues[idx] = nativeValueToBlock(type, nativeValue);
            fileReadChannels[idx] = -1;
        }
        else {
            prefillValues[idx] = null;
            int fileReadChannel = fileReadColumnIds.indexOf(column.getId());
            if (fileReadChannel > -1) {
                fileReadChannels[idx] = fileReadChannel;
            }
            else {
                fileReadChannels[idx] = fileReadColumns.size();
                fileReadColumnIds.add(column.getId());
                fileReadColumns.add(column);
                isRowPositionChannel.add(isRowPosition);
            }
        }
    }

    private ReaderPageSource createDataPageSource(
            ConnectorSession session,
            HdfsContext hdfsContext,
            Path path,
            long start,
            long length,
            long fileSize,
            FileFormat fileFormat,
            List<IcebergColumnHandle> dataColumns,
            TupleDomain<IcebergColumnHandle> predicate,
            List<Boolean> isRowPositionChannel)
    {
        if (!isUseFileSizeFromMetadata(session)) {
            try {
                FileStatus fileStatus = hdfsEnvironment.doAs(session.getIdentity(),
                        () -> hdfsEnvironment.getFileSystem(hdfsContext, path).getFileStatus(path));
                fileSize = fileStatus.getLen();
            }
            catch (IOException e) {
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, e);
            }
        }

        switch (fileFormat) {
            case ORC:
                return createOrcPageSource(
                        hdfsEnvironment,
                        session.getIdentity(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        fileSize,
                        dataColumns,
                        predicate,
                        orcReaderOptions
                                .withMaxMergeDistance(getOrcMaxMergeDistance(session))
                                .withMaxBufferSize(getOrcMaxBufferSize(session))
                                .withStreamBufferSize(getOrcStreamBufferSize(session))
                                .withTinyStripeThreshold(getOrcTinyStripeThreshold(session))
                                .withMaxReadBlockSize(getOrcMaxReadBlockSize(session))
                                .withLazyReadSmallRanges(getOrcLazyReadSmallRanges(session))
                                .withNestedLazy(isOrcNestedLazy(session))
                                .withBloomFiltersEnabled(isOrcBloomFiltersEnabled(session)),
                        fileFormatDataSourceStats);
            case PARQUET:
                return createParquetPageSource(
                        hdfsEnvironment,
                        session.getIdentity(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        fileSize,
                        dataColumns,
                        parquetReaderOptions
                                .withMaxReadBlockSize(getParquetMaxReadBlockSize(session)),
                        predicate,
                        fileFormatDataSourceStats,
                        isRowPositionChannel);
            default:
                throw new TrinoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
        }
    }

    public static Optional<PartitionData> coercePartitionData(PartitionSpec newSpec, PartitionSpec spec, Optional<StructLike> partition)
    {
        // TODO: requires 0.13 StructProjection.createAllowMissing for the correct behavior
        StructProjection projection = StructProjection.create(new Schema(spec.partitionType().fields()), new Schema(newSpec.partitionType().fields()));
        projection.wrap(partition.orElse(null));
        PartitionData projectedPartition = null;
        if (!newSpec.isUnpartitioned()) {
            Object[] partitionValues = new Object[projection.size()];
            for (int i = 0; i < projection.size(); i++) {
                partitionValues[i] = projection.get(i, Object.class);
            }
            projectedPartition = new PartitionData(partitionValues);
        }
        return Optional.ofNullable(projectedPartition);
    }

    private static ReaderPageSource createOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorIdentity identity,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<IcebergColumnHandle> columns,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
            OrcReaderOptions options,
            FileFormatDataSourceStats stats)
    {
        OrcDataSource orcDataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(identity, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(identity, () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    options,
                    inputStream,
                    stats);

            OrcReader reader = OrcReader.createOrcReader(orcDataSource, options)
                    .orElseThrow(() -> new TrinoException(ICEBERG_BAD_DATA, "ORC file is zero length"));
            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            Map<Integer, OrcColumn> fileColumnsByIcebergId = mapIdsToOrcFileColumns(fileColumns);
            Map<String, OrcColumn> fileColumnsByName = null;
            if (fileColumnsByIcebergId.isEmpty()) {
                fileColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName().toLowerCase(ENGLISH));
            }

            TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder()
                    .setBloomFiltersEnabled(options.isBloomFiltersEnabled());
            Map<IcebergColumnHandle, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                    .orElseThrow(() -> new IllegalArgumentException("Effective predicate is none"));

            Optional<ReaderColumns> columnProjections = projectColumns(columns);
            Map<Integer, List<List<Integer>>> projectionsByFieldId = columns.stream()
                    .collect(groupingBy(
                            column -> column.getBaseColumnIdentity().getId(),
                            mapping(IcebergColumnHandle::getPath, toUnmodifiableList())));
            Map<String, List<List<Integer>>> projectionsByName = null;
            if (fileColumnsByIcebergId.isEmpty()) {
                projectionsByName = columns.stream()
                        .collect(groupingBy(
                                column -> column.getBaseColumnIdentity().getName(),
                                mapping(IcebergColumnHandle::getPath, toUnmodifiableList())));
            }

            List<IcebergColumnHandle> readColumns = columnProjections
                    .map(readerColumns -> (List<IcebergColumnHandle>) readerColumns.get().stream().map(IcebergColumnHandle.class::cast).collect(toImmutableList()))
                    .orElse(columns);
            List<OrcColumn> fileReadColumns = new ArrayList<>(readColumns.size());
            List<Type> fileReadTypes = new ArrayList<>(readColumns.size());
            List<ProjectedLayout> projectedLayouts = new ArrayList<>(readColumns.size());
            List<ColumnAdaptation> columnAdaptations = new ArrayList<>(readColumns.size());
            for (IcebergColumnHandle column : readColumns) {
                verify(column.isBaseColumn(), "Column projections must be based from a root column");

                OrcColumn orcColumn;
                if (fileColumnsByIcebergId.isEmpty()) {
                    orcColumn = fileColumnsByName.get(column.getName().toLowerCase(ENGLISH));
                }
                else {
                    orcColumn = fileColumnsByIcebergId.get(column.getId());
                }

                if (orcColumn != null) {
                    Type readType;
                    if (column.getType() == UUID) {
                        if (!"UUID".equals(orcColumn.getAttributes().get(ICEBERG_BINARY_TYPE))) {
                            throw new TrinoException(ICEBERG_BAD_DATA, format("Expected ORC column for UUID data to be annotated with %s=UUID: %s", ICEBERG_BINARY_TYPE, orcColumn));
                        }
                        // ORC spec doesn't have UUID
                        // TODO read into Int128ArrayBlock for better performance when operating on read values
                        readType = VARBINARY;
                    }
                    else {
                        readType = column.getType();
                    }

                    List<List<Integer>> fieldIdProjections = fileColumnsByIcebergId.isEmpty() ?
                            projectionsByName.get(column.getBaseColumnIdentity().getName()) :
                            projectionsByFieldId.get(column.getId());
                    ProjectedLayout projectedLayout = IcebergOrcProjectedLayout.createProjectedLayout(orcColumn, fieldIdProjections);

                    int sourceIndex = fileReadColumns.size();
                    columnAdaptations.add(ColumnAdaptation.sourceColumn(sourceIndex));
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(readType);
                    projectedLayouts.add(projectedLayout);

                    for (Map.Entry<IcebergColumnHandle, Domain> domainEntry : effectivePredicateDomains.entrySet()) {
                        IcebergColumnHandle predicateColumn = domainEntry.getKey();
                        OrcColumn predicateOrcColumn;
                        if (fileColumnsByIcebergId.isEmpty()) {
                            predicateOrcColumn = fileColumnsByName.get(predicateColumn.getName().toLowerCase(ENGLISH));
                        }
                        else {
                            predicateOrcColumn = fileColumnsByIcebergId.get(predicateColumn.getId());
                        }

                        if (predicateOrcColumn != null && column.getColumnIdentity().equals(predicateColumn.getBaseColumnIdentity())) {
                            predicateBuilder.addColumn(predicateOrcColumn.getColumnId(), domainEntry.getValue());
                        }
                    }
                }
                else {
                    columnAdaptations.add(ColumnAdaptation.nullColumn(column.getType()));
                }
            }

            AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
            OrcDataSourceId orcDataSourceId = orcDataSource.getId();
            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    projectedLayouts,
                    predicateBuilder.build(),
                    start,
                    length,
                    UTC,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleException(orcDataSourceId, exception),
                    fileColumnsByIcebergId.isEmpty()
                            ? NameBasedFieldMapper::create
                            : new IdBasedFieldMapperFactory(readColumns));

            return new ReaderPageSource(
                    new OrcPageSource(
                            recordReader,
                            columnAdaptations,
                            orcDataSource,
                            Optional.empty(),
                            Optional.empty(),
                            systemMemoryUsage,
                            stats),
                    columnProjections);
        }
        catch (Exception e) {
            if (orcDataSource != null) {
                try {
                    orcDataSource.close();
                }
                catch (IOException ignored) {
                }
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e instanceof BlockMissingException) {
                throw new TrinoException(ICEBERG_MISSING_DATA, message, e);
            }
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    /**
     * Gets the index based dereference chain to get from the readColumnHandle to the expectedColumnHandle
     */
    private static List<Integer> applyProjection(ColumnHandle expectedColumnHandle, ColumnHandle readColumnHandle)
    {
        IcebergColumnHandle expectedColumn = (IcebergColumnHandle) expectedColumnHandle;
        IcebergColumnHandle readColumn = (IcebergColumnHandle) readColumnHandle;
        checkState(readColumn.isBaseColumn(), "Read column path must be a base column");

        ImmutableList.Builder<Integer> dereferenceChain = ImmutableList.builder();
        ColumnIdentity columnIdentity = readColumn.getColumnIdentity();
        for (Integer fieldId : expectedColumn.getPath()) {
            ColumnIdentity nextChild = columnIdentity.getChildByFieldId(fieldId);
            dereferenceChain.add(columnIdentity.getChildIndexByFieldId(fieldId));
            columnIdentity = nextChild;
        }

        return dereferenceChain.build();
    }

    private static Map<Integer, OrcColumn> mapIdsToOrcFileColumns(List<OrcColumn> columns)
    {
        ImmutableMap.Builder<Integer, OrcColumn> columnsById = ImmutableMap.builder();
        Traverser.forTree(OrcColumn::getNestedColumns)
                .depthFirstPreOrder(columns)
                .forEach(column -> columnsById.put(getIcebergFieldId(column), column));
        return columnsById.build();
    }

    private static Integer getIcebergFieldId(OrcColumn column)
    {
        String icebergId = column.getAttributes().get(ORC_ICEBERG_ID_KEY);
        verify(icebergId != null, format("column %s does not have %s property", column, ORC_ICEBERG_ID_KEY));
        return Integer.valueOf(icebergId);
    }

    private static class IdBasedFieldMapperFactory
            implements OrcReader.FieldMapperFactory
    {
        // Stores a mapping between subfield names and ids for every top-level/nested column id
        private final Map<Integer, Map<String, Integer>> fieldNameToIdMappingForTableColumns;

        public IdBasedFieldMapperFactory(List<IcebergColumnHandle> columns)
        {
            requireNonNull(columns, "columns is null");

            ImmutableMap.Builder<Integer, Map<String, Integer>> mapping = ImmutableMap.builder();
            for (IcebergColumnHandle column : columns) {
                // Recursively compute subfield name to id mapping for every column
                populateMapping(column.getColumnIdentity(), mapping);
            }

            this.fieldNameToIdMappingForTableColumns = mapping.build();
        }

        @Override
        public OrcReader.FieldMapper create(OrcColumn column)
        {
            Map<Integer, OrcColumn> nestedColumns = Maps.uniqueIndex(
                    column.getNestedColumns(),
                    IcebergPageSourceProvider::getIcebergFieldId);

            int icebergId = getIcebergFieldId(column);
            return new IdBasedFieldMapper(nestedColumns, fieldNameToIdMappingForTableColumns.get(icebergId));
        }

        private static void populateMapping(
                ColumnIdentity identity,
                ImmutableMap.Builder<Integer, Map<String, Integer>> fieldNameToIdMappingForTableColumns)
        {
            List<ColumnIdentity> children = identity.getChildren();
            fieldNameToIdMappingForTableColumns.put(
                    identity.getId(),
                    children.stream()
                            // Lower casing is required here because ORC StructColumnReader does the same before mapping
                            .collect(toImmutableMap(child -> child.getName().toLowerCase(ENGLISH), ColumnIdentity::getId)));

            for (ColumnIdentity child : children) {
                populateMapping(child, fieldNameToIdMappingForTableColumns);
            }
        }
    }

    private static class IdBasedFieldMapper
            implements OrcReader.FieldMapper
    {
        private final Map<Integer, OrcColumn> idToColumnMappingForFile;
        private final Map<String, Integer> nameToIdMappingForTableColumns;

        public IdBasedFieldMapper(Map<Integer, OrcColumn> idToColumnMappingForFile, Map<String, Integer> nameToIdMappingForTableColumns)
        {
            this.idToColumnMappingForFile = requireNonNull(idToColumnMappingForFile, "idToColumnMappingForFile is null");
            this.nameToIdMappingForTableColumns = requireNonNull(nameToIdMappingForTableColumns, "nameToIdMappingForTableColumns is null");
        }

        @Override
        public OrcColumn get(String fieldName)
        {
            int fieldId = requireNonNull(
                    nameToIdMappingForTableColumns.get(fieldName),
                    () -> format("Id mapping for field %s not found", fieldName));
            return idToColumnMappingForFile.get(fieldId);
        }
    }

    private static ReaderPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorIdentity identity,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<IcebergColumnHandle> regularColumns,
            ParquetReaderOptions options,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            List<Boolean> isRowPositionChannel)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(identity, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(identity, () -> fileSystem.open(path));
            dataSource = new HdfsParquetDataSource(new ParquetDataSourceId(path.toString()), fileSize, inputStream, fileFormatDataSourceStats, options);
            ParquetDataSource theDataSource = dataSource; // extra variable required for lambda below
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(identity, () -> MetadataReader.readFooter(theDataSource));
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // Mapping from Iceberg field ID to Parquet fields.
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(toImmutableMap(field -> field.getId().intValue(), Function.identity()));

            // Map by name for a migrated table
            boolean mapByName = parquetIdToField.isEmpty();

            Pair<Optional<ReaderColumns>, List<Boolean>> projectResult = projectColumnsWithRowPositionChannel(regularColumns, isRowPositionChannel);
            Optional<ReaderColumns> columnProjections = projectResult.first();
            List<Boolean> projectedIsRowPositionChannel = projectResult.second();
            List<IcebergColumnHandle> readColumns = columnProjections
                    .map(readerColumns -> (List<IcebergColumnHandle>) readerColumns.get().stream().map(IcebergColumnHandle.class::cast).collect(toImmutableList()))
                    .orElse(regularColumns);

            List<org.apache.parquet.schema.Type> parquetFields = readColumns.stream()
                    .map(column -> {
                        if (mapByName) {
                            return getParquetTypeByName(column.getName(), fileSchema);
                        }
                        return parquetIdToField.get(column.getId());
                    })
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), parquetFields.stream().filter(Objects::nonNull).collect(toImmutableList()));
            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, UTC);

            long nextStart = 0;
            List<BlockMetaData> blocks = new ArrayList<>();
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (start <= firstDataPage && firstDataPage < start + length &&
                        predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain)) {
                    blocks.add(block);
                    blockStarts.add(nextStart);
                }
                nextStart += block.getRowCount();
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumnIO,
                    blocks,
                    Optional.of(blockStarts.build()),
                    dataSource,
                    UTC,
                    systemMemoryContext,
                    options);

            ImmutableList.Builder<Type> trinoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
                IcebergColumnHandle column = readColumns.get(columnIndex);
                org.apache.parquet.schema.Type parquetField = parquetFields.get(columnIndex);

                Type trinoType = column.getBaseType();

                trinoTypes.add(trinoType);

                if (parquetField == null) {
                    internalFields.add(Optional.empty());
                }
                else {
                    // The top level columns are already mapped by name/id appropriately.
                    ColumnIO columnIO = messageColumnIO.getChild(parquetField.getName());
                    internalFields.add(mapByName
                            ? HiveParquetColumnIOConverter.constructField(trinoType, columnIO)
                            : IcebergParquetColumnIOConverter.constructField(new FieldContext(trinoType, column.getColumnIdentity()), columnIO));
                }
            }

            return new ReaderPageSource(new ParquetPageSource(parquetReader, trinoTypes.build(), projectedIsRowPositionChannel, internalFields.build()), columnProjections);
        }
        catch (IOException | RuntimeException e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());

            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(ICEBERG_BAD_DATA, message, e);
            }

            if (e instanceof BlockMissingException) {
                throw new TrinoException(ICEBERG_MISSING_DATA, message, e);
            }
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static class IcebergOrcProjectedLayout
            implements ProjectedLayout
    {
        private final Map<Integer, ProjectedLayout> projectedLayoutForFieldId;

        private IcebergOrcProjectedLayout(Map<Integer, ProjectedLayout> projectedLayoutForFieldId)
        {
            this.projectedLayoutForFieldId = ImmutableMap.copyOf(requireNonNull(projectedLayoutForFieldId, "projectedLayoutForFieldId is null"));
        }

        public static ProjectedLayout createProjectedLayout(OrcColumn root, List<List<Integer>> fieldIdDereferences)
        {
            if (fieldIdDereferences.stream().anyMatch(List::isEmpty)) {
                return fullyProjectedLayout();
            }

            Map<Integer, List<List<Integer>>> dereferencesByField = fieldIdDereferences.stream().collect(
                    Collectors.groupingBy(
                            sequence -> sequence.get(0),
                            mapping(sequence -> sequence.subList(1, sequence.size()), toUnmodifiableList())));

            ImmutableMap.Builder<Integer, ProjectedLayout> fieldLayouts = ImmutableMap.builder();
            for (OrcColumn nestedColumn : root.getNestedColumns()) {
                Integer fieldId = getIcebergFieldId(nestedColumn);
                if (dereferencesByField.containsKey(fieldId)) {
                    fieldLayouts.put(fieldId, createProjectedLayout(nestedColumn, dereferencesByField.get(fieldId)));
                }
            }

            return new IcebergOrcProjectedLayout(fieldLayouts.build());
        }

        @Override
        public ProjectedLayout getFieldLayout(OrcColumn orcColumn)
        {
            int fieldId = getIcebergFieldId(orcColumn);
            return projectedLayoutForFieldId.getOrDefault(fieldId, fullyProjectedLayout());
        }
    }

    /**
     * Creates a mapping between the input {@param columns} and base columns if required.
     */
    public static Optional<ReaderColumns> projectColumns(List<IcebergColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        // No projection is required if all columns are base columns
        if (columns.stream().allMatch(IcebergColumnHandle::isBaseColumn)) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();
        Map<Integer, Integer> mappedFieldIds = new HashMap<>();
        int projectedColumnCount = 0;

        for (IcebergColumnHandle column : columns) {
            int baseColumnId = column.getBaseColumnIdentity().getId();
            Integer mapped = mappedFieldIds.get(baseColumnId);

            if (mapped == null) {
                projectedColumns.add(column.getBaseColumn());
                mappedFieldIds.put(baseColumnId, projectedColumnCount);
                outputColumnMapping.add(projectedColumnCount);
                projectedColumnCount++;
            }
            else {
                outputColumnMapping.add(mapped);
            }
        }

        return Optional.of(new ReaderColumns(projectedColumns.build(), outputColumnMapping.build()));
    }

    /**
     * Creates a mapping between the input {@param columns} and base columns if required.
     */
    public static Pair<Optional<ReaderColumns>, List<Boolean>> projectColumnsWithRowPositionChannel(
            List<IcebergColumnHandle> columns,
            List<Boolean> isRowPositionChannel)
    {
        requireNonNull(columns, "columns is null");
        requireNonNull(isRowPositionChannel, "isRowPositionChannel is null");

        // No projection is required if all columns are base columns
        if (columns.stream().allMatch(IcebergColumnHandle::isBaseColumn)) {
            return Pair.of(Optional.empty(), isRowPositionChannel);
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();
        ImmutableList.Builder<Boolean> projectedIsRowPositionChannel = ImmutableList.builder();
        Map<Integer, Integer> mappedFieldIds = new HashMap<>();
        int projectedColumnCount = 0;

        for (IcebergColumnHandle column : columns) {
            int baseColumnId = column.getBaseColumnIdentity().getId();
            Integer mapped = mappedFieldIds.get(baseColumnId);

            if (mapped == null) {
                projectedColumns.add(column.getBaseColumn());
                mappedFieldIds.put(baseColumnId, projectedColumnCount);
                outputColumnMapping.add(projectedColumnCount);
                projectedIsRowPositionChannel.add(column.isIcebergRowPositionMetadataColumn());
                projectedColumnCount++;
            }
            else {
                outputColumnMapping.add(mapped);
            }
        }

        Optional<ReaderColumns> readerColumns = Optional.of(new ReaderColumns(projectedColumns.build(), outputColumnMapping.build()));
        return Pair.of(readerColumns, projectedIsRowPositionChannel.build());
    }

    private static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getType().getTypeSignature().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
                if (descriptor != null) {
                    predicate.put(descriptor, domain);
                }
            }
        });
        return TupleDomain.withColumnDomains(predicate.build());
    }

    private static TrinoException handleException(OrcDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof OrcCorruptionException) {
            return new TrinoException(ICEBERG_BAD_DATA, exception);
        }
        return new TrinoException(ICEBERG_CURSOR_ERROR, format("Failed to read ORC file: %s", dataSourceId), exception);
    }
}
