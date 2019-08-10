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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.TupleDomainOrcPredicate;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.predicate.Predicate;
import io.prestosql.parquet.reader.MetadataReader;
import io.prestosql.parquet.reader.ParquetReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSource;
import io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.prestosql.plugin.hive.HivePartitionKey;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.plugin.hive.orc.OrcPageSource;
import io.prestosql.plugin.hive.parquet.ParquetPageSource;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static io.prestosql.plugin.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory.getParquetType;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getParquetMaxReadBlockSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.isFailOnCorruptedParquetStatistics;
import static io.prestosql.plugin.iceberg.IcebergUtil.ICEBERG_FIELD_ID_KEY;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;

    private final HiveConfig hiveConfig;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            HiveConfig hiveConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.hiveConfig = requireNonNull(hiveConfig, "hiveConfig is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit connectorSplit, ConnectorTableHandle connectorTable, List<ColumnHandle> columns)
    {
        IcebergSplit split = (IcebergSplit) connectorSplit;
        IcebergTableHandle table = (IcebergTableHandle) connectorTable;

        Path path = new Path(split.getPath());
        long start = split.getStart();
        long length = split.getLength();
        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        switch (split.getFileFormat()) {
            case PARQUET:
                return createParquetPageSource(
                        hdfsEnvironment,
                        session.getUser(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        hiveColumns,
                        split.getNameToId(),
                        false,
                        typeManager,
                        getParquetMaxReadBlockSize(session),
                        isFailOnCorruptedParquetStatistics(session),
                        split.getPredicate(),
                        split.getPartitionKeys(),
                        fileFormatDataSourceStats);
            case ORC:
                try {
                    FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, path);
                    FileStatus fileStatus = fileSystem.getFileStatus(path);
                    long fileSize = fileStatus.getLen();
                    return createOrcPageSource(
                            hdfsEnvironment,
                            session.getUser(),
                            hdfsEnvironment.getConfiguration(hdfsContext, path),
                            path,
                            start,
                            length,
                            fileSize,
                            hiveColumns,
                            split.getNameToId(),
                            split.getPredicate(),
                            hiveConfig.getDateTimeZone(),
                            typeManager,
                            getOrcMaxMergeDistance(session),
                            getOrcMaxBufferSize(session),
                            getOrcStreamBufferSize(session),
                            getOrcTinyStripeThreshold(session),
                            getOrcMaxReadBlockSize(session),
                            getOrcLazyReadSmallRanges(session),
                            isOrcBloomFiltersEnabled(session),
                            split.getPartitionKeys(),
                            fileFormatDataSourceStats);
                }
                catch (IOException e) {
                    throw new PrestoException(HIVE_FILESYSTEM_ERROR, e);
                }
        }
        throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + split.getFileFormat());
    }

    // TODO: move column rename handling out and reuse ParquetPageSourceFactory.createPageSource()
    private static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            Map<String, Integer> icebergNameToId,
            boolean useParquetColumnNames,
            TypeManager typeManager,
            DataSize maxReadBlockSize,
            boolean failOnCorruptedStatistics,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HivePartitionKey> partitionKeys,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            long fileSize = fileSystem.getFileStatus(path).getLen();
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.open(path));
            dataSource = buildHdfsParquetDataSource(inputStream, path, fileSize, fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(fileSystem, path, fileSize);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // We need to transform columns so they have the parquet column name and not table column name.
            // In order to make that transformation we need to pass the iceberg schema (not the hive schema)
            // in split and use that here to map from iceberg schema column name to ID, lookup parquet column
            // with same ID and all of its children and use the index of all those columns as requested schema.

            Map<String, HiveColumnHandle> parquetColumns = convertToParquetNames(columns, icebergNameToId, fileSchema);

            List<Type> fields = parquetColumns.values().stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(column, fileSchema, true)) // we always use parquet column names in case of iceberg.
                    .filter(Objects::nonNull)
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);
            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);

            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if ((firstDataPage >= start) && (firstDataPage < (start + length)) &&
                        predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, failOnCorruptedStatistics)) {
                    blocks.add(block);
                }
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks,
                    dataSource,
                    systemMemoryContext,
                    maxReadBlockSize);

            List<ColumnMapping> columnMappings = buildColumnMappings(
                    partitionKeys,
                    columns.stream()
                            .filter(column -> !column.isHidden())
                            .collect(toImmutableList()),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    path,
                    OptionalInt.empty());

            // This transformation is solely done so columns that are renames can be read.
            // ParquetPageSource tries to get column type from column name and because the
            // name in Parquet file is different than the Iceberg column name it gets a
            // null back. When it can't find a field it assumes that field is missing and
            // just assigns a null block for the whole field.
            List<HiveColumnHandle> columnNameReplaced = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> parquetColumns.getOrDefault(column.getName(), column))
                    .collect(toImmutableList());

            ParquetPageSource parquetPageSource = new ParquetPageSource(
                    parquetReader,
                    fileSchema,
                    messageColumnIO,
                    typeManager,
                    new Properties(),
                    columnNameReplaced,
                    effectivePredicate,
                    useParquetColumnNames);

            return new HivePageSource(
                    columnMappings,
                    Optional.empty(),
                    DateTimeZone.UTC,
                    typeManager,
                    parquetPageSource);
        }
        catch (IOException | RuntimeException e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());

            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, message, e);
            }

            if (e instanceof BlockMissingException) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    /**
     * This method maps the Iceberg column names to corresponding parquet column names
     * by matching their IDs rather than relying on name or index.
     *
     * If no Parquet fields have an ID, this is a case of migrated table and the method
     * just returns the same column name as the Hive column name.
     *
     * If any Parquet field has an ID, it returns a column name that has the same Iceberg ID.
     *
     * If no Iceberg ID matches the given parquet ID, it assumes the column must have been
     * added to table later and does not return any column name for that column.
     *
     * @param columns iceberg columns
     * @param icebergNameToId column name to id
     * @param parquetSchema parquet file schema
     * @return Map from iceberg column names to column handles with replace column names.
     */
    private static Map<String, HiveColumnHandle> convertToParquetNames(List<HiveColumnHandle> columns, Map<String, Integer> icebergNameToId, MessageType parquetSchema)
    {
        List<Type> fields = parquetSchema.getFields();
        ImmutableMap.Builder<String, HiveColumnHandle> builder = ImmutableMap.builder();
        Map<Integer, String> parquetIdToName = fields.stream()
                .filter(field -> field.getId() != null)
                .collect(Collectors.toMap((x) -> x.getId().intValue(), Type::getName));

        for (HiveColumnHandle column : columns) {
            if (!column.isHidden()) {
                String name = column.getName();
                Integer id = icebergNameToId.get(name);
                if (parquetIdToName.containsKey(id)) {
                    String parquetName = parquetIdToName.get(id);
                    HiveColumnHandle columnHandle = new HiveColumnHandle(parquetName, column.getHiveType(), column.getTypeSignature(), column.getHiveColumnIndex(), column.getColumnType(), column.getComment());
                    builder.put(name, columnHandle);
                }
                else {
                    if (parquetIdToName.isEmpty()) {
                        // a case of migrated tables so we just add the column as is.
                        builder.put(name, column);
                    }
                    else {
                        // this is not a migrated table but not parquet id matches. This could mean the column was added after this parquet file was created
                        // so we should ignore this column
                    }
                }
            }
        }
        return builder.build();
    }

    private static ConnectorPageSource createOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            Map<String, Integer> icebergNameToId,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize tinyStripeThreshold,
            DataSize maxReadBlockSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            List<HivePartitionKey> partitionKeys,
            FileFormatDataSourceStats stats)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    maxMergeDistance,
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage()));
        }

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        OrcPageSource orcPageSource;
        try {
            OrcReader reader = new OrcReader(orcDataSource, maxMergeDistance, tinyStripeThreshold, maxReadBlockSize);

            List<OrcType> flattenedOrcTypes = reader.getFooter().getTypes();
            OrcType rootType = flattenedOrcTypes.get(0);
            ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
            for (int physicalOrdinal = 0; physicalOrdinal < rootType.getFieldCount(); physicalOrdinal++) {
                int index = rootType.getFieldTypeIndex(physicalOrdinal);
                Map<String, String> attributes = flattenedOrcTypes.get(index).getAttributes();
                if (attributes.containsKey(ICEBERG_FIELD_ID_KEY)) {
                    builder.put(Integer.valueOf(attributes.get(ICEBERG_FIELD_ID_KEY)), physicalOrdinal);
                }
            }
            ImmutableMap<Integer, Integer> icebergIdToPhysicalOrdinal = builder.build();

            int nextMissingColumnIndex = rootType.getFieldCount();

            List<HiveColumnHandle> physicalColumns;
            if (icebergIdToPhysicalOrdinal.isEmpty()) {
                physicalColumns = columns.stream().filter(column -> column.getColumnType() == REGULAR).collect(toImmutableList());
            }
            else {
                ImmutableList.Builder<HiveColumnHandle> columnsBuilder = ImmutableList.builder();
                for (HiveColumnHandle column : columns) {
                    if (column.getColumnType() == REGULAR) {
                        int physicalOrdinal;
                        String name = column.getName();
                        Integer id = icebergNameToId.get(name);
                        if (icebergIdToPhysicalOrdinal.containsKey(id)) {
                            physicalOrdinal = icebergIdToPhysicalOrdinal.get(id);
                        }
                        else {
                            physicalOrdinal = nextMissingColumnIndex;
                            nextMissingColumnIndex++;
                        }
                        columnsBuilder.add(new HiveColumnHandle(column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.getColumnType(), column.getComment()));
                    }
                }
                physicalColumns = columnsBuilder.build();
            }

            ImmutableMap.Builder<Integer, io.prestosql.spi.type.Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<TupleDomainOrcPredicate.ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
            for (HiveColumnHandle column : physicalColumns) {
                if (column.getColumnType() == REGULAR) {
                    io.prestosql.spi.type.Type type = typeManager.getType(column.getTypeSignature());
                    includedColumns.put(column.getHiveColumnIndex(), type);
                    columnReferences.add(new TupleDomainOrcPredicate.ColumnReference<>(column, column.getHiveColumnIndex(), type));
                }
            }

            OrcPredicate predicate = new TupleDomainOrcPredicate<>(effectivePredicate, columnReferences.build(), orcBloomFiltersEnabled);

            OrcRecordReader recordReader = reader.createRecordReader(
                    includedColumns.build(),
                    predicate,
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE);

            orcPageSource = new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    physicalColumns,
                    typeManager,
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
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e instanceof BlockMissingException) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }

        List<ColumnMapping> columnMappings = buildColumnMappings(
                partitionKeys,
                columns.stream()
                        .filter(column -> !column.isHidden())
                        .collect(toImmutableList()),
                ImmutableList.of(),
                ImmutableMap.of(),
                path,
                OptionalInt.empty());

        return new HivePageSource(
                columnMappings,
                Optional.empty(),
                hiveStorageTimeZone,
                typeManager,
                orcPageSource);
    }
}
