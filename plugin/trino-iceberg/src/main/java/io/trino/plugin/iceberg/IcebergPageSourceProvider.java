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
import io.trino.plugin.hive.orc.HdfsOrcDataSource;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.orc.OrcPageSource.ColumnAdaptation;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.HdfsParquetDataSource;
import io.trino.plugin.hive.parquet.HiveParquetColumnIOConverter;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.iceberg.IcebergParquetColumnIOConverter.FieldContext;
import io.trino.spi.TrinoException;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.iceberg.FileFormat;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcReader.ProjectedLayout.fullyProjectedLayout;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
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
import static io.trino.plugin.iceberg.TypeConverter.ICEBERG_BINARY_TYPE;
import static io.trino.plugin.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.orcReaderOptions = requireNonNull(orcReaderConfig, "orcReaderConfig is null").toOrcReaderOptions();
        this.parquetReaderOptions = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
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

        List<IcebergColumnHandle> icebergColumns = columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .collect(toImmutableList());

        Map<Integer, String> partitionKeys = split.getPartitionKeys();

        List<IcebergColumnHandle> regularColumns = columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .filter(column -> !partitionKeys.containsKey(column.getId()))
                .collect(toImmutableList());

        HdfsContext hdfsContext = new HdfsContext(session);
        ConnectorPageSource dataPageSource = createDataPageSource(
                session,
                hdfsContext,
                new Path(split.getPath()),
                split.getStart(),
                split.getLength(),
                split.getFileSize(),
                split.getFileFormat(),
                regularColumns,
                table.getUnenforcedPredicate());

        return new IcebergPageSource(icebergColumns, partitionKeys, dataPageSource, session.getTimeZoneKey());
    }

    private ConnectorPageSource createDataPageSource(
            ConnectorSession session,
            HdfsContext hdfsContext,
            Path path,
            long start,
            long length,
            long fileSize,
            FileFormat fileFormat,
            List<IcebergColumnHandle> dataColumns,
            TupleDomain<IcebergColumnHandle> predicate)
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
                        fileFormatDataSourceStats);
            default:
                throw new TrinoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
        }
    }

    private static ConnectorPageSource createOrcPageSource(
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
            Map<Integer, OrcColumn> fileColumnsByIcebergId = fileColumns.stream()
                    .filter(orcColumn -> orcColumn.getAttributes().containsKey(ORC_ICEBERG_ID_KEY))
                    .collect(toImmutableMap(
                            orcColumn -> Integer.valueOf(orcColumn.getAttributes().get(ORC_ICEBERG_ID_KEY)),
                            identity()));
            Map<String, OrcColumn> fileColumnsByName = null;
            if (fileColumnsByIcebergId.isEmpty()) {
                fileColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName().toLowerCase(ENGLISH));
            }

            TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder()
                    .setBloomFiltersEnabled(options.isBloomFiltersEnabled());
            Map<IcebergColumnHandle, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                    .orElseThrow(() -> new IllegalArgumentException("Effective predicate is none"));
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.size());
            List<Type> fileReadTypes = new ArrayList<>(columns.size());
            List<ColumnAdaptation> columnAdaptations = new ArrayList<>(columns.size());
            for (IcebergColumnHandle column : columns) {
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

                    int sourceIndex = fileReadColumns.size();
                    columnAdaptations.add(ColumnAdaptation.sourceColumn(sourceIndex));
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(readType);

                    Domain domain = effectivePredicateDomains.get(column);
                    if (domain != null) {
                        predicateBuilder.addColumn(orcColumn.getColumnId(), domain);
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
                    Collections.nCopies(fileReadColumns.size(), fullyProjectedLayout()),
                    predicateBuilder.build(),
                    start,
                    length,
                    UTC,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleException(orcDataSourceId, exception),
                    fileColumnsByIcebergId.isEmpty()
                            ? NameBasedFieldMapper::create
                            : new IdBasedFieldMapperFactory(columns));

            return new OrcPageSource(
                    recordReader,
                    columnAdaptations,
                    orcDataSource,
                    Optional.empty(),
                    Optional.empty(),
                    systemMemoryUsage,
                    stats);
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
                    field -> Integer.valueOf(field.getAttributes().get(ORC_ICEBERG_ID_KEY)));

            int icebergId = Integer.valueOf(column.getAttributes().get(ORC_ICEBERG_ID_KEY));
            return new IdBasedFieldMapper(nestedColumns, fieldNameToIdMappingForTableColumns.get(icebergId));
        }

        private static void populateMapping(
                ColumnIdentity identity,
                ImmutableMap.Builder<Integer, Map<String, Integer>> fieldNameToIdMappingForTableColumns)
        {
            fieldNameToIdMappingForTableColumns.put(
                    identity.getId(),
                    identity.getChildren().stream()
                            // Lower casing is required here because ORC StructColumnReader does the same before mapping
                            .collect(toImmutableMap(child -> child.getName().toLowerCase(ENGLISH), ColumnIdentity::getId)));

            for (ColumnIdentity child : identity.getChildren()) {
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

    private static ConnectorPageSource createParquetPageSource(
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
            FileFormatDataSourceStats fileFormatDataSourceStats)
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

            List<org.apache.parquet.schema.Type> parquetFields = regularColumns.stream()
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

            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (start <= firstDataPage && firstDataPage < start + length &&
                        predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain)) {
                    blocks.add(block);
                }
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumnIO,
                    blocks,
                    Optional.empty(),
                    dataSource,
                    UTC,
                    systemMemoryContext,
                    options);

            ImmutableList.Builder<Type> trinoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < regularColumns.size(); columnIndex++) {
                IcebergColumnHandle column = regularColumns.get(columnIndex);
                org.apache.parquet.schema.Type parquetField = parquetFields.get(columnIndex);

                Type trinoType = column.getType();

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

            return new ParquetPageSource(parquetReader, trinoTypes.build(), internalFields.build());
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
