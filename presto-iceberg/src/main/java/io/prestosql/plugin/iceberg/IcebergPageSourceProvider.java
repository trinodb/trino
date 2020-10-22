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
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.TupleDomainOrcPredicate;
import io.prestosql.orc.TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder;
import io.prestosql.parquet.Field;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.ParquetDataSourceId;
import io.prestosql.parquet.ParquetReaderOptions;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.predicate.Predicate;
import io.prestosql.parquet.reader.MetadataReader;
import io.prestosql.parquet.reader.ParquetReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.plugin.hive.orc.OrcPageSource;
import io.prestosql.plugin.hive.orc.OrcPageSource.ColumnAdaptation;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.parquet.HdfsParquetDataSource;
import io.prestosql.plugin.hive.parquet.ParquetPageSource;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
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
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.plugin.hive.parquet.ParquetColumnIOConverter.constructField;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_CURSOR_ERROR;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_DATA;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcLazyReadSmallRanges;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcMaxBufferSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcMaxMergeDistance;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcMaxReadBlockSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcStreamBufferSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getOrcTinyStripeThreshold;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getParquetMaxReadBlockSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.isFailOnCorruptedParquetStatistics;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.isOrcBloomFiltersEnabled;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.isOrcNestedLazy;
import static io.prestosql.plugin.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
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
            TupleDomain<ColumnHandle> dynamicFilter)
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

        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        ConnectorPageSource dataPageSource = createDataPageSource(
                session,
                hdfsContext,
                new Path(split.getPath()),
                split.getStart(),
                split.getLength(),
                split.getFileFormat(),
                regularColumns,
                table.getPredicate());

        return new IcebergPageSource(icebergColumns, partitionKeys, dataPageSource, session.getTimeZoneKey());
    }

    private ConnectorPageSource createDataPageSource(
            ConnectorSession session,
            HdfsContext hdfsContext,
            Path path,
            long start,
            long length,
            FileFormat fileFormat,
            List<IcebergColumnHandle> dataColumns,
            TupleDomain<IcebergColumnHandle> predicate)
    {
        switch (fileFormat) {
            case ORC:
                FileStatus fileStatus = null;
                try {
                    fileStatus = hdfsEnvironment.doAs(session.getUser(), () -> hdfsEnvironment.getFileSystem(hdfsContext, path).getFileStatus(path));
                }
                catch (IOException e) {
                    throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, e);
                }
                long fileSize = fileStatus.getLen();
                return createOrcPageSource(
                        hdfsEnvironment,
                        session.getUser(),
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
                        session.getUser(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        dataColumns,
                        parquetReaderOptions
                                .withFailOnCorruptedStatistics(isFailOnCorruptedParquetStatistics(session))
                                .withMaxReadBlockSize(getParquetMaxReadBlockSize(session)),
                        predicate,
                        fileFormatDataSourceStats);
        }
        throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
    }

    private static ConnectorPageSource createOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
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
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    options,
                    inputStream,
                    stats);

            OrcReader reader = OrcReader.createOrcReader(orcDataSource, options)
                    .orElseThrow(() -> new PrestoException(ICEBERG_BAD_DATA, "ORC file is zero length"));
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
                Type readType = column.getType();
                if (orcColumn != null) {
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
                    columnAdaptations.add(ColumnAdaptation.nullColumn(readType));
                }
            }

            AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
            OrcDataSourceId orcDataSourceId = orcDataSource.getId();
            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    predicateBuilder.build(),
                    start,
                    length,
                    UTC,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleException(orcDataSourceId, exception));

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
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e instanceof BlockMissingException) {
                throw new PrestoException(ICEBERG_MISSING_DATA, message, e);
            }
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<IcebergColumnHandle> regularColumns,
            ParquetReaderOptions options,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FileStatus fileStatus = hdfsEnvironment.doAs(user, () -> fileSystem.getFileStatus(path));
            long estimatedFileSize = fileStatus.getLen();
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.open(path));
            dataSource = new HdfsParquetDataSource(new ParquetDataSourceId(path.toString()), estimatedFileSize, inputStream, fileFormatDataSourceStats, options);
            ParquetDataSource theDataSource = dataSource; // extra variable required for lambda below
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(user, () -> MetadataReader.readFooter(theDataSource));
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // Mapping from Iceberg field ID to Parquet fields.
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(toImmutableMap(field -> field.getId().intValue(), Function.identity()));

            List<org.apache.parquet.schema.Type> parquetFields = regularColumns.stream()
                    .map(column -> {
                        if (parquetIdToField.isEmpty()) {
                            // This is a migrated table
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
                if ((firstDataPage >= start) && (firstDataPage < (start + length)) &&
                        predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, options.isFailOnCorruptedStatistics())) {
                    blocks.add(block);
                }
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumnIO,
                    blocks,
                    dataSource,
                    UTC,
                    systemMemoryContext,
                    options);

            ImmutableList.Builder<Type> prestoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < regularColumns.size(); columnIndex++) {
                IcebergColumnHandle column = regularColumns.get(columnIndex);
                org.apache.parquet.schema.Type parquetField = parquetFields.get(columnIndex);

                Type prestoType = column.getType();

                prestoTypes.add(prestoType);

                if (parquetField == null) {
                    internalFields.add(Optional.empty());
                }
                else {
                    internalFields.add(constructField(column.getType(), messageColumnIO.getChild(parquetField.getName())));
                }
            }

            return new ParquetPageSource(parquetReader, prestoTypes.build(), internalFields.build());
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
                throw new PrestoException(ICEBERG_BAD_DATA, message, e);
            }

            if (e instanceof BlockMissingException) {
                throw new PrestoException(ICEBERG_MISSING_DATA, message, e);
            }
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, message, e);
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

    private static PrestoException handleException(OrcDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof PrestoException) {
            return (PrestoException) exception;
        }
        if (exception instanceof OrcCorruptionException) {
            return new PrestoException(ICEBERG_BAD_DATA, exception);
        }
        return new PrestoException(ICEBERG_CURSOR_ERROR, format("Failed to read ORC file: %s", dataSourceId), exception);
    }
}
