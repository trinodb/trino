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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeColumn;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSource;
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
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

/**
 * PageSourceProvider for Ducklake connector.
 * Leverages Trino's ParquetPageSource for all Parquet reading logic.
 */
public class DucklakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(DucklakePageSourceProvider.class);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final DucklakeCatalog catalog;

    @Inject
    public DucklakePageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderOptions parquetReaderOptions,
            DucklakeCatalog catalog)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        if (split instanceof DucklakeInlinedSplit inlinedSplit) {
            return createInlinedPageSource(inlinedSplit, columns);
        }

        DucklakeSplit ducklakeSplit = (DucklakeSplit) split;

        // Combine file statistics domain with dynamic filter for effective predicate
        TupleDomain<DucklakeColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(DucklakeColumnHandle.class::cast);
        TupleDomain<DucklakeColumnHandle> effectivePredicate = ducklakeSplit.fileStatisticsDomain()
                .intersect(dynamicFilterPredicate);

        if (effectivePredicate.isNone()) {
            return new EmptyPageSource();
        }

        // Extract column information
        List<DucklakeColumnHandle> ducklakeColumns = columns.stream()
                .map(DucklakeColumnHandle.class::cast)
                .collect(toImmutableList());

        log.debug("Creating page source for file: %s", ducklakeSplit.dataFilePath());

        try {
            // Get file system for the session
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);

            // Open the data file
            Location dataFileLocation = toLocation(ducklakeSplit.dataFilePath());
            TrinoInputFile inputFile = fileSystem.newInputFile(dataFileLocation);

            // Verify file format
            if (!"parquet".equalsIgnoreCase(ducklakeSplit.fileFormat())) {
                throw new IllegalArgumentException("Unsupported file format: " + ducklakeSplit.fileFormat());
            }

            // Create Parquet page source using Trino's infrastructure
            return createParquetPageSource(
                    inputFile,
                    ducklakeColumns,
                    ducklakeSplit,
                    effectivePredicate,
                    fileSystem);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create page source for file: " + ducklakeSplit.dataFilePath(), e);
        }
    }

    private ConnectorPageSource createInlinedPageSource(
            DucklakeInlinedSplit inlinedSplit,
            List<ColumnHandle> columns)
    {
        List<DucklakeColumnHandle> ducklakeColumns = columns.stream()
                .map(DucklakeColumnHandle.class::cast)
                .collect(toImmutableList());

        // Get the full column metadata to know column names for the SQL query
        List<DucklakeColumn> tableColumns = catalog.getTableColumns(
                inlinedSplit.tableId(), inlinedSplit.snapshotId());

        // Build ordered list of columns matching the requested projection
        Map<Long, DucklakeColumn> columnById = tableColumns.stream()
                .collect(toImmutableMap(DucklakeColumn::columnId, col -> col));

        List<DucklakeColumn> requestedColumns = ducklakeColumns.stream()
                .map(handle -> {
                    DucklakeColumn col = columnById.get(handle.columnId());
                    if (col == null) {
                        throw new IllegalStateException("Column not found in table metadata: " + handle.columnName());
                    }
                    return col;
                })
                .collect(toImmutableList());

        // Read inlined data from the metadata catalog
        List<List<Object>> rawRows = catalog.readInlinedData(
                inlinedSplit.tableId(),
                inlinedSplit.schemaVersion(),
                inlinedSplit.snapshotId(),
                requestedColumns);

        // Extract Trino types for each column
        List<Type> types = ducklakeColumns.stream()
                .map(DucklakeColumnHandle::columnType)
                .collect(toImmutableList());

        // Convert JDBC values to Trino-native values
        // InMemoryRecordSet expects null for null values in the row lists
        List<List<Object>> convertedRows = rawRows.stream()
                .map(row -> {
                    List<Object> converted = new java.util.ArrayList<>(row.size());
                    for (int i = 0; i < row.size(); i++) {
                        converted.add(DucklakeInlinedValueConverter.convertJdbcValue(row.get(i), types.get(i)));
                    }
                    return (List<Object>) converted;
                })
                .collect(toImmutableList());

        log.debug("Created inlined page source with %d rows for tableId=%d", rawRows.size(), inlinedSplit.tableId());

        InMemoryRecordSet recordSet = new InMemoryRecordSet(types, convertedRows);
        return new RecordPageSource(recordSet);
    }

    private ConnectorPageSource createParquetPageSource(
            TrinoInputFile inputFile,
            List<DucklakeColumnHandle> columns,
            DucklakeSplit split,
            TupleDomain<DucklakeColumnHandle> effectivePredicate,
            TrinoFileSystem fileSystem)
            throws IOException
    {
        // Create memory context for reading
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            // Create Parquet data source
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(split.fileSizeBytes()),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats);

            // Read Parquet metadata
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions.getMaxFooterReadSize(),
                    Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetadata.getSchema();
            ParquetDataSourceId dataSourceId = dataSource.getId();
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = toParquetTupleDomain(descriptorsByPath, effectivePredicate);
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC);
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    0,
                    split.fileSizeBytes(),
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    Domain.DEFAULT_COMPACTION_THRESHOLD,
                    parquetReaderOptions);

            // Build list of columns to read, handling missing columns for schema evolution
            ImmutableList.Builder<Column> parquetColumns = ImmutableList.builder();
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);
            TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
            int parquetColumnOrdinal = 0;

            for (DucklakeColumnHandle column : columns) {
                String columnName = column.columnName();
                ColumnIO columnIO = messageColumnIO.getChild(columnName);

                if (columnIO == null) {
                    // Missing column in file — return nulls (schema evolution)
                    transforms.constantValue(column.columnType().createNullBlock());
                    continue;
                }

                Optional<Field> field = DucklakeParquetTypeUtils.constructField(
                        column.columnType(),
                        columnIO);
                if (field.isEmpty()) {
                    // Could not construct field — return nulls
                    transforms.constantValue(column.columnType().createNullBlock());
                    continue;
                }

                parquetColumns.add(new Column(columnName, field.get()));
                transforms.column(parquetColumnOrdinal);
                parquetColumnOrdinal++;
            }

            List<Column> presentColumns = parquetColumns.build();

            // Create ParquetReader with only the columns present in the file
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    presentColumns,
                    false, // appendRowNumberColumn
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    exception -> handleParquetException(dataSourceId, exception),
                    parquetTupleDomain.isAll() ? Optional.empty() : Optional.of(parquetPredicate),
                    Optional.empty(), // bloomFilterStore
                    Optional.empty()); // rowFilter

            // Wrap in ParquetPageSource, apply column transforms for missing columns,
            // then apply merge-on-read delete filtering if present
            ConnectorPageSource pageSource = new ParquetPageSource(parquetReader);
            pageSource = transforms.build(pageSource);
            pageSource = applyDeleteFile(fileSystem, split, pageSource);

            log.debug("Created Parquet page source for %d columns from file: %s",
                    columns.size(), split.dataFilePath());

            return pageSource;
        }
        catch (IOException | RuntimeException e) {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException ex) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex);
                    }
                }
            }
            throw new RuntimeException("Failed to create Parquet page source for file: " + split.dataFilePath(), e);
        }
    }

    private ConnectorPageSource applyDeleteFile(TrinoFileSystem fileSystem, DucklakeSplit split, ConnectorPageSource dataSource)
            throws IOException
    {
        if (split.deleteFilePath().isEmpty()) {
            return dataSource;
        }

        Set<Long> deletedRows = readDeletedRows(fileSystem, split);
        if (deletedRows.isEmpty()) {
            return dataSource;
        }

        log.debug("Applying delete file %s with %d deleted rows for data file %s",
                split.deleteFilePath().orElseThrow(),
                deletedRows.size(),
                split.dataFilePath());
        return TransformConnectorPageSource.create(dataSource, new DeleteRowFilterTransform(deletedRows, split.rowIdStart()));
    }

    private Set<Long> readDeletedRows(TrinoFileSystem fileSystem, DucklakeSplit split)
            throws IOException
    {
        String deleteFilePath = split.deleteFilePath().orElseThrow();
        TrinoInputFile inputFile = fileSystem.newInputFile(toLocation(deleteFilePath));

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetDataSource dataSource = null;
        try {
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.empty(),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions.getMaxFooterReadSize(),
                    Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
            ParquetDataSourceId dataSourceId = dataSource.getId();
            MessageType fileSchema = fileMetadata.getSchema();
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);

            DeleteFileColumn deleteFileColumn = getDeleteFileColumn(fileSchema, messageColumnIO);
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = TupleDomain.all();
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC);
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    0,
                    inputFile.length(),
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    Domain.DEFAULT_COMPACTION_THRESHOLD,
                    parquetReaderOptions);

            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    ImmutableList.of(new Column(deleteFileColumn.columnName(), deleteFileColumn.field())),
                    false,
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    exception -> handleParquetException(dataSourceId, exception),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            Set<Long> deletedRows = new HashSet<>();
            try (ConnectorPageSource pageSource = new ParquetPageSource(parquetReader)) {
                while (!pageSource.isFinished()) {
                    SourcePage page = pageSource.getNextSourcePage();
                    if (page == null) {
                        continue;
                    }
                    Block block = page.getBlock(0);
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        if (block.isNull(position)) {
                            continue;
                        }
                        deletedRows.add(readDeleteValue(deleteFileColumn.columnType(), block, position));
                    }
                }
            }
            return deletedRows;
        }
        catch (IOException | RuntimeException e) {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException ex) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex);
                    }
                }
            }
            throw new RuntimeException("Failed to read delete file: " + deleteFilePath, e);
        }
    }

    private static DeleteFileColumn getDeleteFileColumn(MessageType fileSchema, MessageColumnIO messageColumnIO)
    {
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
            if (!field.isPrimitive()) {
                continue;
            }
            ColumnIO columnIO = messageColumnIO.getChild(field.getName());
            if (!(columnIO instanceof PrimitiveColumnIO primitiveColumnIO)) {
                continue;
            }

            PrimitiveTypeName primitiveTypeName = primitiveColumnIO.getPrimitive();
            Type columnType = switch (primitiveTypeName) {
                case INT64 -> BIGINT;
                case INT32 -> INTEGER;
                default -> null;
            };

            if (columnType != null) {
                Field fieldDefinition = DucklakeParquetTypeUtils.constructField(columnType, columnIO)
                        .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Could not construct field for delete file column: " + field.getName()));
                return new DeleteFileColumn(field.getName(), columnType, fieldDefinition);
            }
        }

        throw new TrinoException(NOT_SUPPORTED, "Delete file must contain at least one INT32/INT64 primitive column");
    }

    private static long readDeleteValue(Type type, Block block, int position)
    {
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        throw new IllegalArgumentException("Unsupported delete file value type: " + type);
    }

    private static TupleDomain<ColumnDescriptor> toParquetTupleDomain(
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<DucklakeColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }
        if (effectivePredicate.isAll()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        Map<String, ColumnDescriptor> topLevelDescriptors = descriptorsByPath.entrySet().stream()
                .filter(entry -> entry.getKey().size() == 1)
                .collect(toImmutableMap(
                        entry -> entry.getKey().get(0).toLowerCase(Locale.ENGLISH),
                        Map.Entry::getValue,
                        (first, _) -> first));

        Optional<Map<DucklakeColumnHandle, Domain>> domains = effectivePredicate.getDomains();
        if (domains.isEmpty()) {
            return TupleDomain.all();
        }

        for (Map.Entry<DucklakeColumnHandle, Domain> entry : domains.get().entrySet()) {
            DucklakeColumnHandle columnHandle = entry.getKey();
            ColumnDescriptor descriptor = topLevelDescriptors.get(columnHandle.columnName().toLowerCase(Locale.ENGLISH));
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }

        Map<ColumnDescriptor, Domain> parquetDomains = predicate.buildOrThrow();
        if (parquetDomains.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(parquetDomains);
    }

    private static Location toLocation(String path)
    {
        Location location = Location.of(path);
        if (location.scheme().isPresent()) {
            return location;
        }
        return Location.of(Path.of(path).toUri().toString());
    }

    private static RuntimeException handleParquetException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        return new TrinoException(
                NOT_SUPPORTED,
                "Error reading Parquet file: " + dataSourceId,
                exception);
    }

    private record DeleteFileColumn(String columnName, Type columnType, Field field) {}

    private static final class DeleteRowFilterTransform
            implements Function<SourcePage, SourcePage>
    {
        private final Set<Long> deletedRows;
        private final long rowIdStart;
        private long nextRowOffset;

        private DeleteRowFilterTransform(Set<Long> deletedRows, long rowIdStart)
        {
            this.deletedRows = Set.copyOf(requireNonNull(deletedRows, "deletedRows is null"));
            this.rowIdStart = rowIdStart;
        }

        @Override
        public SourcePage apply(SourcePage page)
        {
            int positionCount = page.getPositionCount();
            int[] retainedPositions = new int[positionCount];
            int retainedCount = 0;

            for (int position = 0; position < positionCount; position++) {
                long rowOffset = nextRowOffset + position;
                long rowId = rowIdStart + rowOffset;

                // Ducklake delete files conceptually store row ids. We also check row offsets to
                // tolerate producers that store file-local row index values.
                if (!deletedRows.contains(rowId) && !deletedRows.contains(rowOffset)) {
                    retainedPositions[retainedCount] = position;
                    retainedCount++;
                }
            }
            nextRowOffset += positionCount;

            if (retainedCount == positionCount) {
                return page;
            }
            page.selectPositions(retainedPositions, 0, retainedCount);
            return page;
        }
    }
}
