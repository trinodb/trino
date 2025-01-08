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
package io.trino.plugin.paimon;

import com.google.cloud.hadoop.repackaged.gcs.com.google.common.collect.ImmutableList;
import com.google.cloud.hadoop.repackaged.gcs.com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.TupleDomainOrcPredicate;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.GroupField;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.parquet.ParquetTypeTranslator;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalog;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalogFactory;
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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IndexFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.orc.OrcTypeTranslator.createCoercer;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Trino {@link ConnectorPageSourceProvider}.
 */
public class PaimonPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final PaimonTrinoCatalogFactory paimonTrinoCatalogFactory;

    @Inject
    public PaimonPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            PaimonTrinoCatalogFactory paimonTrinoCatalogFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        this.paimonTrinoCatalogFactory =
                requireNonNull(paimonTrinoCatalogFactory, "paimonTrinoCatalogFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats =
                requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
    }

    static TupleDomain<ColumnDescriptor> getParquetTupleDomain(
            MessageType fileSchema,
            List<PaimonColumnHandle> columns,
            TupleDomain<PaimonColumnHandle> predicates)
    {
        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        Map<Integer, ColumnDescriptor> columnDescriptorMap = new HashMap<>();

        fileSchema
                .getPaths()
                .forEach(
                        path -> {
                            org.apache.parquet.schema.Type type = fileSchema.getType(path[0]);
                            if (type.isPrimitive()) {
                                columnDescriptorMap.put(
                                        type.getId().intValue(),
                                        fileSchema.getColumnDescription(path));
                            }
                        });

        for (PaimonColumnHandle column : columns) {
            if (predicates.getDomains().isPresent()) {
                Domain domain = predicates.getDomains().get().get(column);
                if (domain != null && columnDescriptorMap.containsKey(column.getColumnId())) {
                    predicate.put(columnDescriptorMap.get(column.getColumnId()), domain);
                }
            }
        }

        return TupleDomain.withColumnDomains(predicate.build());
    }

    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        if (type instanceof io.trino.spi.type.RowType rowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<io.trino.spi.type.RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (int i = 0; i < fields.size(); i++) {
                io.trino.spi.type.RowType.Field rowField = fields.get(i);
                Optional<Field> field =
                        constructField(rowField.getType(), groupColumnIO.getChild(i));
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(
                        new GroupField(
                                type,
                                repetitionLevel,
                                definitionLevel,
                                required,
                                fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType mapType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField =
                    constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0));
            Optional<Field> valueField =
                    constructField(mapType.getValueType(), keyValueColumnIO.getChild(1));
            return Optional.of(
                    new GroupField(
                            type,
                            repetitionLevel,
                            definitionLevel,
                            required,
                            ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType arrayType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field =
                    constructField(
                            arrayType.getElementType(),
                            getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(
                    new GroupField(
                            type,
                            repetitionLevel,
                            definitionLevel,
                            required,
                            ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        return Optional.of(
                new PrimitiveField(
                        type,
                        required,
                        primitiveColumnIO.getColumnDescriptor(),
                        primitiveColumnIO.getId()));
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            // TODO: support dynamic filter
            DynamicFilter dynamicFilter)
    {
        PaimonTrinoCatalog paimonTrinoCatalog = paimonTrinoCatalogFactory.create(session.getIdentity());
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Table table = paimonTableHandle.tableWithDynamicOptions(paimonTrinoCatalog, session);
        return runWithContextClassLoader(
                () -> {
                    Optional<PaimonColumnHandle> rowId =
                            columns.stream()
                                    .map(PaimonColumnHandle.class::cast)
                                    .filter(PaimonColumnHandle::isRowId)
                                    .findFirst();
                    if (rowId.isPresent()) {
                        List<ColumnHandle> dataColumns =
                                columns.stream()
                                        .map(PaimonColumnHandle.class::cast)
                                        .filter(column -> !column.isRowId())
                                        .collect(Collectors.toList());
                        Set<String> rowIdFileds =
                                ((io.trino.spi.type.RowType) rowId.get().getTrinoType())
                                        .getFields().stream()
                                        .map(io.trino.spi.type.RowType.Field::getName)
                                        .map(Optional::get)
                                        .collect(Collectors.toSet());

                        HashMap<String, Integer> fieldToIndex = new HashMap<>();
                        for (int i = 0; i < dataColumns.size(); i++) {
                            PaimonColumnHandle paimonColumnHandle =
                                    (PaimonColumnHandle) dataColumns.get(i);
                            if (rowIdFileds.contains(paimonColumnHandle.getColumnName())) {
                                fieldToIndex.put(paimonColumnHandle.getColumnName(), i);
                            }
                        }
                        return PaimonMergePageSourceWrapper.wrap(
                                createPageSource(
                                        session,
                                        table,
                                        paimonTableHandle.getPredicate(),
                                        (PaimonSplit) split,
                                        dataColumns,
                                        paimonTableHandle.getLimit()),
                                fieldToIndex);
                    }
                    else {
                        return createPageSource(
                                session,
                                table,
                                paimonTableHandle.getPredicate(),
                                (PaimonSplit) split,
                                columns,
                                paimonTableHandle.getLimit());
                    }
                },
                PaimonPageSourceProvider.class.getClassLoader());
    }

    private ConnectorPageSource createPageSource(
            ConnectorSession session,
            Table table,
            TupleDomain<PaimonColumnHandle> filter,
            PaimonSplit split,
            List<ColumnHandle> columns,
            OptionalLong limit)
    {
        RowType rowType = table.rowType();
        List<String> fieldNames = rowType.getFieldNames();
        List<String> projectedFields =
                columns.stream()
                        .map(PaimonColumnHandle.class::cast)
                        .map(PaimonColumnHandle::getColumnName)
                        .toList();

        List<PaimonColumnHandle> projectedColumns =
                columns.stream().map(PaimonColumnHandle.class::cast).toList();

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        Optional<Predicate> paimonFilter = new PaimonFilterConverter(rowType).convert(filter);
        Optional<ConnectorPageSource> pageSource = Optional.empty();

        try {
            Split paimonSplit = split.decodeSplit();
            Optional<List<RawFile>> optionalRawFiles = paimonSplit.convertToRawFiles();
            if (checkRawFile(optionalRawFiles)) {
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                boolean readIndex = fileStoreTable.coreOptions().fileIndexReadEnabled();

                Optional<List<DeletionFile>> deletionFiles = paimonSplit.deletionFiles();
                Optional<List<IndexFile>> indexFiles =
                        readIndex ? paimonSplit.indexFiles() : Optional.empty();

                try {
                    List<RawFile> files = optionalRawFiles.orElseThrow();
                    LinkedList<ConnectorPageSource> sources = new LinkedList<>();

                    // if file index exists, do the filter.
                    for (int i = 0; i < files.size(); i++) {
                        RawFile rawFile = files.get(i);
                        if (indexFiles.isPresent()) {
                            IndexFile indexFile = indexFiles.get().get(i);
                            if (indexFile != null && paimonFilter.isPresent()) {
                                try (FileIndexPredicate fileIndexPredicate =
                                        new FileIndexPredicate(
                                                new Path(indexFile.path()),
                                                ((FileStoreTable) table).fileIO(),
                                                rowType)) {
                                    if (!fileIndexPredicate.evaluate(paimonFilter.get()).remain()) {
                                        continue;
                                    }
                                }
                            }
                        }
                        ConnectorPageSource source =
                                createDataPageSource(
                                        rawFile.format(),
                                        fileSystem.newInputFile(Location.of(rawFile.path())),
                                        projectedColumns,
                                        filter);

                        if (deletionFiles.isPresent()) {
                            source =
                                    PaimonPageSourceWrapper.wrap(
                                            source,
                                            Optional.ofNullable(deletionFiles.get().get(i))
                                                    .map(
                                                            deletionFile -> {
                                                                try {
                                                                    return DeletionVector.read(
                                                                            fileStoreTable.fileIO(),
                                                                            deletionFile);
                                                                }
                                                                catch (IOException e) {
                                                                    throw new RuntimeException(e);
                                                                }
                                                            }));
                        }
                        sources.add(source);
                    }

                    pageSource = Optional.of(new DirectTrinoPageSource(sources, limit));
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            if (pageSource.isEmpty()) {
                int[] columnIndex =
                        projectedFields.stream().mapToInt(fieldNames::indexOf).toArray();

                // old read way
                ReadBuilder read = table.newReadBuilder();
                paimonFilter.ifPresent(read::withFilter);

                if (!fieldNames.equals(projectedFields)) {
                    read.withProjection(columnIndex);
                }

                pageSource = Optional.of(new PaimonPageSource(
                        read.newRead().executeFilter().createReader(paimonSplit), columns, limit));
            }
            return pageSource.get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkRawFile(Optional<List<RawFile>> optionalRawFiles)
    {
        return optionalRawFiles.isPresent() && canUseTrinoPageSource(optionalRawFiles.get());
    }

    // TODO: support avro
    private boolean canUseTrinoPageSource(List<RawFile> rawFiles)
    {
        for (RawFile rawFile : rawFiles) {
            if (!(rawFile.format().equals("orc") || rawFile.format().equals("parquet"))) {
                return false;
            }
        }
        return true;
    }

    private ConnectorPageSource createDataPageSource(
            String format,
            TrinoInputFile inputFile,
            List<PaimonColumnHandle> columns,
            TupleDomain<PaimonColumnHandle> predicates)
    {
        return switch (format) {
            case "orc" -> createOrcDataPageSource(
                    inputFile,
                    // TODO: pass options to orc read from configuration
                    new OrcReaderOptions()
                            // Default tiny stripe size 8 M is too big for paimon.
                            // Cache stripe will cause more read (I want to read one column,
                            // but not the whole stripe)
                            .withTinyStripeThreshold(
                                    DataSize.of(4, DataSize.Unit.KILOBYTE)),
                    columns,
                    predicates);
            case "parquet" -> createParquetDataPageSource(
                    // TODO: pass options to parquet read from configuration
                    inputFile, new ParquetReaderOptions(), columns, predicates);
            case "avro" -> throw new RuntimeException("Unsupport file format: " + format);
            default -> throw new RuntimeException("Unsupport file format: " + format);
        };
    }

    private ConnectorPageSource createOrcDataPageSource(
            TrinoInputFile inputFile,
            OrcReaderOptions options,
            List<PaimonColumnHandle> columns,
            TupleDomain<PaimonColumnHandle> predicates)
    {
        try {
            OrcDataSource orcDataSource =
                    new PaimonOrcDataSource(inputFile, options, fileFormatDataSourceStats);
            OrcReader reader =
                    OrcReader.createOrcReader(orcDataSource, options)
                            .orElseThrow(() -> new RuntimeException("ORC file is zero length"));

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            Map<Integer, OrcColumn> fieldsMap = new HashMap<>();
            fileColumns.forEach(
                    column ->
                            fieldsMap.put(
                                    Integer.parseInt(column.getAttributes().get("paimon.id")),
                                    column));
            TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder predicateBuilder =
                    TupleDomainOrcPredicate.builder();
            List<OrcPageSource.ColumnAdaptation> columnAdaptations = new ArrayList<>();
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.size());
            List<Type> fileReadTypes = new ArrayList<>(columns.size());

            for (PaimonColumnHandle column : columns) {
                OrcColumn orcColumn = fieldsMap.get(column.getColumnId());
                if (orcColumn == null) {
                    columnAdaptations.add(
                            OrcPageSource.ColumnAdaptation.nullColumn(column.getTrinoType()));
                }
                else {
                    Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = createCoercer(orcColumn.getColumnType(), orcColumn.getNestedColumns(), column.getTrinoType());
                    if (coercer.isPresent()) {
                        fileReadTypes.add(coercer.get().getFromType());
                        columnAdaptations.add(
                                OrcPageSource.ColumnAdaptation.coercedColumn(
                                        fileReadColumns.size(), coercer.get()));
                    }
                    else {
                        fileReadTypes.add(column.getTrinoType());
                        columnAdaptations.add(
                                OrcPageSource.ColumnAdaptation.sourceColumn(fileReadColumns.size()));
                    }
                    fileReadColumns.add(orcColumn);
                    if (predicates.getDomains().isPresent()) {
                        Domain predicate = predicates.getDomains().get().get(column);
                        if (predicate != null) {
                            predicateBuilder.addColumn(orcColumn.getColumnId(), predicate);
                        }
                    }
                }
            }

            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            OrcRecordReader recordReader =
                    reader.createRecordReader(
                            fileReadColumns,
                            fileReadTypes,
                            predicateBuilder.build(),
                            DateTimeZone.UTC,
                            memoryUsage,
                            INITIAL_BATCH_SIZE,
                            RuntimeException::new);

            return new OrcPageSource(
                    recordReader,
                    columnAdaptations,
                    orcDataSource,
                    Optional.empty(),
                    Optional.empty(),
                    memoryUsage,
                    fileFormatDataSourceStats,
                    reader.getCompressionKind());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectorPageSource createParquetDataPageSource(
            TrinoInputFile inputFile,
            ParquetReaderOptions options,
            List<PaimonColumnHandle> columns,
            TupleDomain<PaimonColumnHandle> predicates)
    {
        try {
            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            ParquetDataSource dataSource =
                    createDataSource(
                            inputFile,
                            OptionalLong.of(inputFile.length()),
                            options,
                            memoryUsage,
                            fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata =
                    MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Map<Integer, org.apache.parquet.schema.Type> fileSchemaMap = new HashMap<>();
            for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
                fileSchemaMap.put(field.getId().intValue(), field);
            }
            List<org.apache.parquet.schema.Type> projectedTypes = new ArrayList<>();
            for (PaimonColumnHandle column : columns) {
                if (fileSchemaMap.containsKey(column.getColumnId())) {
                    projectedTypes.add(fileSchemaMap.get(column.getColumnId()));
                }
            }
            MessageType projectedSchema = new MessageType(fileSchema.getName(), projectedTypes);
            Map<List<String>, ColumnDescriptor> descriptorsByPath =
                    getDescriptors(fileSchema, projectedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain =
                    options.isIgnoreStatistics()
                            ? TupleDomain.all()
                            : getParquetTupleDomain(fileSchema, columns, predicates);
            TupleDomainParquetPredicate parquetPredicate =
                    buildPredicate(projectedSchema, parquetTupleDomain, descriptorsByPath, UTC);
            List<RowGroupInfo> rowGroups =
                    getFilteredRowGroups(
                            0,
                            inputFile.length(),
                            dataSource,
                            parquetMetadata,
                            ImmutableList.of(parquetTupleDomain),
                            ImmutableList.of(parquetPredicate),
                            descriptorsByPath,
                            UTC,
                            1000,
                            options);

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, projectedSchema);
            ParquetPageSource.Builder pageSourceBuilder = ParquetPageSource.builder();

            int parquetSourceChannel = 0;
            List<Column> returnColumns = new ArrayList<>();
            for (PaimonColumnHandle columnHandle : columns) {
                if (fileSchemaMap.containsKey(columnHandle.getColumnId())) {
                    org.apache.parquet.schema.Type type =
                            fileSchemaMap.get(columnHandle.getColumnId());
                    ColumnIO columnIO = messageColumnIO.getChild(type.getName());
                    Type trinoType = columnHandle.getTrinoType();
                    Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = Optional.empty();
                    if (type.isPrimitive()) {
                        coercer = ParquetTypeTranslator.createCoercer(type.asPrimitiveType().getPrimitiveTypeName(), type.getLogicalTypeAnnotation(), trinoType);
                    }
                    if (coercer.isPresent()) {
                        returnColumns.add(
                                new Column(
                                        columnHandle.getColumnName(),
                                        constructField(coercer.get().getFromType(), columnIO)
                                                .orElseThrow()));
                        pageSourceBuilder.addCoercedColumn(parquetSourceChannel++, coercer.get());
                    }
                    else {
                        returnColumns.add(
                                new Column(
                                        columnHandle.getColumnName(),
                                        constructField(columnHandle.getTrinoType(), columnIO)
                                                .orElseThrow()));
                        pageSourceBuilder.addSourceColumn(parquetSourceChannel++);
                    }
                }
                else {
                    pageSourceBuilder.addNullColumn(columnHandle.getTrinoType());
                }
            }

            ParquetReader parquetReader =
                    new ParquetReader(
                            Optional.ofNullable(fileMetaData.getCreatedBy()),
                            returnColumns,
                            rowGroups,
                            dataSource,
                            UTC,
                            memoryUsage,
                            options,
                            RuntimeException::new,
                            Optional.empty(),
                            Optional.empty());

            return pageSourceBuilder.build(parquetReader);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
