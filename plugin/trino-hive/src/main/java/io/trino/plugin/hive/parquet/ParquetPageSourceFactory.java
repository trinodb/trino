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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.metastore.HiveType;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetWriteValidation;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metastore.type.Category.PRIMITIVE;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HivePageSourceProvider.projectSufficientColumns;
import static io.trino.plugin.hive.HiveSessionProperties.getParquetMaxReadBlockRowCount;
import static io.trino.plugin.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.hive.HiveSessionProperties.getParquetSmallFileThreshold;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetIgnoreStatistics;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetVectorizedDecodingEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isUseParquetColumnNames;
import static io.trino.plugin.hive.HiveSessionProperties.useParquetBloomFilter;
import static io.trino.plugin.hive.parquet.ParquetPageSource.handleException;
import static io.trino.plugin.hive.parquet.ParquetTypeTranslator.createCoercer;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class ParquetPageSourceFactory
        implements HivePageSourceFactory
{
    /**
     * If this object is passed as one of the columns for {@code createPageSource},
     * it will be populated as an additional column containing the index of each
     * row read.
     */
    public static final HiveColumnHandle PARQUET_ROW_INDEX_COLUMN = new HiveColumnHandle(
            "$parquet$row_index",
            -1, // no real column index
            HiveType.HIVE_LONG,
            BIGINT,
            Optional.empty(),
            HiveColumnHandle.ColumnType.SYNTHESIZED,
            Optional.empty());

    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add(PARQUET_HIVE_SERDE_CLASS)
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();

    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats stats;
    private final ParquetReaderOptions options;
    private final DateTimeZone timeZone;
    private final int domainCompactionThreshold;

    @Inject
    public ParquetPageSourceFactory(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats stats,
            ParquetReaderConfig config,
            HiveConfig hiveConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
        options = config.toParquetReaderOptions();
        timeZone = hiveConfig.getParquetDateTimeZone();
        domainCompactionThreshold = hiveConfig.getDomainCompactionThreshold();
    }

    public static Map<String, String> stripUnnecessaryProperties(Map<String, String> schema)
    {
        if (PARQUET_SERDE_CLASS_NAMES.contains(getDeserializerClassName(schema))) {
            return ImmutableMap.of(SERIALIZATION_LIB, schema.get(SERIALIZATION_LIB));
        }
        return schema;
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            Map<String, String> schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!PARQUET_SERDE_CLASS_NAMES.contains(getDeserializerClassName(schema))) {
            return Optional.empty();
        }

        checkArgument(acidInfo.isEmpty(), "Acid is not supported");

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(path, estimatedFileSize, Instant.ofEpochMilli(fileModifiedTime));

        return Optional.of(createPageSource(
                inputFile,
                start,
                length,
                columns,
                ImmutableList.of(effectivePredicate),
                isUseParquetColumnNames(session),
                timeZone,
                stats,
                options.withIgnoreStatistics(isParquetIgnoreStatistics(session))
                        .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                        .withMaxReadBlockRowCount(getParquetMaxReadBlockRowCount(session))
                        .withSmallFileThreshold(getParquetSmallFileThreshold(session))
                        .withUseColumnIndex(isParquetUseColumnIndex(session))
                        .withBloomFilter(useParquetBloomFilter(session))
                        .withVectorizedDecodingEnabled(isParquetVectorizedDecodingEnabled(session)),
                Optional.empty(),
                domainCompactionThreshold,
                OptionalLong.of(estimatedFileSize)));
    }

    /**
     * This method is available for other callers to use directly.
     */
    public static ReaderPageSource createPageSource(
            TrinoInputFile inputFile,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            List<TupleDomain<HiveColumnHandle>> disjunctTupleDomains,
            boolean useColumnNames,
            DateTimeZone timeZone,
            FileFormatDataSourceStats stats,
            ParquetReaderOptions options,
            Optional<ParquetWriteValidation> parquetWriteValidation,
            int domainCompactionThreshold,
            OptionalLong estimatedFileSize)
    {
        MessageType fileSchema;
        MessageType requestedSchema;
        MessageColumnIO messageColumn;
        ParquetDataSource dataSource = null;
        try {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            dataSource = createDataSource(inputFile, estimatedFileSize, options, memoryContext, stats);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, parquetWriteValidation);
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = getParquetMessageType(columns, useColumnNames, fileSchema);

            requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            messageColumn = getColumnIO(fileSchema, requestedSchema);

            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            List<TupleDomain<ColumnDescriptor>> parquetTupleDomains;
            List<TupleDomainParquetPredicate> parquetPredicates;
            if (options.isIgnoreStatistics()) {
                parquetTupleDomains = ImmutableList.of(TupleDomain.all());
                parquetPredicates = ImmutableList.of(buildPredicate(requestedSchema, TupleDomain.all(), descriptorsByPath, timeZone));
            }
            else {
                ImmutableList.Builder<TupleDomain<ColumnDescriptor>> parquetTupleDomainsBuilder = ImmutableList.builderWithExpectedSize(disjunctTupleDomains.size());
                ImmutableList.Builder<TupleDomainParquetPredicate> parquetPredicatesBuilder = ImmutableList.builderWithExpectedSize(disjunctTupleDomains.size());
                for (TupleDomain<HiveColumnHandle> tupleDomain : disjunctTupleDomains) {
                    TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, tupleDomain, fileSchema, useColumnNames);
                    parquetTupleDomainsBuilder.add(parquetTupleDomain);
                    parquetPredicatesBuilder.add(buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, timeZone));
                }
                parquetTupleDomains = parquetTupleDomainsBuilder.build();
                parquetPredicates = parquetPredicatesBuilder.build();
            }

            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    start,
                    length,
                    dataSource,
                    parquetMetadata.getBlocks(),
                    parquetTupleDomains,
                    parquetPredicates,
                    descriptorsByPath,
                    timeZone,
                    domainCompactionThreshold,
                    options);

            Optional<ReaderColumns> readerProjections = projectBaseColumns(columns, useColumnNames);
            List<HiveColumnHandle> baseColumns = readerProjections.map(projection ->
                            projection.get().stream()
                                    .map(HiveColumnHandle.class::cast)
                                    .collect(toUnmodifiableList()))
                    .orElse(columns);

            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetDataSource finalDataSource = dataSource;
            ParquetReaderProvider parquetReaderProvider = fields -> new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    fields,
                    rowGroups,
                    finalDataSource,
                    timeZone,
                    memoryContext,
                    options,
                    exception -> handleException(dataSourceId, exception),
                    // We avoid using disjuncts of parquetPredicate for page pruning in ParquetReader as currently column indexes
                    // are not present in the Parquet files which are read with disjunct predicates.
                    parquetPredicates.size() == 1 ? Optional.of(parquetPredicates.get(0)) : Optional.empty(),
                    parquetWriteValidation);
            ConnectorPageSource parquetPageSource = createParquetPageSource(baseColumns, fileSchema, messageColumn, useColumnNames, parquetReaderProvider);
            return new ReaderPageSource(parquetPageSource, readerProjections);
        }
        catch (Exception e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException _) {
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(HIVE_BAD_DATA, e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", inputFile.location(), start, length, e.getMessage());
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    public static ParquetDataSource createDataSource(
            TrinoInputFile inputFile,
            OptionalLong estimatedFileSize,
            ParquetReaderOptions options,
            AggregatedMemoryContext memoryContext,
            FileFormatDataSourceStats stats)
            throws IOException
    {
        if (estimatedFileSize.isEmpty() || estimatedFileSize.getAsLong() > options.getSmallFileThreshold().toBytes()) {
            return new TrinoParquetDataSource(inputFile, options, stats);
        }
        return new MemoryParquetDataSource(inputFile, memoryContext, stats);
    }

    public static Optional<MessageType> getParquetMessageType(List<HiveColumnHandle> columns, boolean useColumnNames, MessageType fileSchema)
    {
        Optional<MessageType> message = projectSufficientColumns(columns)
                .map(projection -> projection.get().stream()
                        .map(HiveColumnHandle.class::cast)
                        .collect(toUnmodifiableList()))
                .orElse(columns).stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .map(column -> getColumnType(column, fileSchema, useColumnNames))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(type -> new MessageType(fileSchema.getName(), type))
                .reduce(MessageType::union);
        return message;
    }

    public static Optional<org.apache.parquet.schema.Type> getColumnType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        Optional<org.apache.parquet.schema.Type> baseColumnType = getBaseColumnParquetType(column, messageType, useParquetColumnNames);
        if (baseColumnType.isEmpty() || column.getHiveColumnProjectionInfo().isEmpty()) {
            return baseColumnType;
        }
        GroupType baseType = baseColumnType.get().asGroupType();
        Optional<List<org.apache.parquet.schema.Type>> subFieldTypesOptional = dereferenceSubFieldTypes(baseType, column.getHiveColumnProjectionInfo().get());

        // if there is a mismatch between parquet schema and the hive schema and the column cannot be dereferenced
        if (subFieldTypesOptional.isEmpty()) {
            return Optional.empty();
        }

        List<org.apache.parquet.schema.Type> subfieldTypes = subFieldTypesOptional.get();
        org.apache.parquet.schema.Type type = subfieldTypes.getLast();
        for (int i = subfieldTypes.size() - 2; i >= 0; --i) {
            GroupType groupType = subfieldTypes.get(i).asGroupType();
            type = new GroupType(groupType.getRepetition(), groupType.getName(), ImmutableList.of(type));
        }
        return Optional.of(new GroupType(baseType.getRepetition(), baseType.getName(), ImmutableList.of(type)));
    }

    public static TupleDomain<ColumnDescriptor> getParquetTupleDomain(
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            MessageType fileSchema,
            boolean useColumnNames)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Entry<HiveColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            HiveColumnHandle columnHandle = entry.getKey();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (columnHandle.getHiveType().getCategory() != PRIMITIVE || columnHandle.getColumnType() != REGULAR) {
                continue;
            }

            ColumnDescriptor descriptor;

            Optional<org.apache.parquet.schema.Type> baseColumnType = getBaseColumnParquetType(columnHandle, fileSchema, useColumnNames);
            // Parquet file has fewer column than partition
            if (baseColumnType.isEmpty()) {
                continue;
            }

            if (baseColumnType.get().isPrimitive()) {
                descriptor = descriptorsByPath.get(ImmutableList.of(baseColumnType.get().getName()));
            }
            else {
                if (columnHandle.getHiveColumnProjectionInfo().isEmpty()) {
                    continue;
                }
                Optional<List<Type>> subfieldTypes = dereferenceSubFieldTypes(baseColumnType.get().asGroupType(), columnHandle.getHiveColumnProjectionInfo().get());
                // failed to look up subfields from the file schema
                if (subfieldTypes.isEmpty()) {
                    continue;
                }

                descriptor = descriptorsByPath.get(ImmutableList.<String>builder()
                        .add(baseColumnType.get().getName())
                        .addAll(subfieldTypes.get().stream().map(Type::getName).collect(toImmutableList()))
                        .build());
            }

            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    public interface ParquetReaderProvider
    {
        ParquetReader createParquetReader(List<Column> fields)
                throws IOException;
    }

    public static ConnectorPageSource createParquetPageSource(
            List<HiveColumnHandle> baseColumns,
            MessageType fileSchema,
            MessageColumnIO messageColumn,
            boolean useColumnNames,
            ParquetReaderProvider parquetReaderProvider)
            throws IOException
    {
        ParquetPageSource.Builder pageSourceBuilder = ParquetPageSource.builder();
        ImmutableList.Builder<Column> parquetColumnFieldsBuilder = ImmutableList.builder();
        int sourceChannel = 0;
        for (HiveColumnHandle column : baseColumns) {
            if (column == PARQUET_ROW_INDEX_COLUMN) {
                pageSourceBuilder.addRowIndexColumn();
                continue;
            }
            checkArgument(column.getColumnType() == REGULAR, "column type must be REGULAR: %s", column);
            Optional<org.apache.parquet.schema.Type> parquetType = getBaseColumnParquetType(column, fileSchema, useColumnNames);
            if (parquetType.isEmpty()) {
                pageSourceBuilder.addNullColumn(column.getBaseType());
                continue;
            }
            String columnName = useColumnNames ? column.getBaseColumnName() : fileSchema.getFields().get(column.getBaseHiveColumnIndex()).getName();

            Optional<TypeCoercer<?, ?>> coercer = Optional.empty();
            ColumnIO columnIO = lookupColumnByName(messageColumn, columnName);
            if (columnIO != null && columnIO.getType().isPrimitive()) {
                PrimitiveType primitiveType = columnIO.getType().asPrimitiveType();
                coercer = createCoercer(primitiveType.getPrimitiveTypeName(), primitiveType.getLogicalTypeAnnotation(), column.getBaseType());
            }

            io.trino.spi.type.Type readType = coercer.map(TypeCoercer::getFromType).orElseGet(column::getBaseType);

            Optional<Field> field = constructField(readType, columnIO);
            if (field.isEmpty()) {
                pageSourceBuilder.addNullColumn(readType);
                continue;
            }
            parquetColumnFieldsBuilder.add(new Column(columnName, field.get()));
            if (coercer.isPresent()) {
                pageSourceBuilder.addCoercedColumn(sourceChannel, coercer.get());
            }
            else {
                pageSourceBuilder.addSourceColumn(sourceChannel);
            }
            sourceChannel++;
        }

        return pageSourceBuilder.build(parquetReaderProvider.createParquetReader(parquetColumnFieldsBuilder.build()));
    }

    private static Optional<org.apache.parquet.schema.Type> getBaseColumnParquetType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            return Optional.ofNullable(getParquetTypeByName(column.getBaseColumnName(), messageType));
        }
        if (column.getBaseHiveColumnIndex() < messageType.getFieldCount()) {
            return Optional.of(messageType.getType(column.getBaseHiveColumnIndex()));
        }

        return Optional.empty();
    }

    /**
     * Dereferencing base parquet type based on projection info's dereference names.
     * For example, when dereferencing baseType(level1Field0, level1Field1, Level1Field2(Level2Field0, Level2Field1))
     * with a projection info's dereferenceNames list as (basetype, Level1Field2, Level2Field1).
     * It would return a list of parquet types in the order of (level1Field2, Level2Field1)
     *
     * @return child fields on each level of dereferencing. Return Optional.empty when failed to do the lookup.
     */
    private static Optional<List<org.apache.parquet.schema.Type>> dereferenceSubFieldTypes(GroupType baseType, HiveColumnProjectionInfo projectionInfo)
    {
        checkArgument(baseType != null, "base type cannot be null when dereferencing");
        checkArgument(projectionInfo != null, "hive column projection info cannot be null when doing dereferencing");

        ImmutableList.Builder<org.apache.parquet.schema.Type> typeBuilder = ImmutableList.builder();
        org.apache.parquet.schema.Type parentType = baseType;

        for (String name : projectionInfo.getDereferenceNames()) {
            org.apache.parquet.schema.Type childType = getParquetTypeByName(name, parentType.asGroupType());
            if (childType == null) {
                return Optional.empty();
            }
            typeBuilder.add(childType);
            parentType = childType;
        }

        return Optional.of(typeBuilder.build());
    }
}
