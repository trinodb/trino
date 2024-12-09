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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
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
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
import static io.trino.plugin.hive.HivePageSourceProvider.getProjection;
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
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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

    public static boolean stripUnnecessaryProperties(String serializationLibraryName)
    {
        return PARQUET_SERDE_CLASS_NAMES.contains(serializationLibraryName);
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
        if (!PARQUET_SERDE_CLASS_NAMES.contains(schema.serializationLibraryName())) {
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
    public static ConnectorPageSource createPageSource(
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

            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetDataSource finalDataSource = dataSource;
            ParquetReaderProvider parquetReaderProvider = (fields, appendRowNumberColumn) -> new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    fields,
                    appendRowNumberColumn,
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
            return createParquetPageSource(columns, fileSchema, messageColumn, useColumnNames, parquetReaderProvider);
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
        Optional<MessageType> message = projectSufficientColumns(columns).stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .map(column -> getColumnType(column, fileSchema, useColumnNames))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(type -> new MessageType(fileSchema.getName(), type))
                .reduce(MessageType::union);
        return message;
    }

    /**
     * Creates a set of sufficient columns for the input projected columns and prepares a mapping between the two. For example,
     * if input columns include columns "a.b" and "a.b.c", then they will be projected from a single column "a.b".
     */
    @VisibleForTesting
    static List<HiveColumnHandle> projectSufficientColumns(List<HiveColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        if (columns.stream().allMatch(HiveColumnHandle::isBaseColumn)) {
            return columns;
        }

        ImmutableBiMap.Builder<DereferenceChain, HiveColumnHandle> dereferenceChainsBuilder = ImmutableBiMap.builder();

        for (HiveColumnHandle column : columns) {
            List<Integer> indices = column.getHiveColumnProjectionInfo()
                    .map(HiveColumnProjectionInfo::getDereferenceIndices)
                    .orElse(ImmutableList.of());

            DereferenceChain dereferenceChain = new DereferenceChain(column.getBaseColumnName(), indices);
            dereferenceChainsBuilder.put(dereferenceChain, column);
        }

        BiMap<DereferenceChain, HiveColumnHandle> dereferenceChains = dereferenceChainsBuilder.build();

        List<HiveColumnHandle> sufficientColumns = new ArrayList<>();
        Set<DereferenceChain> chosenColumns = new HashSet<>();

        // Pick a covering column for every column
        for (HiveColumnHandle columnHandle : columns) {
            DereferenceChain column = requireNonNull(dereferenceChains.inverse().get(columnHandle));
            List<DereferenceChain> orderedPrefixes = column.getOrderedPrefixes();

            // Shortest existing prefix is chosen as the input.
            DereferenceChain chosenColumn = null;
            for (DereferenceChain prefix : orderedPrefixes) {
                if (dereferenceChains.containsKey(prefix)) {
                    chosenColumn = prefix;
                    break;
                }
            }
            checkState(chosenColumn != null, "chosenColumn is null");

            if (chosenColumns.add(chosenColumn)) {
                sufficientColumns.add(dereferenceChains.get(chosenColumn));
            }
        }

        return sufficientColumns;
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
        ParquetReader createParquetReader(List<Column> fields, boolean appendRowNumberColumn)
                throws IOException;
    }

    public static ConnectorPageSource createParquetPageSource(
            List<HiveColumnHandle> columnHandles,
            MessageType fileSchema,
            MessageColumnIO messageColumn,
            boolean useColumnNames,
            ParquetReaderProvider parquetReaderProvider)
            throws IOException
    {
        List<Column> parquetColumnFieldsBuilder = new ArrayList<>(columnHandles.size());
        Map<String, Integer> baseColumnIdToOrdinal = new HashMap<>();
        TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
        boolean appendRowNumberColumn = false;
        for (HiveColumnHandle column : columnHandles) {
            if (column == PARQUET_ROW_INDEX_COLUMN) {
                appendRowNumberColumn = true;
                transforms.transform(new GetRowPositionFromSource());
                continue;
            }

            HiveColumnHandle baseColumn = column.getBaseColumn();
            Optional<org.apache.parquet.schema.Type> parquetType = getBaseColumnParquetType(baseColumn, fileSchema, useColumnNames);
            if (parquetType.isEmpty()) {
                transforms.constantValue(column.getBaseType().createNullBlock());
                continue;
            }
            String baseColumnName = useColumnNames ? baseColumn.getBaseColumnName() : fileSchema.getFields().get(baseColumn.getBaseHiveColumnIndex()).getName();

            Optional<TypeCoercer<?, ?>> coercer = Optional.empty();
            Integer ordinal = baseColumnIdToOrdinal.get(baseColumnName);
            if (ordinal == null) {
                ColumnIO columnIO = lookupColumnByName(messageColumn, baseColumnName);
                if (columnIO != null && columnIO.getType().isPrimitive()) {
                    PrimitiveType primitiveType = columnIO.getType().asPrimitiveType();
                    coercer = createCoercer(primitiveType.getPrimitiveTypeName(), primitiveType.getLogicalTypeAnnotation(), baseColumn.getBaseType());
                }
                io.trino.spi.type.Type readType = coercer.map(TypeCoercer::getFromType).orElseGet(baseColumn::getBaseType);

                Optional<Field> field = constructField(readType, columnIO);
                if (field.isEmpty()) {
                    transforms.constantValue(column.getType().createNullBlock());
                    continue;
                }

                ordinal = parquetColumnFieldsBuilder.size();
                parquetColumnFieldsBuilder.add(new Column(baseColumnName, field.get()));
                baseColumnIdToOrdinal.put(baseColumnName, ordinal);
            }

            if (column.isBaseColumn()) {
                transforms.column(ordinal, coercer.map(Function.identity()));
            }
            else {
                transforms.dereferenceField(
                        ImmutableList.<Integer>builder()
                                .add(ordinal)
                                .addAll(getProjection(column, baseColumn))
                                .build(),
                        coercer.map(Function.identity()));
            }
        }
        ParquetReader parquetReader = parquetReaderProvider.createParquetReader(parquetColumnFieldsBuilder, appendRowNumberColumn);
        ConnectorPageSource pageSource = new ParquetPageSource(parquetReader);
        return transforms.build(pageSource);
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

    private record GetRowPositionFromSource()
            implements Function<SourcePage, Block>
    {
        @Override
        public Block apply(SourcePage page)
        {
            return page.getBlock(page.getChannelCount() - 1);
        }
    }

    private record DereferenceChain(String name, List<Integer> indices)
    {
        private DereferenceChain
        {
            requireNonNull(name, "name is null");
            indices = ImmutableList.copyOf(requireNonNull(indices, "indices is null"));
        }

        /**
         * Get Prefixes of this Dereference chain in increasing order of lengths
         */
        public List<DereferenceChain> getOrderedPrefixes()
        {
            ImmutableList.Builder<DereferenceChain> prefixes = ImmutableList.builder();

            for (int prefixLen = 0; prefixLen <= indices.size(); prefixLen++) {
                prefixes.add(new DereferenceChain(name, indices.subList(0, prefixLen)));
            }

            return prefixes.build();
        }
    }
}
