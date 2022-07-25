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
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.parquet.predicate.Predicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TrinoColumnIndexStore;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HivePageSourceProvider.projectSufficientColumns;
import static io.trino.plugin.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetIgnoreStatistics;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.hive.HiveSessionProperties.isUseParquetColumnNames;
import static io.trino.plugin.hive.parquet.HiveParquetColumnIOConverter.constructField;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

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
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();

    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final ParquetReaderOptions options;
    private final DateTimeZone timeZone;

    @Inject
    public ParquetPageSourceFactory(HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, ParquetReaderConfig config, HiveConfig hiveConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        requireNonNull(config, "config is null");

        options = config.toParquetReaderOptions();
        timeZone = requireNonNull(hiveConfig, "hiveConfig is null").getParquetDateTimeZone();
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
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

        return Optional.of(createPageSource(
                path,
                start,
                length,
                estimatedFileSize,
                columns,
                effectivePredicate,
                isUseParquetColumnNames(session),
                hdfsEnvironment,
                configuration,
                session.getIdentity(),
                timeZone,
                stats,
                options.withIgnoreStatistics(isParquetIgnoreStatistics(session))
                        .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                        .withUseColumnIndex(isParquetUseColumnIndex(session))));
    }

    /**
     * This method is available for other callers to use directly.
     */
    public static ReaderPageSource createPageSource(
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            boolean useColumnNames,
            HdfsEnvironment hdfsEnvironment,
            Configuration configuration,
            ConnectorIdentity identity,
            DateTimeZone timeZone,
            FileFormatDataSourceStats stats,
            ParquetReaderOptions options)
    {
        // Ignore predicates on partial columns for now.
        effectivePredicate = effectivePredicate.filter((column, domain) -> column.isBaseColumn());

        MessageType fileSchema;
        MessageType requestedSchema;
        MessageColumnIO messageColumn;
        ParquetReader parquetReader;
        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(identity, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(identity, () -> fileSystem.open(path));
            dataSource = new HdfsParquetDataSource(new ParquetDataSourceId(path.toString()), estimatedFileSize, inputStream, stats, options);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            fileSchema = fileMetaData.getSchema();

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

            requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            messageColumn = getColumnIO(fileSchema, requestedSchema);

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = options.isIgnoreStatistics()
                    ? TupleDomain.all()
                    : getParquetTupleDomain(descriptorsByPath, effectivePredicate, fileSchema, useColumnNames);

            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, timeZone);

            long nextStart = 0;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            ImmutableList.Builder<Optional<ColumnIndexStore>> columnIndexes = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                Optional<ColumnIndexStore> columnIndex = getColumnIndexStore(dataSource, block, descriptorsByPath, parquetTupleDomain, options);
                if (start <= firstDataPage && firstDataPage < start + length
                        && predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, columnIndex)) {
                    blocks.add(block);
                    blockStarts.add(nextStart);
                    columnIndexes.add(columnIndex);
                }
                nextStart += block.getRowCount();
            }
            parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumn,
                    blocks.build(),
                    blockStarts.build(),
                    dataSource,
                    timeZone,
                    newSimpleAggregatedMemoryContext(),
                    options,
                    parquetPredicate,
                    columnIndexes.build());
        }
        catch (Exception e) {
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
            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(HIVE_BAD_DATA, e);
            }
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e instanceof BlockMissingException) {
                throw new TrinoException(HIVE_MISSING_DATA, message, e);
            }
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }

        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);
        List<HiveColumnHandle> baseColumns = readerProjections.map(projection ->
                projection.get().stream()
                        .map(HiveColumnHandle.class::cast)
                        .collect(toUnmodifiableList()))
                .orElse(columns);

        for (HiveColumnHandle column : baseColumns) {
            checkArgument(column == PARQUET_ROW_INDEX_COLUMN || column.getColumnType() == REGULAR, "column type must be REGULAR: %s", column);
        }

        ImmutableList.Builder<Type> trinoTypes = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
        ImmutableList.Builder<Boolean> rowIndexColumns = ImmutableList.builder();
        for (HiveColumnHandle column : baseColumns) {
            trinoTypes.add(column.getBaseType());
            rowIndexColumns.add(column == PARQUET_ROW_INDEX_COLUMN);
            if (column == PARQUET_ROW_INDEX_COLUMN) {
                internalFields.add(Optional.empty());
            }
            else {
                internalFields.add(Optional.ofNullable(getParquetType(column, fileSchema, useColumnNames))
                        .flatMap(field -> {
                            String columnName = useColumnNames ? column.getBaseColumnName() : fileSchema.getFields().get(column.getBaseHiveColumnIndex()).getName();
                            return constructField(column.getBaseType(), lookupColumnByName(messageColumn, columnName));
                        }));
            }
        }

        ConnectorPageSource parquetPageSource = new ParquetPageSource(
                parquetReader,
                trinoTypes.build(),
                rowIndexColumns.build(),
                internalFields.build());
        return new ReaderPageSource(parquetPageSource, readerProjections);
    }

    public static Optional<org.apache.parquet.schema.Type> getParquetType(GroupType groupType, boolean useParquetColumnNames, HiveColumnHandle column)
    {
        if (useParquetColumnNames) {
            return Optional.ofNullable(getParquetTypeByName(column.getBaseColumnName(), groupType));
        }
        if (column.getBaseHiveColumnIndex() < groupType.getFieldCount()) {
            return Optional.of(groupType.getType(column.getBaseHiveColumnIndex()));
        }

        return Optional.empty();
    }

    public static Optional<org.apache.parquet.schema.Type> getColumnType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        Optional<org.apache.parquet.schema.Type> columnType = getParquetType(messageType, useParquetColumnNames, column);
        if (columnType.isEmpty() || column.getHiveColumnProjectionInfo().isEmpty()) {
            return columnType;
        }
        GroupType baseType = columnType.get().asGroupType();
        ImmutableList.Builder<org.apache.parquet.schema.Type> typeBuilder = ImmutableList.builder();
        org.apache.parquet.schema.Type parentType = baseType;

        for (String name : column.getHiveColumnProjectionInfo().get().getDereferenceNames()) {
            org.apache.parquet.schema.Type childType = getParquetTypeByName(name, parentType.asGroupType());
            if (childType == null) {
                return Optional.empty();
            }
            typeBuilder.add(childType);
            parentType = childType;
        }

        List<org.apache.parquet.schema.Type> subfieldTypes = typeBuilder.build();
        org.apache.parquet.schema.Type type = subfieldTypes.get(subfieldTypes.size() - 1);
        for (int i = subfieldTypes.size() - 2; i >= 0; --i) {
            GroupType groupType = subfieldTypes.get(i).asGroupType();
            type = new GroupType(groupType.getRepetition(), groupType.getName(), ImmutableList.of(type));
        }
        return Optional.of(new GroupType(baseType.getRepetition(), baseType.getName(), ImmutableList.of(type)));
    }

    private static Optional<ColumnIndexStore> getColumnIndexStore(
            ParquetDataSource dataSource,
            BlockMetaData blockMetadata,
            Map<List<String>, RichColumnDescriptor> descriptorsByPath,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            ParquetReaderOptions options)
    {
        if (!options.isUseColumnIndex() || parquetTupleDomain.isAll() || parquetTupleDomain.isNone()) {
            return Optional.empty();
        }

        boolean hasColumnIndex = false;
        for (ColumnChunkMetaData column : blockMetadata.getColumns()) {
            if (column.getColumnIndexReference() != null && column.getOffsetIndexReference() != null) {
                hasColumnIndex = true;
                break;
            }
        }

        if (!hasColumnIndex) {
            return Optional.empty();
        }

        Set<ColumnPath> columnsReadPaths = new HashSet<>(descriptorsByPath.size());
        for (List<String> path : descriptorsByPath.keySet()) {
            columnsReadPaths.add(ColumnPath.get(path.toArray(new String[0])));
        }

        Map<ColumnDescriptor, Domain> parquetDomains = parquetTupleDomain.getDomains()
                .orElseThrow(() -> new IllegalStateException("Predicate other than none should have domains"));
        Set<ColumnPath> columnsFilteredPaths = parquetDomains.keySet().stream()
                .map(column -> ColumnPath.get(column.getPath()))
                .collect(toImmutableSet());

        return Optional.of(new TrinoColumnIndexStore(dataSource, blockMetadata, columnsReadPaths, columnsFilteredPaths));
    }

    public static TupleDomain<ColumnDescriptor> getParquetTupleDomain(
            Map<List<String>, RichColumnDescriptor> descriptorsByPath,
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

            RichColumnDescriptor descriptor;
            if (useColumnNames) {
                descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
            }
            else {
                org.apache.parquet.schema.Type parquetField = getParquetType(columnHandle, fileSchema, false);
                if (parquetField == null || !parquetField.isPrimitive()) {
                    // Parquet file has fewer column than partition
                    // Or the field is a complex type
                    continue;
                }
                descriptor = descriptorsByPath.get(ImmutableList.of(parquetField.getName()));
            }
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    private static org.apache.parquet.schema.Type getParquetType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            return getParquetTypeByName(column.getBaseColumnName(), messageType);
        }

        if (column.getBaseHiveColumnIndex() < messageType.getFieldCount()) {
            return messageType.getType(column.getBaseHiveColumnIndex());
        }
        return null;
    }
}
