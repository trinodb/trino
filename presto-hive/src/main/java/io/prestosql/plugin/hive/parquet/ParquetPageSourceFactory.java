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
package io.prestosql.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.parquet.Field;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.ParquetDataSourceId;
import io.prestosql.parquet.ParquetReaderOptions;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.predicate.Predicate;
import io.prestosql.parquet.reader.MetadataReader;
import io.prestosql.parquet.reader.ParquetReader;
import io.prestosql.plugin.hive.AcidInfo;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.ReaderProjections;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.prestosql.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.isFailOnCorruptedParquetStatistics;
import static io.prestosql.plugin.hive.HiveSessionProperties.isUseParquetColumnNames;
import static io.prestosql.plugin.hive.ReaderProjections.projectBaseColumns;
import static io.prestosql.plugin.hive.ReaderProjections.projectSufficientColumns;
import static io.prestosql.plugin.hive.parquet.ParquetColumnIOConverter.constructField;
import static io.prestosql.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

public class ParquetPageSourceFactory
        implements HivePageSourceFactory
{
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
    public Optional<ReaderPageSourceWithProjections> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo)
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
                session.getUser(),
                timeZone,
                stats,
                options.withFailOnCorruptedStatistics(isFailOnCorruptedParquetStatistics(session))
                        .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))));
    }

    /**
     * This method is available for other callers to use directly.
     */
    public static ReaderPageSourceWithProjections createPageSource(
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            boolean useColumnNames,
            HdfsEnvironment hdfsEnvironment,
            Configuration configuration,
            String user,
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
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.open(path));
            dataSource = new HdfsParquetDataSource(new ParquetDataSourceId(path.toString()), estimatedFileSize, inputStream, stats, options);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = projectSufficientColumns(columns)
                    .map(ReaderProjections::getReaderColumns)
                    .orElse(columns).stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getColumnType(column, fileSchema, useColumnNames))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);

            requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            messageColumn = getColumnIO(fileSchema, requestedSchema);

            ImmutableList.Builder<BlockMetaData> footerBlocks = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    footerBlocks.add(block);
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate, fileSchema, useColumnNames);

            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, timeZone);
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            for (BlockMetaData block : footerBlocks.build()) {
                if (predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, options.isFailOnCorruptedStatistics())) {
                    blocks.add(block);
                }
            }
            parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumn,
                    blocks.build(),
                    dataSource,
                    timeZone,
                    newSimpleAggregatedMemoryContext(),
                    options);
        }
        catch (Exception e) {
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
            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e instanceof BlockMissingException) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }

        Optional<ReaderProjections> readerProjections = projectBaseColumns(columns);
        List<HiveColumnHandle> baseColumns = readerProjections.map(ReaderProjections::getReaderColumns).orElse(columns);
        for (HiveColumnHandle column : baseColumns) {
            checkArgument(column.getColumnType() == REGULAR, "column type must be REGULAR: %s", column);
        }

        List<Optional<org.apache.parquet.schema.Type>> parquetFields = baseColumns.stream()
                .map(column -> getParquetType(column, fileSchema, useColumnNames))
                .map(Optional::ofNullable)
                .collect(toImmutableList());
        ImmutableList.Builder<Type> prestoTypes = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < baseColumns.size(); columnIndex++) {
            HiveColumnHandle column = baseColumns.get(columnIndex);
            Optional<org.apache.parquet.schema.Type> parquetField = parquetFields.get(columnIndex);

            prestoTypes.add(column.getBaseType());

            internalFields.add(parquetField.flatMap(field -> {
                String columnName = useColumnNames ? column.getBaseColumnName() : fileSchema.getFields().get(column.getBaseHiveColumnIndex()).getName();
                return constructField(column.getBaseType(), lookupColumnByName(messageColumn, columnName));
            }));
        }

        ConnectorPageSource parquetPageSource = new ParquetPageSource(parquetReader, prestoTypes.build(), internalFields.build());
        return new ReaderPageSourceWithProjections(parquetPageSource, readerProjections);
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
            type = new GroupType(type.getRepetition(), groupType.getName(), ImmutableList.of(type));
        }
        return Optional.of(new GroupType(baseType.getRepetition(), baseType.getName(), ImmutableList.of(type)));
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
        return TupleDomain.withColumnDomains(predicate.build());
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
