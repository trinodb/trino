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
package io.prestosql.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.predicate.Predicate;
import io.prestosql.parquet.reader.MetadataReader;
import io.prestosql.parquet.reader.ParquetReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveClientConfig;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSource;
import io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.prestosql.plugin.hive.HivePartitionKey;
import io.prestosql.plugin.hive.parquet.ParquetPageSource;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.prestosql.iceberg.IcebergUtil.SNAPSHOT_ID;
import static io.prestosql.iceberg.IcebergUtil.SNAPSHOT_TIMESTAMP_MS;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.prestosql.plugin.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.isFailOnCorruptedParquetStatistics;
import static io.prestosql.plugin.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory.getParquetType;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final HiveClientConfig hiveClientConfig;
    private FileFormatDataSourceStats fileFormatDataSourceStats;

    @Inject
    public IcebergPageSourceProvider(
            HiveClientConfig hiveClientConfig,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.hiveClientConfig = hiveClientConfig;
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeManager = typeManager;
        this.fileFormatDataSourceStats = fileFormatDataSourceStats;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        IcebergSplit icebergSplit = (IcebergSplit) split;
        Path path = new Path(icebergSplit.getPath());
        long start = icebergSplit.getStart();
        long length = icebergSplit.getLength();
        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());
        return createParquetPageSource(hdfsEnvironment,
                session.getUser(),
                hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, icebergSplit.getDatabase(), icebergSplit.getTable()), path),
                path,
                start,
                length,
                hiveColumns,
                icebergSplit.getNameToId(),
                hiveClientConfig.isUseParquetColumnNames(),
                typeManager,
                getParquetMaxReadBlockSize(session),
                isFailOnCorruptedParquetStatistics(session),
                icebergSplit.getEffectivePredicate(),
                icebergSplit.getPartitionKeys(),
                fileFormatDataSourceStats,
                icebergSplit.getSnapshotId(),
                icebergSplit.getSnapshotTimestamp());
    }

    public ConnectorPageSource createParquetPageSource(
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
            boolean failOnCorruptedParquetStatistics,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HivePartitionKey> partitionKeys,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            Long snapshotId,
            Long snapshotTimeStamp)
    {
        AggregatedMemoryContext systemMemoryContext = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            long fileSize = fileSystem.getFileStatus(path).getLen();
            dataSource = buildHdfsParquetDataSource(fileSystem, path, start, length, fileSize, fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(fileSystem, path, fileSize);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // We need to transform columns so they have the parquet column name and not table column name.
            // In order to make that transformation we need to pass the iceberg schema (not the hive schema) in split and
            // use that here to map from iceberg schema column name to ID, lookup parquet column with same ID and all of its children
            // and use the index of all those columns as requested schema.

            Map<String, HiveColumnHandle> parquetColumns = convertToParquetNames(columns, icebergNameToId, fileSchema);

            // TODO may be move away from streams as this code is executed for each split and streams are known to be slow.
            List<org.apache.parquet.schema.Type> fields = parquetColumns.values().stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(column, fileSchema, true)) // we always use parquet column names in case of iceberg.
                    .filter(Objects::nonNull)
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    blocks.add(block);
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            ParquetDataSource finalDataSource = dataSource;

            ListIterator<BlockMetaData> blockMetaDataListIterator = blocks.listIterator();
            while (blockMetaDataListIterator.hasNext()) {
                if (!predicateMatches(parquetPredicate, blockMetaDataListIterator.next(), finalDataSource, descriptorsByPath, parquetTupleDomain, failOnCorruptedParquetStatistics)) {
                    blockMetaDataListIterator.remove();
                }
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks,
                    dataSource,
                    systemMemoryContext,
                    maxReadBlockSize);

            ImmutableList.Builder<ColumnMapping> mappingBuilder = new ImmutableList.Builder<>();
            mappingBuilder.addAll(buildColumnMappings(partitionKeys,
                    columns.stream().filter(columnHandle -> !columnHandle.isHidden()).collect(toList()),
                    emptyList(),
                    Collections.emptyMap(),
                    path,
                    OptionalInt.empty()));

            setIfPresent(mappingBuilder, getColumnHandle(SNAPSHOT_ID, columns), String.valueOf(snapshotId));
            setIfPresent(mappingBuilder, getColumnHandle(SNAPSHOT_TIMESTAMP_MS, columns), String.valueOf(snapshotTimeStamp));

            // This transformation is solely done so columns that are renames can be read. ParquetPageSource tries to get
            // column type from column name and because the name in parquet file is different than the iceberg column name
            // it gets a null back. When it can't find a field it assumes that field is missing and just assigns a null block
            // for the whole field.
            List<HiveColumnHandle> columnNameReplaced = columns.stream()
                    .filter(c -> c.getColumnType() == REGULAR)
                    .map(c -> parquetColumns.containsKey(c.getName()) ? parquetColumns.get(c.getName()) : c)
                    .collect(toList());

            return new HivePageSource(
                    mappingBuilder.build(),
                    Optional.empty(),
                    DateTimeZone.UTC,
                    typeManager,
                    new ParquetPageSource(
                            parquetReader,
                            fileSchema,
                            messageColumnIO,
                            typeManager,
                            new Properties(),
                            columnNameReplaced,
                            effectivePredicate,
                            useParquetColumnNames));
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
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());

            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, message, e);
            }

            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    /**
     * This method maps the iceberg column names to corresponding parquet column names by matching their Ids rather then relying on name or index.
     * If no parquet fields have an id, this is a case of migrated table and the method just returns the same column name as the hive column name.
     * If any parquet field has an Id, it returns a column name that has the same icebergId. If no icebergId matches the given parquetId, it assumes
     * the column must have been added to table later and does not return any column name for that column.
     * @param columns iceberg columns
     * @param icebergNameToId
     * @param parquetSchema
     * @return Map from iceberg column names to column handles with replace column names.
     */
    private Map<String, HiveColumnHandle> convertToParquetNames(List<HiveColumnHandle> columns, Map<String, Integer> icebergNameToId, MessageType parquetSchema)
    {
        List<org.apache.parquet.schema.Type> fields = parquetSchema.getFields();
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

    private final Optional<HiveColumnHandle> getColumnHandle(String columnName, List<HiveColumnHandle> columns)
    {
        // Assumes the call is only made for columns that are always present
        return columns.stream().filter(columnHandle -> columnHandle.getName().equals(columnName)).findFirst();
    }

    private void setIfPresent(ImmutableList.Builder<ColumnMapping> mappingBuilder, Optional<HiveColumnHandle> columnHandle, String value)
    {
        if (columnHandle.isPresent()) {
            mappingBuilder.add(ColumnMapping.prefilled(columnHandle.get(), value, Optional.empty()));
        }
    }
}
