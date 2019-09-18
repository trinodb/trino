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
import io.prestosql.parquet.Field;
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
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
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
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.prestosql.plugin.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static io.prestosql.plugin.hive.parquet.ParquetColumnIOConverter.constructField;
import static io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getParquetMaxReadBlockSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.isFailOnCorruptedParquetStatistics;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
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
        return createParquetPageSource(
                hdfsEnvironment,
                session.getUser(),
                hdfsEnvironment.getConfiguration(hdfsContext, path),
                path,
                start,
                length,
                hiveColumns,
                split.getNameToId(),
                typeManager,
                getParquetMaxReadBlockSize(session),
                isFailOnCorruptedParquetStatistics(session),
                split.getPredicate(),
                split.getPartitionKeys(),
                fileFormatDataSourceStats);
    }

    private static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            Map<String, Integer> icebergNameToId,
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
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            long fileSize = fileStatus.getLen();
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.open(path));
            dataSource = buildHdfsParquetDataSource(inputStream, path, fileSize, fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(fileSystem, path, fileSize);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            List<HiveColumnHandle> regularColumns = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .collect(toImmutableList());

            // Mapping from Iceberg field ID to Parquet fields.
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(toImmutableMap(field -> field.getId().intValue(), Function.identity()));

            List<org.apache.parquet.schema.Type> parquetFields = regularColumns.stream()
                    .map(column -> {
                        String columnName = column.getName();
                        Integer id = icebergNameToId.get(columnName);
                        checkArgument(id != null, "column name not in ID map: %s", columnName);

                        org.apache.parquet.schema.Type parquetField;
                        if (parquetIdToField.isEmpty()) {
                            // This is a migrated table
                            return getParquetTypeByName(columnName, fileSchema);
                        }

                        return parquetIdToField.get(id);
                    })
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), parquetFields.stream().filter(Objects::nonNull).collect(toImmutableList()));
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
                    OptionalInt.empty(),
                    fileSize,
                    fileStatus.getModificationTime());

            ImmutableList.Builder<Type> prestoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < regularColumns.size(); columnIndex++) {
                HiveColumnHandle column = regularColumns.get(columnIndex);
                org.apache.parquet.schema.Type parquetField = parquetFields.get(columnIndex);

                Type prestoType = typeManager.getType(column.getTypeSignature());

                prestoTypes.add(prestoType);

                if (parquetField == null) {
                    internalFields.add(Optional.empty());
                }
                else {
                    internalFields.add(constructField(prestoType, messageColumnIO.getChild(parquetField.getName())));
                }
            }

            ParquetPageSource parquetPageSource = new ParquetPageSource(parquetReader, prestoTypes.build(), internalFields.build());

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
}
