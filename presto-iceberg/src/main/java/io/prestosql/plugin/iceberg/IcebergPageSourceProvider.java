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
import io.prestosql.plugin.hive.parquet.ParquetPageSource;
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
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.plugin.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static io.prestosql.plugin.hive.parquet.ParquetColumnIOConverter.constructField;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_DATA;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.getParquetMaxReadBlockSize;
import static io.prestosql.plugin.iceberg.IcebergSessionProperties.isFailOnCorruptedParquetStatistics;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit connectorSplit, ConnectorTableHandle connectorTable, List<ColumnHandle> columns)
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

        Path path = new Path(split.getPath());
        long start = split.getStart();
        long length = split.getLength();
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        ConnectorPageSource parquetPageSource = createParquetPageSource(
                hdfsEnvironment,
                session.getUser(),
                hdfsEnvironment.getConfiguration(hdfsContext, path),
                path,
                start,
                length,
                regularColumns,
                getParquetMaxReadBlockSize(session),
                isFailOnCorruptedParquetStatistics(session),
                split.getPredicate(),
                fileFormatDataSourceStats);

        return new IcebergPageSource(icebergColumns, partitionKeys, parquetPageSource, session.getTimeZoneKey());
    }

    private static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<IcebergColumnHandle> regularColumns,
            DataSize maxReadBlockSize,
            boolean failOnCorruptedStatistics,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
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
}
