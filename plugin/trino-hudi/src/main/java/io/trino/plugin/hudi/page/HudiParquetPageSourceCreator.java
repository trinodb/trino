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

package io.trino.plugin.hudi.page;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.parquet.predicate.Predicate;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.parquet.HdfsParquetDataSource;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hudi.HudiConfig;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
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
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
import static io.trino.parquet.reader.MetadataReader.readFooter;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HivePageSourceProvider.projectSufficientColumns;
import static io.trino.plugin.hive.parquet.HiveParquetColumnIOConverter.constructField;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getColumnIndexStore;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getColumnType;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetType;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_MISSING_DATA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class HudiParquetPageSourceCreator
        extends HudiPageSourceCreator
{
    public static final String CONTEXT_KEY_PARQUET_READER_OPTIONS = "parquet_reader_options";
    private final ParquetReaderOptions options;

    public HudiParquetPageSourceCreator(
            HudiConfig hudiConfig, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats,
            DateTimeZone timeZone, Map<String, Object> context)
    {
        super(hudiConfig, hdfsEnvironment, stats, timeZone);
        this.options = (ParquetReaderOptions) requireNonNull(
                context.get(CONTEXT_KEY_PARQUET_READER_OPTIONS), "Parquet reader options are not present");
    }

    @Override
    public ConnectorPageSource createPageSource(
            Configuration configuration,
            ConnectorIdentity identity,
            List<HiveColumnHandle> regularColumns,
            HudiSplit hudiSplit)
    {
        ParquetDataSource dataSource = null;
        boolean useParquetColumnNames = hudiConfig.getUseParquetColumnNames();
        Path path = new Path(hudiSplit.getPath());
        long start = hudiSplit.getStart();
        long length = hudiSplit.getLength();
        long estimatedFileSize = hudiSplit.getFileSize();
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(identity, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(identity, () -> fileSystem.open(path));
            dataSource = new HdfsParquetDataSource(
                    new ParquetDataSourceId(path.toString()), estimatedFileSize, inputStream, stats, options);
            ParquetDataSource parquetDataSource = dataSource;
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(identity, () -> readFooter(parquetDataSource));
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = projectSufficientColumns(regularColumns)
                    .map(projection -> projection.get().stream()
                            .map(HiveColumnHandle.class::cast)
                            .collect(toUnmodifiableList()))
                    .orElse(regularColumns).stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getColumnType(column, fileSchema, useParquetColumnNames))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = options.isIgnoreStatistics()
                    ? TupleDomain.all()
                    : getParquetTupleDomain(
                            descriptorsByPath, hudiSplit.getPredicate(), fileSchema, useParquetColumnNames);

            Predicate parquetPredicate = buildPredicate(
                    requestedSchema, parquetTupleDomain, descriptorsByPath, timeZone);

            long nextStart = 0;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            ImmutableList.Builder<Optional<ColumnIndexStore>> columnIndexes = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                Optional<ColumnIndexStore> columnIndex = getColumnIndexStore(
                        dataSource, block, descriptorsByPath, parquetTupleDomain, options);
                if (start <= firstDataPage && firstDataPage < start + length
                        && predicateMatches(parquetPredicate, block, dataSource,
                        descriptorsByPath, parquetTupleDomain, columnIndex)) {
                    blocks.add(block);
                    blockStarts.add(nextStart);
                    columnIndexes.add(columnIndex);
                }
                nextStart += block.getRowCount();
            }

            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumn,
                    blocks.build(),
                    Optional.of(blockStarts.build()),
                    dataSource,
                    timeZone,
                    newSimpleAggregatedMemoryContext(),
                    options,
                    parquetPredicate,
                    columnIndexes.build());
            Optional<ReaderColumns> readerProjections = projectBaseColumns(regularColumns);
            List<HiveColumnHandle> baseColumns = readerProjections.map(projection ->
                            projection.get().stream()
                                    .map(HiveColumnHandle.class::cast)
                                    .collect(toUnmodifiableList()))
                    .orElse(regularColumns);

            for (HiveColumnHandle column : baseColumns) {
                checkArgument(column == PARQUET_ROW_INDEX_COLUMN
                        || column.getColumnType() == REGULAR, "column type must be REGULAR: %s", column);
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
                    internalFields.add(Optional.ofNullable(
                            getParquetType(column, fileSchema, useParquetColumnNames))
                            .flatMap(field -> {
                                String columnName = useParquetColumnNames
                                        ? column.getBaseColumnName()
                                        : fileSchema.getFields().get(column.getBaseHiveColumnIndex()).getName();
                                return constructField(column.getBaseType(), lookupColumnByName(messageColumn, columnName));
                            }));
                }
            }

            return new ParquetPageSource(
                    parquetReader,
                    trinoTypes.build(),
                    rowIndexColumns.build(),
                    internalFields.build());
        }
        catch (IOException | RuntimeException e) {
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
            String message = format("Error opening Hudi split %s (offset=%s, length=%s): %s",
                    path, start, length, e.getMessage());

            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(HUDI_BAD_DATA, message, e);
            }

            if (e instanceof BlockMissingException) {
                throw new TrinoException(HUDI_MISSING_DATA, message, e);
            }
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, message, e);
        }
    }
}
