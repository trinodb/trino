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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.HiveTypeName;
import io.trino.plugin.hive.RecordFileWriter;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.plugin.hive.util.HiveWriteUtils;
import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_WRITE;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getCompressionCodec;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetWriterPageSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetOptimizedWriterEnabled;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.CompressionConfigUtil.configureCompression;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.common.FileUtils.escapePathName;

public class DeltaLakePageSink
        implements ConnectorPageSink
{
    private static final Logger LOG = Logger.get(DeltaLakePageSink.class);

    private static final int MAX_PAGE_POSITIONS = 4096;

    private final List<DeltaLakeColumnHandle> dataColumnHandles;
    private final int[] dataColumnInputIndex;
    private final List<String> dataColumnNames;
    private final List<Type> dataColumnTypes;

    private final int[] partitionColumnsInputIndex;
    private final List<String> originalPartitionColumnNames;
    private final List<Type> partitionColumnTypes;

    private final PageIndexer pageIndexer;
    private final HdfsEnvironment hdfsEnvironment;

    private final int maxOpenWriters;

    private final JsonCodec<DataFileInfo> dataFileInfoCodec;

    private final List<DeltaLakeWriter> writers = new ArrayList<>();

    private final String outputPath;
    private final ConnectorSession session;
    private final DeltaLakeWriterStats stats;
    private final JobConf conf;
    private final TypeManager typeManager;
    private final String trinoVersion;

    private long writtenBytes;
    private long memoryUsage;

    public DeltaLakePageSink(
            List<DeltaLakeColumnHandle> inputColumns,
            List<String> originalPartitionColumns,
            PageIndexerFactory pageIndexerFactory,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenWriters,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            String outputPath,
            ConnectorSession session,
            DeltaLakeWriterStats stats,
            TypeManager typeManager,
            String trinoVersion)
    {
        requireNonNull(inputColumns, "inputColumns is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.maxOpenWriters = maxOpenWriters;
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");

        // determine the input index of the partition columns and data columns
        ImmutableList.Builder<Integer> partitionColumnInputIndex = ImmutableList.builder();
        ImmutableList.Builder<Integer> dataColumnsInputIndex = ImmutableList.builder();

        ImmutableList.Builder<Type> partitionColumnTypes = ImmutableList.builder();
        ImmutableList.Builder<String> originalPartitionColumnNames = ImmutableList.builder();

        ImmutableList.Builder<DeltaLakeColumnHandle> dataColumnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> dataColumnTypes = ImmutableList.builder();
        ImmutableList.Builder<String> dataColumnNames = ImmutableList.builder();

        Map<String, String> canonicalToOriginalPartitionColumns = originalPartitionColumns.stream()
                .collect(toImmutableMap(TransactionLogAccess::canonicalizeColumnName, identity()));
        // sample weight column is passed separately, so index must be calculated without this column
        for (int inputIndex = 0; inputIndex < inputColumns.size(); inputIndex++) {
            DeltaLakeColumnHandle column = inputColumns.get(inputIndex);
            switch (column.getColumnType()) {
                case PARTITION_KEY:
                    partitionColumnInputIndex.add(inputIndex);
                    originalPartitionColumnNames.add(canonicalToOriginalPartitionColumns.get(column.getName()));
                    partitionColumnTypes.add(column.getType());
                    break;
                case REGULAR:
                    dataColumnHandles.add(column);
                    dataColumnsInputIndex.add(inputIndex);
                    dataColumnNames.add(column.getName());
                    dataColumnTypes.add(column.getType());
                    break;
                case SYNTHESIZED:
                default:
                    throw new IllegalStateException("Unexpected column type: " + column.getColumnType());
            }
        }
        this.partitionColumnsInputIndex = Ints.toArray(partitionColumnInputIndex.build());
        this.dataColumnInputIndex = Ints.toArray(dataColumnsInputIndex.build());
        this.originalPartitionColumnNames = originalPartitionColumnNames.build();
        this.dataColumnHandles = dataColumnHandles.build();
        this.partitionColumnTypes = partitionColumnTypes.build();
        this.dataColumnNames = dataColumnNames.build();
        this.dataColumnTypes = dataColumnTypes.build();

        this.pageIndexer = pageIndexerFactory.createPageIndexer(this.partitionColumnTypes);

        this.outputPath = outputPath;
        this.session = requireNonNull(session, "session is null");
        this.stats = stats;

        Configuration conf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), new Path(outputPath));
        configureCompression(conf, getCompressionCodec(session));
        this.conf = toJobConf(conf);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        ListenableFuture<Collection<Slice>> result = hdfsEnvironment.doAs(session.getIdentity(), this::doFinish);
        return MoreFutures.toCompletableFuture(result);
    }

    private ListenableFuture<Collection<Slice>> doFinish()
    {
        ImmutableList.Builder<Slice> dataFileInfos = ImmutableList.builder();
        Optional<Exception> commitException = Optional.empty();
        for (DeltaLakeWriter writer : writers) {
            writer.commit();
            try {
                DataFileInfo dataFileInfo = writer.getDataFileInfo();
                dataFileInfos.add(wrappedBuffer(dataFileInfoCodec.toJsonBytes(dataFileInfo)));
            }
            catch (IOException e) {
                LOG.warn("exception '%s' while finishing write on %s", e, writer);
                commitException = Optional.of(e);
            }
        }
        if (commitException.isPresent()) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Error committing Parquet file to Delta Lake", commitException.get());
        }

        List<Slice> result = dataFileInfos.build();

        writtenBytes = writers.stream()
                .mapToLong(DeltaLakeWriter::getWrittenBytes)
                .sum();

        return Futures.immediateFuture(result);
    }

    @Override
    public void abort()
    {
        hdfsEnvironment.doAs(session.getIdentity(), this::doAbort);
    }

    private void doAbort()
    {
        Optional<Exception> rollbackException = Optional.empty();
        for (DeltaLakeWriter writer : writers) {
            // writers can contain nulls if an exception is thrown when doAppend expends the writer list
            if (writer != null) {
                try {
                    writer.rollback();
                }
                catch (Exception e) {
                    LOG.warn("exception '%s' while rollback on %s", e, writer);
                    rollbackException = Optional.of(e);
                }
            }
        }
        if (rollbackException.isPresent()) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Error rolling back write to Delta Lake", rollbackException.get());
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() > 0) {
            hdfsEnvironment.doAs(session.getIdentity(), () -> doAppend(page));
        }

        return NOT_BLOCKED;
    }

    private void doAppend(Page page)
    {
        while (page.getPositionCount() > MAX_PAGE_POSITIONS) {
            Page chunk = page.getRegion(0, MAX_PAGE_POSITIONS);
            page = page.getRegion(MAX_PAGE_POSITIONS, page.getPositionCount() - MAX_PAGE_POSITIONS);
            writePage(chunk);
        }

        writePage(page);
    }

    private void writePage(Page page)
    {
        int[] writerIndexes = getWriterIndexes(page);

        // position count for each writer
        int[] sizes = new int[writers.size()];
        for (int index : writerIndexes) {
            sizes[index]++;
        }

        // record which positions are used by which writer
        int[][] writerPositions = new int[writers.size()][];
        int[] counts = new int[writers.size()];

        for (int position = 0; position < page.getPositionCount(); position++) {
            int index = writerIndexes[position];

            int count = counts[index];
            if (count == 0) {
                writerPositions[index] = new int[sizes[index]];
            }
            writerPositions[index][count] = position;
            counts[index] = count + 1;
        }

        // invoke the writers
        Page dataPage = getDataPage(page);
        for (int index = 0; index < writerPositions.length; index++) {
            int[] positions = writerPositions[index];
            if (positions == null) {
                continue;
            }

            // If write is partitioned across multiple writers, filter page using dictionary blocks
            Page pageForWriter = dataPage;
            if (positions.length != dataPage.getPositionCount()) {
                verify(positions.length == counts[index]);
                pageForWriter = pageForWriter.getPositions(positions, 0, positions.length);
            }

            DeltaLakeWriter writer = writers.get(index);

            long currentWritten = writer.getWrittenBytes();
            long currentMemory = writer.getMemoryUsage();

            writer.appendRows(pageForWriter);

            writtenBytes += (writer.getWrittenBytes() - currentWritten);
            memoryUsage += (writer.getMemoryUsage() - currentMemory);
        }
    }

    private int[] getWriterIndexes(Page page)
    {
        Page partitionColumns = extractColumns(page, partitionColumnsInputIndex);
        int[] writerIndexes = pageIndexer.indexPage(partitionColumns);
        if (pageIndexer.getMaxIndex() >= maxOpenWriters) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Exceeded limit of %s open writers for partitions", maxOpenWriters));
        }

        // expand writers list to new size
        while (writers.size() <= pageIndexer.getMaxIndex()) {
            writers.add(null);
        }

        // create missing writers
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            if (writers.get(writerIndex) != null) {
                continue;
            }

            Path filePath = new Path(outputPath);

            List<String> partitionValues = createPartitionValues(partitionColumnTypes, partitionColumns, position);
            Optional<String> partitionName = Optional.empty();
            if (!originalPartitionColumnNames.isEmpty()) {
                String partName = makePartName(originalPartitionColumnNames, partitionValues);
                filePath = new Path(outputPath, partName);
                partitionName = Optional.of(partName);
            }

            String fileName = randomUUID().toString();
            filePath = new Path(filePath, fileName);

            FileWriter fileWriter;
            if (isParquetOptimizedWriterEnabled(session)) {
                fileWriter = createParquetFileWriter(filePath);
            }
            else {
                fileWriter = createRecordFileWriter(filePath);
            }

            Path rootTableLocation = new Path(outputPath);
            try {
                DeltaLakeWriter writer = new DeltaLakeWriter(
                        hdfsEnvironment.getFileSystem(session.getIdentity(), rootTableLocation, conf),
                        fileWriter,
                        rootTableLocation,
                        partitionName.map(partition -> new Path(partition, fileName).toString()).orElse(fileName),
                        partitionValues,
                        stats,
                        dataColumnHandles);

                writers.set(writerIndex, writer);
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to create writer for location: " + outputPath, e);
            }
        }
        verify(writers.size() == pageIndexer.getMaxIndex() + 1);
        verify(!writers.contains(null));

        return writerIndexes;
    }

    /**
     * Copy of {@link FileUtils#makePartName(List, List)} modified to preserve case of partition columns.
     */
    private static String makePartName(List<String> partitionColumns, List<String> partitionValues)
    {
        StringBuilder name = new StringBuilder();

        for (int i = 0; i < partitionColumns.size(); ++i) {
            if (i > 0) {
                name.append("/");
            }

            name.append(escapePathName(partitionColumns.get(i), null));
            name.append('=');
            name.append(escapePathName(partitionValues.get(i), null));
        }

        return name.toString();
    }

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        return HiveWriteUtils.createPartitionValues(partitionColumnTypes, partitionColumns, position).stream()
                .map(value -> value.equals(HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION) ? null : value)
                .collect(toList());
    }

    private FileWriter createParquetFileWriter(Path path)
    {
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(getParquetWriterBlockSize(session))
                .setMaxPageSize(getParquetWriterPageSize(session))
                .build();
        CompressionCodecName compressionCodecName = getCompressionCodec(session).getParquetCompressionCodec();

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getIdentity(), path, conf);
            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };

            List<Type> parquetTypes = dataColumnTypes.stream()
                    .map(type -> {
                        if (type instanceof TimestampWithTimeZoneType) {
                            verify(((TimestampWithTimeZoneType) type).getPrecision() == 3, "Unsupported type: %s", type);
                            return TIMESTAMP_MILLIS;
                        }
                        return type;
                    })
                    .collect(toImmutableList());

            // we use identity column mapping; input page already contains only data columns per
            // DataLagePageSink.getDataPage()
            int[] identityMapping = new int[dataColumnTypes.size()];
            for (int i = 0; i < identityMapping.length; ++i) {
                identityMapping[i] = i;
            }

            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(parquetTypes, dataColumnNames);
            return new ParquetFileWriter(
                    fileSystem.create(path),
                    rollbackAction,
                    parquetTypes,
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    parquetWriterOptions,
                    identityMapping,
                    compressionCodecName,
                    trinoVersion);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Error creating Parquet file", e);
        }
    }

    private FileWriter createRecordFileWriter(Path path)
    {
        Properties schema = buildSchemaProperties(dataColumnNames, dataColumnTypes);
        return new RecordFileWriter(
                path,
                dataColumnNames,
                fromHiveStorageFormat(PARQUET),
                schema,
                PARQUET.getEstimatedWriterMemoryUsage(),
                conf,
                typeManager,
                DateTimeZone.UTC,
                session);
    }

    static Properties buildSchemaProperties(List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        schema.setProperty(IOConstants.COLUMNS, String.join(",", columnNames));
        schema.setProperty(IOConstants.COLUMNS_TYPES, columnTypes.stream()
                .map(DeltaHiveTypeTranslator::toHiveType)
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
    }

    private Page getDataPage(Page page)
    {
        Block[] blocks = new Block[dataColumnInputIndex.length];
        for (int i = 0; i < dataColumnInputIndex.length; i++) {
            int dataColumn = dataColumnInputIndex[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }
}
