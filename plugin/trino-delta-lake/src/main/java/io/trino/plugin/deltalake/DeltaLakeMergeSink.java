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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MergePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.DateTimeZone;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_WRITE;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getCompressionCodec;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetWriterPageSize;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DeltaLakeMergeSink
        implements ConnectorMergeSink
{
    private static final JsonCodec<List<String>> PARTITIONS_CODEC = listJsonCodec(String.class);

    private final TrinoFileSystem fileSystem;
    private final ConnectorSession session;
    private final DateTimeZone parquetDateTimeZone;
    private final String trinoVersion;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final DeltaLakeWriterStats writerStats;
    private final String rootTableLocation;
    private final ConnectorPageSink insertPageSink;
    private final List<DeltaLakeColumnHandle> dataColumns;
    private final int tableColumnCount;
    private final int domainCompactionThreshold;
    private final Map<Slice, FileDeletion> fileDeletions = new HashMap<>();

    public DeltaLakeMergeSink(
            TrinoFileSystemFactory fileSystemFactory,
            ConnectorSession session,
            DateTimeZone parquetDateTimeZone,
            String trinoVersion,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            DeltaLakeWriterStats writerStats,
            String rootTableLocation,
            ConnectorPageSink insertPageSink,
            List<DeltaLakeColumnHandle> tableColumns,
            int domainCompactionThreshold)
    {
        this.session = requireNonNull(session, "session is null");
        this.fileSystem = fileSystemFactory.create(session);
        this.parquetDateTimeZone = requireNonNull(parquetDateTimeZone, "parquetDateTimeZone is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.writerStats = requireNonNull(writerStats, "writerStats is null");
        this.rootTableLocation = requireNonNull(rootTableLocation, "rootTableLocation is null");
        this.insertPageSink = requireNonNull(insertPageSink, "insertPageSink is null");
        requireNonNull(tableColumns, "tableColumns is null");
        this.tableColumnCount = tableColumns.size();
        this.dataColumns = tableColumns.stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .collect(toImmutableList());
        this.domainCompactionThreshold = domainCompactionThreshold;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = createDeleteAndInsertPages(page, tableColumnCount);

        mergePage.getInsertionsPage().ifPresent(insertPageSink::appendPage);

        mergePage.getDeletionsPage().ifPresent(deletions -> {
            ColumnarRow rowIdRow = toColumnarRow(deletions.getBlock(deletions.getChannelCount() - 1));

            for (int position = 0; position < rowIdRow.getPositionCount(); position++) {
                Slice filePath = VARCHAR.getSlice(rowIdRow.getField(0), position);
                long rowPosition = BIGINT.getLong(rowIdRow.getField(1), position);
                Slice partitions = VARCHAR.getSlice(rowIdRow.getField(2), position);

                List<String> partitionValues = PARTITIONS_CODEC.fromJson(partitions.toStringUtf8());

                FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, x -> new FileDeletion(partitionValues));

                deletion.rowsToDelete().addLong(rowPosition);
            }
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>();

        insertPageSink.finish().join().stream()
                .map(Slice::getBytes)
                .map(dataFileInfoCodec::fromJson)
                .map(info -> new DeltaLakeMergeResult(Optional.empty(), Optional.of(info)))
                .map(mergeResultJsonCodec::toJsonBytes)
                .map(Slices::wrappedBuffer)
                .forEach(fragments::add);

        fileDeletions.forEach((path, deletion) ->
                fragments.addAll(rewriteFile(new Path(path.toStringUtf8()), deletion)));

        return completedFuture(fragments);
    }

    // In spite of the name "Delta" Lake, we must rewrite the entire file to delete rows.
    private List<Slice> rewriteFile(Path sourcePath, FileDeletion deletion)
    {
        try {
            Path rootTablePath = new Path(rootTableLocation);
            String sourceRelativePath = rootTablePath.toUri().relativize(sourcePath.toUri()).toString();

            Path targetPath = new Path(sourcePath.getParent(), session.getQueryId() + "_" + randomUUID());
            String targetRelativePath = rootTablePath.toUri().relativize(targetPath.toUri()).toString();
            FileWriter fileWriter = createParquetFileWriter(targetPath.toString(), dataColumns);

            DeltaLakeWriter writer = new DeltaLakeWriter(
                    fileSystem,
                    fileWriter,
                    rootTablePath,
                    targetRelativePath,
                    deletion.partitionValues(),
                    writerStats,
                    dataColumns);

            Optional<DataFileInfo> newFileInfo = rewriteParquetFile(sourcePath, deletion.rowsToDelete(), writer);

            DeltaLakeMergeResult result = new DeltaLakeMergeResult(Optional.of(sourceRelativePath), newFileInfo);
            return ImmutableList.of(utf8Slice(mergeResultJsonCodec.toJson(result)));
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to rewrite Parquet file", e);
        }
    }

    private FileWriter createParquetFileWriter(String path, List<DeltaLakeColumnHandle> dataColumns)
    {
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(getParquetWriterBlockSize(session))
                .setMaxPageSize(getParquetWriterPageSize(session))
                .build();
        CompressionCodecName compressionCodecName = getCompressionCodec(session).getParquetCompressionCodec();

        try {
            Closeable rollbackAction = () -> fileSystem.deleteFile(path);

            List<Type> parquetTypes = dataColumns.stream()
                    .map(column -> {
                        Type type = column.getType();
                        if (type instanceof TimestampWithTimeZoneType timestamp) {
                            verify(timestamp.getPrecision() == 3, "Unsupported type: %s", type);
                            return TIMESTAMP_MILLIS;
                        }
                        return type;
                    })
                    .collect(toImmutableList());

            List<String> dataColumnNames = dataColumns.stream()
                    .map(DeltaLakeColumnHandle::getName)
                    .collect(toImmutableList());
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    parquetTypes,
                    dataColumnNames,
                    false,
                    false);

            return new ParquetFileWriter(
                    fileSystem.newOutputFile(path),
                    rollbackAction,
                    parquetTypes,
                    dataColumnNames,
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    parquetWriterOptions,
                    IntStream.range(0, dataColumns.size()).toArray(),
                    compressionCodecName,
                    trinoVersion,
                    false,
                    Optional.empty(),
                    Optional.empty());
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Error creating Parquet file", e);
        }
    }

    private Optional<DataFileInfo> rewriteParquetFile(Path path, ImmutableLongBitmapDataProvider rowsToDelete, DeltaLakeWriter fileWriter)
            throws IOException
    {
        try (ConnectorPageSource connectorPageSource = createParquetPageSource(path).get()) {
            long filePosition = 0;
            while (!connectorPageSource.isFinished()) {
                Page page = connectorPageSource.getNextPage();
                if (page == null) {
                    continue;
                }

                int positionCount = page.getPositionCount();
                int[] retained = new int[positionCount];
                int retainedCount = 0;
                for (int position = 0; position < positionCount; position++) {
                    if (!rowsToDelete.contains(filePosition)) {
                        retained[retainedCount] = position;
                        retainedCount++;
                    }
                    filePosition++;
                }
                if (retainedCount != positionCount) {
                    page = page.getPositions(retained, 0, retainedCount);
                }

                if (page.getPositionCount() > 0) {
                    fileWriter.appendRows(page);
                }
            }
            if (fileWriter.getRowCount() == 0) {
                fileWriter.rollback();
                return Optional.empty();
            }
            fileWriter.commit();
        }
        catch (Throwable t) {
            try {
                fileWriter.rollback();
            }
            catch (RuntimeException e) {
                if (!t.equals(e)) {
                    t.addSuppressed(e);
                }
            }
            throw t;
        }

        return Optional.of(fileWriter.getDataFileInfo());
    }

    private ReaderPageSource createParquetPageSource(Path path)
            throws IOException
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(path.toString());

        return ParquetPageSourceFactory.createPageSource(
                inputFile,
                0,
                inputFile.length(),
                dataColumns.stream()
                        .map(DeltaLakeColumnHandle::toHiveColumnHandle)
                        .collect(toImmutableList()),
                TupleDomain.all(),
                true,
                parquetDateTimeZone,
                new FileFormatDataSourceStats(),
                new ParquetReaderOptions().withBloomFilter(false),
                Optional.empty(),
                domainCompactionThreshold);
    }

    private static class FileDeletion
    {
        private final List<String> partitionValues;
        private final LongBitmapDataProvider rowsToDelete = new Roaring64Bitmap();

        private FileDeletion(List<String> partitionValues)
        {
            // Use ArrayList since Delta Lake allows NULL partition values, and wrap it in
            // an unmodifiableList.
            this.partitionValues = unmodifiableList(new ArrayList<>(requireNonNull(partitionValues, "partitionValues is null")));
        }

        public List<String> partitionValues()
        {
            return partitionValues;
        }

        public LongBitmapDataProvider rowsToDelete()
        {
            return rowsToDelete;
        }
    }
}
