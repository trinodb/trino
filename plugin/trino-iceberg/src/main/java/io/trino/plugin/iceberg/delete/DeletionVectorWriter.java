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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.IcebergFileWriter;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.LongArrayBlock;
import org.apache.iceberg.ContentFileParsers;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.util.DeleteFileSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_CLOSE_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileFormat.PUFFIN;

public class DeletionVectorWriter
        implements IcebergFileWriter
{
    private static final int INSTANCE_SIZE = instanceSize(DeletionVectorWriter.class);

    private final DeletionVectorFileWriter writer;
    private final String dataFilePath;
    private final PartitionSpec partitionSpec;
    private final PartitionData partition;
    private final int positionChannel;
    private final Closeable rollbackAction;
    private DeleteWriteResult result;

    public DeletionVectorWriter(
            NodeVersion nodeVersion,
            TrinoFileSystem fileSystem,
            Location outputPath,
            String dataFilePath,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partition,
            Function<String, PositionDeleteIndex> loadPreviousDeletes,
            int positionChannel)
    {
        writer = new DeletionVectorFileWriter(nodeVersion, fileSystem, outputPath, loadPreviousDeletes);
        this.dataFilePath = requireNonNull(dataFilePath, "dataFilePath is null");
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.partition = requireNonNull(partition, "partition is null").orElse(null);
        checkArgument(positionChannel >= 0, "positionChannel is negative");
        this.positionChannel = positionChannel;
        rollbackAction = () -> fileSystem.deleteFile(outputPath);
    }

    public static Function<CharSequence, PositionDeleteIndex> create(TrinoFileSystem fileSystem, Map<String, DeleteFileSet> deleteFiles)
    {
        if (deleteFiles == null) {
            return _ -> null;
        }
        return new PreviousDeleteLoader(fileSystem, deleteFiles);
    }

    private static class PreviousDeleteLoader
            implements Function<CharSequence, PositionDeleteIndex>
    {
        private final Map<String, DeleteFileSet> deleteFiles;
        private final DeleteLoader deleteLoader;

        private PreviousDeleteLoader(TrinoFileSystem fileSystem, Map<String, DeleteFileSet> deleteFiles)
        {
            requireNonNull(fileSystem, "fileSystem is null");
            this.deleteFiles = ImmutableMap.copyOf(deleteFiles);
            this.deleteLoader = new BaseDeleteLoader(deleteFile -> new ForwardingInputFile(fileSystem.newInputFile(Location.of(deleteFile.location()))));
        }

        @Override
        public PositionDeleteIndex apply(CharSequence path)
        {
            DeleteFileSet deleteFileSet = deleteFiles.get(path.toString());
            if (deleteFileSet == null) {
                return null;
            }

            return deleteLoader.loadPositionDeletes(deleteFileSet, path);
        }
    }

    @Override
    public FileFormat fileFormat()
    {
        return PUFFIN;
    }

    @Override
    public String location()
    {
        return deleteFile().location();
    }

    @Override
    public List<String> rewrittenDeleteFiles()
    {
        return result.rewrittenDeleteFiles().stream()
                .map(file -> ContentFileParsers.toJson(file, partitionSpec))
                .collect(toImmutableList());
    }

    @Override
    public FileMetrics getFileMetrics()
    {
        DeleteFile deleteFile = deleteFile();
        Metrics metrics = new Metrics(
                deleteFile.recordCount(),
                deleteFile.columnSizes(),
                deleteFile.valueCounts(),
                deleteFile.nullValueCounts(),
                deleteFile.nanValueCounts(),
                deleteFile.lowerBounds(),
                deleteFile.upperBounds());
        return new FileMetrics(metrics, Optional.ofNullable(deleteFile.splitOffsets()));
    }

    @Override
    public long getWrittenBytes()
    {
        return deleteFile().fileSizeInBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public void appendRows(Page dataPage)
    {
        LongArrayBlock block = (LongArrayBlock) dataPage.getBlock(positionChannel);
        for (int i = 0; i < block.getPositionCount(); i++) {
            writer.delete(dataFilePath, block.getLong(i), partitionSpec, partition);
        }
    }

    private DeleteFile deleteFile()
    {
        try {
            return result.deleteFiles().getLast();
        }
        catch (NoSuchElementException e) {
            throw new TrinoException(ICEBERG_WRITER_OPEN_ERROR, "Delete file must exist", e);
        }
    }

    public DeleteWriteResult result()
    {
        return writer.result();
    }

    @Override
    public Closeable commit()
    {
        try {
            writer.close();
            result = writer.result();
        }
        catch (IOException e) {
            try {
                rollbackAction.close();
            }
            catch (Exception ex) {
                if (!e.equals(ex)) {
                    e.addSuppressed(ex);
                }
            }
            throw new TrinoException(ICEBERG_WRITER_OPEN_ERROR, "Error closing Deletion Vector file", e);
        }
        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            writer.close();
            result = writer.result();
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_WRITER_CLOSE_ERROR, "Error rolling back write to Deletion Vector file", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }
}
