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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.fileio.ForwardingOutputFile;
import jakarta.annotation.Nullable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.TrinoBitmapPositionDeleteIndex;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileFormat.PUFFIN;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.puffin.StandardBlobTypes.DV_V1;

/**
 * Copy {@link org.apache.iceberg.deletes.BaseDVFileWriter} and replace its file system with TrinoFileSystem
 */
public class DeletionVectorFileWriter
        implements DVFileWriter
{
    private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
    private static final String CARDINALITY_KEY = "cardinality";

    private final NodeVersion nodeVersion;
    private final TrinoFileSystem fileSystem;
    private final Location location;
    private final Function<String, PositionDeleteIndex> loadPreviousDeletes;
    private final Map<String, Deletes> deletesByPath = new HashMap<>();
    private final Map<String, BlobMetadata> blobsByPath = new HashMap<>();

    private DeleteWriteResult result;

    public DeletionVectorFileWriter(
            NodeVersion nodeVersion,
            TrinoFileSystem fileSystem,
            Location location,
            Function<String, PositionDeleteIndex> loadPreviousDeletes)
    {
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.location = requireNonNull(location, "location is null");
        this.loadPreviousDeletes = requireNonNull(loadPreviousDeletes, "loadPreviousDeletes is null");
    }

    @Override
    public void delete(String path, long pos, PartitionSpec spec, StructLike partition)
    {
        Deletes deletes = deletesByPath.computeIfAbsent(path, _ -> new Deletes(path, spec, partition));
        PositionDeleteIndex positions = deletes.positions();
        positions.delete(pos);
    }

    @Override
    public DeleteWriteResult result()
    {
        checkState(result != null, "Cannot get result from unclosed writer");
        return result;
    }

    @Override
    public void close()
            throws IOException
    {
        if (result == null) {
            CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
            List<DeleteFile> rewrittenDeleteFiles = new ArrayList<>();

            PuffinWriter writer = newWriter();

            try (PuffinWriter closeableWriter = writer) {
                for (Deletes deletes : deletesByPath.values()) {
                    String path = deletes.path();
                    PositionDeleteIndex positions = deletes.positions();
                    PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);
                    if (previousPositions != null) {
                        positions.merge(previousPositions);
                        for (DeleteFile previousDeleteFile : previousPositions.deleteFiles()) {
                            // only DVs and file-scoped deletes can be discarded from the table state
                            if (ContentFileUtil.isFileScoped(previousDeleteFile)) {
                                rewrittenDeleteFiles.add(previousDeleteFile);
                            }
                        }
                    }
                    write(closeableWriter, deletes);
                    referencedDataFiles.add(path);
                }
            }

            // DVs share the Puffin path and file size but have different offsets
            String puffinPath = writer.location();
            long puffinFileSize = writer.fileSize();

            List<DeleteFile> dvs = deletesByPath.keySet().stream()
                    .map(path -> createDV(puffinPath, puffinFileSize, path))
                    .collect(toImmutableList());

            this.result = new DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles);
        }
    }

    private DeleteFile createDV(String path, long size, String referencedDataFile)
    {
        Deletes deletes = deletesByPath.get(referencedDataFile);
        BlobMetadata blobMetadata = blobsByPath.get(referencedDataFile);
        return FileMetadata.deleteFileBuilder(deletes.spec())
                .ofPositionDeletes()
                .withFormat(PUFFIN)
                .withPath(path)
                .withPartition(deletes.partition())
                .withFileSizeInBytes(size)
                .withReferencedDataFile(referencedDataFile)
                .withContentOffset(blobMetadata.offset())
                .withContentSizeInBytes(blobMetadata.length())
                .withRecordCount(deletes.positions().cardinality())
                .build();
    }

    private void write(PuffinWriter writer, Deletes deletes)
    {
        String path = deletes.path();
        PositionDeleteIndex positions = deletes.positions();
        BlobMetadata blobMetadata = writer.write(toBlob(positions, path));
        blobsByPath.put(path, blobMetadata);
    }

    private PuffinWriter newWriter()
    {
        return Puffin.write(new ForwardingOutputFile(fileSystem, location))
                .createdBy("Trino version " + nodeVersion.toString())
                .build();
    }

    private static Blob toBlob(PositionDeleteIndex positions, String path)
    {
        return new Blob(
                DV_V1,
                ImmutableList.of(ROW_POSITION.fieldId()),
                -1 /* snapshot ID is inherited */,
                -1 /* sequence number is inherited */,
                positions.serialize(),
                null /* uncompressed */,
                ImmutableMap.<String, String>builder()
                        .put(REFERENCED_DATA_FILE_KEY, path)
                        .put(CARDINALITY_KEY, String.valueOf(positions.cardinality()))
                        .buildOrThrow());
    }

    private static class Deletes
    {
        private final String path;
        private final PartitionSpec spec;
        private final StructLike partition;
        private final PositionDeleteIndex positions;

        private Deletes(String path, PartitionSpec spec, @Nullable StructLike partition)
        {
            this.path = requireNonNull(path, "path is null");
            this.spec = requireNonNull(spec, "spec is null");
            this.partition = StructLikeUtil.copy(partition);
            this.positions = new TrinoBitmapPositionDeleteIndex();
        }

        public String path()
        {
            return path;
        }

        public PartitionSpec spec()
        {
            return spec;
        }

        @Nullable
        public StructLike partition()
        {
            return partition;
        }

        public PositionDeleteIndex positions()
        {
            return positions;
        }
    }
}
