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
package io.trino.plugin.iceberg.system.files;

import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

public record TrinoManifestFile(
        String path,
        long length,
        int partitionSpecId,
        ManifestContent content,
        long sequenceNumber,
        long minSequenceNumber,
        Long snapshotId,
        Integer addedFilesCount,
        Integer existingFilesCount,
        Integer deletedFilesCount,
        Long addedRowsCount,
        Long existingRowsCount,
        Long deletedRowsCount,
        Long firstRowId)
        implements ManifestFile
{
    private static final long INSTANCE_SIZE = instanceSize(TrinoManifestFile.class);

    @Override
    public List<PartitionFieldSummary> partitions()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ManifestFile copy()
    {
        throw new UnsupportedOperationException("Cannot copy");
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + sizeOf(length)
                + sizeOf(partitionSpecId)
                + sizeOf(content.id())
                + sizeOf(sequenceNumber)
                + sizeOf(minSequenceNumber)
                + sizeOf(snapshotId)
                + sizeOf(addedFilesCount)
                + sizeOf(existingFilesCount)
                + sizeOf(deletedFilesCount)
                + sizeOf(addedRowsCount)
                + sizeOf(existingRowsCount)
                + sizeOf(deletedRowsCount)
                + sizeOf(firstRowId);
    }

    public static TrinoManifestFile from(ManifestFile manifestFile)
    {
        return new TrinoManifestFile(
                manifestFile.path(),
                manifestFile.length(),
                manifestFile.partitionSpecId(),
                manifestFile.content(),
                manifestFile.sequenceNumber(),
                manifestFile.minSequenceNumber(),
                manifestFile.snapshotId(),
                manifestFile.addedFilesCount(),
                manifestFile.existingFilesCount(),
                manifestFile.deletedFilesCount(),
                manifestFile.addedRowsCount(),
                manifestFile.existingRowsCount(),
                manifestFile.deletedRowsCount(),
                manifestFile.firstRowId());
    }
}
