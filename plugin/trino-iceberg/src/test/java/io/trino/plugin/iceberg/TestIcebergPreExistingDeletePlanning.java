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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.delete.DeleteFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergMetadata.extractPreExistingDeletesByDataFile;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.FileFormat.PUFFIN;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the Wave 7 coordinator-side helper that plans pre-existing delete files per
 * data file in {@link IcebergMetadata#extractPreExistingDeletesByDataFile}. These tests exercise
 * the pure data transform directly (no Iceberg scan planning) and therefore serve as a guard
 * against accidentally re-introducing per-worker manifest-list reads.
 */
final class TestIcebergPreExistingDeletePlanning
{
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();

    @Test
    public void extractSkipsDataFilesWithoutDeletes()
    {
        DataFile dataFile = newDataFile("s3://bucket/data/no-deletes.parquet");

        Map<String, List<DeleteFile>> result = extractPreExistingDeletesByDataFile(
                ImmutableList.of(fakeFileScanTask(dataFile, ImmutableList.of())));

        assertThat(result).isEmpty();
    }

    @Test
    public void extractKeepsPositionAndEqualityDeletes()
    {
        DataFile dataFile = newDataFile("s3://bucket/data/file-with-deletes.parquet");
        org.apache.iceberg.DeleteFile positional = newPositionDeleteFile("s3://bucket/deletes/pos-1.parquet");
        org.apache.iceberg.DeleteFile equality = newEqualityDeleteFile("s3://bucket/deletes/eq-1.parquet");

        Map<String, List<DeleteFile>> result = extractPreExistingDeletesByDataFile(
                ImmutableList.of(fakeFileScanTask(dataFile, ImmutableList.of(positional, equality))));

        assertThat(result).containsOnlyKeys(dataFile.location());
        assertThat(result.get(dataFile.location()))
                .extracting(DeleteFile::path)
                .containsExactlyInAnyOrder(positional.location(), equality.location());
    }

    @Test
    public void extractPropagatesDeletionVectorsOneToOne()
    {
        DataFile dataFile = newDataFile("s3://bucket/data/dv-source.parquet");
        org.apache.iceberg.DeleteFile deletionVector = newDeletionVector(
                "s3://bucket/deletes/dv.puffin",
                dataFile.location());

        Map<String, List<DeleteFile>> result = extractPreExistingDeletesByDataFile(
                ImmutableList.of(fakeFileScanTask(dataFile, ImmutableList.of(deletionVector))));

        assertThat(result).containsOnlyKeys(dataFile.location());
        List<DeleteFile> deletes = result.get(dataFile.location());
        assertThat(deletes).hasSize(1);
        DeleteFile propagated = deletes.getFirst();
        assertThat(propagated.isDeletionVector()).isTrue();
        assertThat(propagated.referencedDataFile()).hasValue(dataFile.location());
    }

    @Test
    public void extractGroupsMultipleDataFilesIndependently()
    {
        DataFile withDeletes = newDataFile("s3://bucket/data/with.parquet");
        DataFile withoutDeletes = newDataFile("s3://bucket/data/without.parquet");
        org.apache.iceberg.DeleteFile positional = newPositionDeleteFile("s3://bucket/deletes/pos.parquet");

        Map<String, List<DeleteFile>> result = extractPreExistingDeletesByDataFile(ImmutableList.of(
                fakeFileScanTask(withDeletes, ImmutableList.of(positional)),
                fakeFileScanTask(withoutDeletes, ImmutableList.of())));

        assertThat(result).containsOnlyKeys(withDeletes.location());
    }

    private static DataFile newDataFile(String path)
    {
        return DataFiles.builder(UNPARTITIONED)
                .withPath(path)
                .withFormat(PARQUET)
                .withFileSizeInBytes(1_024)
                .withRecordCount(10)
                .build();
    }

    private static org.apache.iceberg.DeleteFile newPositionDeleteFile(String path)
    {
        return withSequenceNumber(FileMetadata.deleteFileBuilder(UNPARTITIONED)
                .withPath(path)
                .withFormat(PARQUET)
                .ofPositionDeletes()
                .withFileSizeInBytes(64)
                .withRecordCount(1)
                .build());
    }

    private static org.apache.iceberg.DeleteFile newEqualityDeleteFile(String path)
    {
        return withSequenceNumber(FileMetadata.deleteFileBuilder(UNPARTITIONED)
                .withPath(path)
                .withFormat(PARQUET)
                .ofEqualityDeletes(1)
                .withFileSizeInBytes(64)
                .withRecordCount(1)
                .build());
    }

    private static org.apache.iceberg.DeleteFile newDeletionVector(String path, String referencedDataFile)
    {
        return withSequenceNumber(FileMetadata.deleteFileBuilder(UNPARTITIONED)
                .withPath(path)
                .withFormat(PUFFIN)
                .ofPositionDeletes()
                .withFileSizeInBytes(32)
                .withRecordCount(1)
                .withContentOffset(0)
                .withContentSizeInBytes(32)
                .withReferencedDataFile(referencedDataFile)
                .build());
    }

    /**
     * The production scan path supplies {@code dataSequenceNumber} from the manifest entry; the
     * builder here does not, so {@link io.trino.plugin.iceberg.delete.DeleteFile#fromIceberg}
     * would unbox {@code null}. Reflectively invoke {@code BaseFile#setDataSequenceNumber} to
     * mimic the post-commit state without depending on package-private types.
     */
    private static org.apache.iceberg.DeleteFile withSequenceNumber(org.apache.iceberg.DeleteFile deleteFile)
    {
        try {
            // BaseFile and GenericDeleteFile are package-private in org.apache.iceberg so
            // Class#getMethod cannot reach the public setter across the package boundary.
            // Walk the class hierarchy to locate setDataSequenceNumber and override access.
            Class<?> current = deleteFile.getClass();
            while (current != null) {
                try {
                    var setter = current.getDeclaredMethod("setDataSequenceNumber", Long.class);
                    setter.setAccessible(true);
                    setter.invoke(deleteFile, 1L);
                    return deleteFile;
                }
                catch (NoSuchMethodException ignored) {
                    current = current.getSuperclass();
                }
            }
            throw new AssertionError("setDataSequenceNumber not found on " + deleteFile.getClass());
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to stamp dataSequenceNumber on Iceberg DeleteFile", e);
        }
    }

    private static FileScanTask fakeFileScanTask(DataFile dataFile, List<org.apache.iceberg.DeleteFile> deletes)
    {
        return new FileScanTask()
        {
            @Override
            public DataFile file()
            {
                return dataFile;
            }

            @Override
            public List<org.apache.iceberg.DeleteFile> deletes()
            {
                return deletes;
            }

            @Override
            public Schema schema()
            {
                return SCHEMA;
            }

            @Override
            public PartitionSpec spec()
            {
                return UNPARTITIONED;
            }

            @Override
            public long start()
            {
                return 0;
            }

            @Override
            public long length()
            {
                return dataFile.fileSizeInBytes();
            }

            @Override
            public Expression residual()
            {
                return Expressions.alwaysTrue();
            }

            @Override
            public Iterable<FileScanTask> split(long splitSize)
            {
                return ImmutableList.of(this);
            }
        };
    }
}
