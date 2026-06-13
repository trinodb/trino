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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDeleteFile
{
    private static final String DATA_FILE = "s3://bucket/data/part-0.parquet";
    private static final String OTHER_DATA_FILE = "s3://bucket/data/part-1.parquet";
    private static final String DELETE_FILE = "s3://bucket/data/deletes.parquet";

    @Test
    void fileScopedPositionDeleteMatchesReferencedDataFile()
    {
        DeleteFile fileScoped = positionDelete(Optional.of(DATA_FILE));

        assertThat(fileScoped.isFileScopedPositionDelete(DATA_FILE)).isTrue();
        assertThat(fileScoped.isFileScopedPositionDelete(OTHER_DATA_FILE)).isFalse();
    }

    @Test
    void partitionScopedPositionDeleteIsNeverFileScoped()
    {
        DeleteFile partitionScoped = positionDelete(Optional.empty());

        assertThat(partitionScoped.isFileScopedPositionDelete(DATA_FILE)).isFalse();
        assertThat(partitionScoped.isFileScopedPositionDelete(OTHER_DATA_FILE)).isFalse();
    }

    @Test
    void equalityDeleteIsNeverFileScopedPositionDelete()
    {
        DeleteFile equalityDelete = new DeleteFile(
                FileContent.EQUALITY_DELETES,
                DELETE_FILE,
                FileFormat.PARQUET,
                1L,
                10L,
                ImmutableList.of(1),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1L,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.of(DATA_FILE));

        assertThat(equalityDelete.isFileScopedPositionDelete(DATA_FILE)).isFalse();
    }

    @Test
    void deletionVectorIsReportedAsDV()
    {
        DeleteFile dv = new DeleteFile(
                FileContent.POSITION_DELETES,
                DELETE_FILE,
                FileFormat.PUFFIN,
                1L,
                10L,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1L,
                OptionalLong.of(0L),
                Optional.of(100),
                Optional.of(DATA_FILE));

        assertThat(dv.isDeletionVector()).isTrue();
        // DVs are also file-scoped by spec; reporting both flags lets callers choose either signal.
        assertThat(dv.isFileScopedPositionDelete(DATA_FILE)).isTrue();
    }

    @Test
    void retainedSizeIncludesReferencedDataFile()
    {
        // Sanity: presence of referencedDataFile must increase reported retained memory.
        // This guards the v3 / file-scoped delete code path where the field is non-empty
        // and must not be silently dropped from memory accounting.
        DeleteFile withReference = positionDelete(Optional.of(DATA_FILE));
        DeleteFile withoutReference = positionDelete(Optional.empty());

        assertThat(withReference.retainedSizeInBytes())
                .isGreaterThan(withoutReference.retainedSizeInBytes());
    }

    @Test
    void fromIcebergRejectsDeleteFileWithoutDataSequenceNumber()
    {
        // Iceberg's DeleteFile.dataSequenceNumber() returns boxed Long; in production the
        // manifest-read path always populates it, but in-memory builders (and any future caller
        // that constructs a delete file without committing it) leave it null. The Trino record
        // uses a primitive long for this field, so the conversion must reject null explicitly
        // rather than silently NPE inside the auto-unboxing.
        org.apache.iceberg.DeleteFile unsequenced = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .withPath(DELETE_FILE)
                .withFormat(FileFormat.PARQUET)
                .ofPositionDeletes()
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();

        assertThatThrownBy(() -> DeleteFile.fromIceberg(unsequenced))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Iceberg DeleteFile is missing dataSequenceNumber")
                .hasMessageContaining(DELETE_FILE);
    }

    private static DeleteFile positionDelete(Optional<String> referencedDataFile)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                DELETE_FILE,
                FileFormat.PARQUET,
                1L,
                10L,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1L,
                OptionalLong.empty(),
                Optional.empty(),
                referencedDataFile);
    }
}
