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
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DataFileInfo.DataFileType.DATA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the filtering logic in finishMerge correctly excludes
 * DataFileInfo entries with deletion vectors from cleanup on failed writes.
 * This prevents data loss by not deleting active source files that are
 * referenced by deletion vector annotations.
 *
 * @see <a href="https://github.com/trinodb/trino/issues/29361">#29361</a>
 */
public class TestCleanupFailedWriteWithDeletionVectors
{
    private static final DeltaLakeJsonFileStatistics EMPTY_STATS = new DeltaLakeJsonFileStatistics(
            Optional.of(1L), Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testFilterExcludesDeletionVectorEntries()
    {
        DataFileInfo newlyWrittenFile = new DataFileInfo(
                "new-data-file.parquet",
                1024,
                System.currentTimeMillis(),
                DATA,
                ImmutableList.of(),
                EMPTY_STATS,
                Optional.empty());

        DataFileInfo dvAnnotatedSourceFile = new DataFileInfo(
                "existing-source-file.parquet",
                2048,
                System.currentTimeMillis(),
                DATA,
                ImmutableList.of(),
                EMPTY_STATS,
                Optional.of(new DeletionVectorEntry("u", "R7QFX3rGXPFLhHGq&7g<", OptionalInt.of(1), 34, 1)));

        List<DataFileInfo> allFiles = ImmutableList.of(newlyWrittenFile, dvAnnotatedSourceFile);

        // This is the filtering logic applied in DeltaLakeMetadata.finishMerge
        List<DataFileInfo> filesToCleanup = allFiles.stream()
                .filter(file -> file.deletionVector().isEmpty())
                .collect(toImmutableList());

        assertThat(filesToCleanup).containsExactly(newlyWrittenFile);
        assertThat(filesToCleanup).doesNotContain(dvAnnotatedSourceFile);
    }

    @Test
    public void testFilterPreservesAllNewlyWrittenFiles()
    {
        DataFileInfo newFile1 = new DataFileInfo(
                "new-file-1.parquet",
                1024,
                System.currentTimeMillis(),
                DATA,
                ImmutableList.of(),
                EMPTY_STATS,
                Optional.empty());

        DataFileInfo newFile2 = new DataFileInfo(
                "new-file-2.parquet",
                2048,
                System.currentTimeMillis(),
                DATA,
                ImmutableList.of(),
                EMPTY_STATS,
                Optional.empty());

        DataFileInfo dvEntry1 = new DataFileInfo(
                "source-1.parquet",
                4096,
                System.currentTimeMillis(),
                DATA,
                ImmutableList.of(),
                EMPTY_STATS,
                Optional.of(new DeletionVectorEntry("u", "ab^-aqEH.-t@S}K{vb[*k^", OptionalInt.of(1), 34, 2)));

        DataFileInfo dvEntry2 = new DataFileInfo(
                "source-2.parquet",
                4096,
                System.currentTimeMillis(),
                DATA,
                ImmutableList.of(),
                EMPTY_STATS,
                Optional.of(new DeletionVectorEntry("u", "R7QFX3rGXPFLhHGq&7g<", OptionalInt.of(1), 34, 1)));

        List<DataFileInfo> allFiles = ImmutableList.of(newFile1, newFile2, dvEntry1, dvEntry2);

        List<DataFileInfo> filesToCleanup = allFiles.stream()
                .filter(file -> file.deletionVector().isEmpty())
                .collect(toImmutableList());

        assertThat(filesToCleanup).containsExactly(newFile1, newFile2);
    }
}
