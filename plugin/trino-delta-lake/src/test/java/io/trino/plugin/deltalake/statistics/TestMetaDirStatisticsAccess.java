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
package io.trino.plugin.deltalake.statistics;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodecFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.deltalake.DefaultDeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.metastore.NoOpVendedCredentialsProvider;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.statistics.MetaDirStatisticsAccess.STATISTICS_META_DIR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMetaDirStatisticsAccess
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("test_schema", "test_table");
    private static final String TABLE_LOCATION = "local:///test_extended_stats";

    @TempDir
    Path tempDir;

    @Test
    void testReadMissingFileReturnsEmpty()
    {
        MetaDirStatisticsAccess access = createAccess();
        Optional<ExtendedStatistics> result = access.readExtendedStatistics(
                SESSION, TABLE_NAME, TABLE_LOCATION, "nonexistent.extended_stats.json", credentials());
        assertThat(result).isEmpty();
    }

    @Test
    void testUpdateAndRead()
    {
        MetaDirStatisticsAccess access = createAccess();
        ExtendedStatistics initialStats = new ExtendedStatistics(Instant.ofEpochMilli(1_000_000), Map.of(), Optional.empty());

        // no previous file
        String firstFile = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.empty(), credentials(), initialStats);

        assertThat(firstFile).isNotBlank();
        Optional<ExtendedStatistics> firstRead = access.readExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, firstFile, credentials());
        assertThat(firstRead).isPresent();
        assertThat(firstRead.get().getAlreadyAnalyzedModifiedTimeMax()).isEqualTo(initialStats.getAlreadyAnalyzedModifiedTimeMax());
        assertThat(firstRead.get().getColumnStatistics()).isEqualTo(initialStats.getColumnStatistics());

        // provide previous file so it gets replaced
        ExtendedStatistics updatedStats = new ExtendedStatistics(Instant.ofEpochMilli(2_000_000), Map.of(), Optional.of(ImmutableSet.of("col_a")));
        String secondFile = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.of(firstFile), credentials(), updatedStats);

        // new file differ from the old one
        assertThat(secondFile).isNotEqualTo(firstFile);
        assertThat(statsFilePath(firstFile)).doesNotExist();
        assertThat(statsFilePath(secondFile)).exists();

        // Reading with the new filename returns the updated content, not the old one
        Optional<ExtendedStatistics> secondRead = access.readExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, secondFile, credentials());
        assertThat(secondRead).isPresent();
        assertThat(secondRead.get().getAlreadyAnalyzedModifiedTimeMax()).isEqualTo(updatedStats.getAlreadyAnalyzedModifiedTimeMax());
        assertThat(secondRead.get().getAnalyzedColumns()).isEqualTo(updatedStats.getAnalyzedColumns());

        assertThat(access.readExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, firstFile, credentials())).isEmpty();
    }

    @Test
    void testUpdateCreatesFile()
    {
        MetaDirStatisticsAccess access = createAccess();
        String statsFile = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.empty(), credentials(), sampleStatistics());
        assertThat(statsFilePath(statsFile)).exists();
    }

    @Test
    void testUpdateReturnsUniqueFilename()
    {
        MetaDirStatisticsAccess access = createAccess();
        String file1 = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.empty(), credentials(), sampleStatistics());
        String file2 = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.empty(), credentials(), sampleStatistics());
        assertThat(file1).isNotEqualTo(file2);
    }

    @Test
    void testUpdateWithNoPreviousFile()
            throws IOException
    {
        MetaDirStatisticsAccess access = createAccess();
        String statsFile = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.empty(), credentials(), sampleStatistics());

        Path statsDir = statsFilePath(statsFile).getParent();
        try (var files = Files.list(statsDir)) {
            List<Path> listed = files.toList();
            assertThat(listed).hasSize(1);
            assertThat(listed.getFirst().getFileName().toString()).isEqualTo(statsFile);
        }
    }

    @Test
    void testUpdateDeletesPreviousFile()
    {
        MetaDirStatisticsAccess access = createAccess();

        String oldFile = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.empty(), credentials(), sampleStatistics());
        assertThat(statsFilePath(oldFile)).exists();

        String newFile = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.of(oldFile), credentials(), sampleStatistics());

        assertThat(statsFilePath(oldFile)).doesNotExist();
        assertThat(statsFilePath(newFile)).exists();
    }

    @Test
    void testDeleteExistingFile()
    {
        MetaDirStatisticsAccess access = createAccess();
        String statsFile = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.empty(), credentials(), sampleStatistics());
        assertThat(statsFilePath(statsFile)).exists();

        access.deleteExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, statsFile, credentials());

        assertThat(statsFilePath(statsFile)).doesNotExist();
    }

    @Test
    void testDeleteMissingFileIsNoOp()
    {
        // Must not throw
        createAccess().deleteExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, "nonexistent.extended_stats.json", credentials());
    }

    @Test
    void testFilenameContainsExpectedSuffix()
    {
        MetaDirStatisticsAccess access = createAccess();
        String statsFile = access.updateExtendedStatistics(SESSION, TABLE_NAME, TABLE_LOCATION, Optional.empty(), credentials(), sampleStatistics());
        assertThat(statsFile).endsWith(".extended_stats.json");
    }

    private MetaDirStatisticsAccess createAccess()
    {
        return new MetaDirStatisticsAccess(
                new DefaultDeltaLakeFileSystemFactory(
                        new LocalFileSystemFactory(tempDir),
                        new NoOpVendedCredentialsProvider()),
                new JsonCodecFactory().jsonCodec(ExtendedStatistics.class));
    }

    private static VendedCredentialsHandle credentials()
    {
        return VendedCredentialsHandle.empty(TABLE_LOCATION);
    }

    private static ExtendedStatistics sampleStatistics()
    {
        return new ExtendedStatistics(Instant.ofEpochMilli(1_000_000), Map.of(), Optional.empty());
    }

    private Path statsFilePath(String statsFile)
    {
        return tempDir.resolve("test_extended_stats").resolve(STATISTICS_META_DIR).resolve(statsFile);
    }
}
