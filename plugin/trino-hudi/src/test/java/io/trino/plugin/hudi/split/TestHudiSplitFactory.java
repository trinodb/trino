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
package io.trino.plugin.hudi.split;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.cache.DefaultCachingHostAddressProvider;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiSplitFactory
{
    private static final String COMMIT_TIME = "20250625153731546";
    private static final List<HivePartitionKey> PARTITION_KEYS = ImmutableList.of();

    @Test
    public void testCreateHudiSplitsWithSmallBaseFile()
    {
        // Test with 20MB target split size and 10MB base file
        // - should create 1 split
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(10, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(10, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithExactSplitDivide()
    {
        // Test with 20MB target and 60MB base file
        // - should create 3 splits
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(60, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(20, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(40, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithSlightlyOversizedFile()
    {
        // Test with 20MB target and 61MB base file
        // - should create 3 splits (61/20 = 3.05, 0.05 is within split slop of 0.1)
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(61, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(20, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(40, MEGABYTE).toBytes(), DataSize.of(21, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithOversizedFileExceedingSlop()
    {
        // Test with 20MB target and 65MB base file
        // - should create 4 splits (65/20 = 3.25)
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(65, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(20, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(40, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(60, MEGABYTE).toBytes(), DataSize.of(5, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithLargerBlockSize()
    {
        // Test with 1MB target split size and 32MB base file
        // - should create 4 splits because the block size of 8MB is larger than the target split size
        testSplitCreation(
                DataSize.of(1, MEGABYTE),
                DataSize.of(32, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(8, MEGABYTE)),
                        Pair.of(DataSize.of(8, MEGABYTE).toBytes(), DataSize.of(8, MEGABYTE)),
                        Pair.of(DataSize.of(16, MEGABYTE).toBytes(), DataSize.of(8, MEGABYTE)),
                        Pair.of(DataSize.of(24, MEGABYTE).toBytes(), DataSize.of(8, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithLogFile()
    {
        // Test with 20MB target and 65MB base file and 10MB log file
        // - should create 1 split regardless of size
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(65, MEGABYTE),
                Option.of(DataSize.of(10, MEGABYTE)),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(65, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithZeroSizeFile()
    {
        // Test with zero-size file - should create 1 split with zero size
        testSplitCreation(
                DataSize.of(128, MEGABYTE),
                DataSize.of(0, MEGABYTE),
                Option.empty(),
                ImmutableList.of(Pair.of(0L, DataSize.of(0, MEGABYTE))));
    }

    private static void testSplitCreation(
            DataSize targetSplitSize,
            DataSize baseFileSize,
            Option<DataSize> logFileSize,
            List<Pair<Long, DataSize>> expectedSplitInfo)
    {
        HudiTableHandle tableHandle = createTableHandle();
        HudiSplitWeightProvider weightProvider = new SizeBasedSplitWeightProvider(0.05, DataSize.of(128, MEGABYTE));

        FileSlice fileSlice = createFileSlice(baseFileSize, logFileSize);

        List<HudiSplit> splits = HudiSplitFactory.createHudiSplits(
                tableHandle, PARTITION_KEYS, fileSlice, COMMIT_TIME, weightProvider, targetSplitSize,
            new DefaultCachingHostAddressProvider());

        assertThat(splits).hasSize(expectedSplitInfo.size());

        for (int i = 0; i < expectedSplitInfo.size(); i++) {
            HudiSplit split = splits.get(i);
            assertThat(split.getBaseFile()).isPresent();
            assertThat(split.getBaseFile().get().getFileSize()).isEqualTo(baseFileSize.toBytes());
            assertThat(split.getBaseFile().get().getStart())
                    .isEqualTo(expectedSplitInfo.get(i).getLeft());
            assertThat(split.getBaseFile().get().getLength())
                    .isEqualTo(expectedSplitInfo.get(i).getRight().toBytes());
            assertThat(split.getCommitTime()).isEqualTo(COMMIT_TIME);
            assertThat(split.getLogFiles().size()).isEqualTo(logFileSize.isPresent() ? 1 : 0);
            long totalSize = logFileSize.isPresent() ?
                    baseFileSize.toBytes() + logFileSize.get().toBytes() : expectedSplitInfo.get(i).getRight().toBytes();
            assertThat(split.getSplitWeight()).isEqualTo(weightProvider.calculateSplitWeight(totalSize));
        }
    }

    private static HudiTableHandle createTableHandle()
    {
        return new HudiTableHandle(
                "test_schema",
                "test_table",
                "/test/path",
                HoodieTableType.MERGE_ON_READ,
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                "",
                "101");
    }

    private static FileSlice createFileSlice(DataSize baseFileSize, Option<DataSize> logFileSize)
    {
        String fileId = "5a4f6a70-0306-40a8-952b-045b0d8ff0d4-0";
        HoodieFileGroupId fileGroupId = new HoodieFileGroupId("partition", fileId);
        long blockSize = 8L * 1024 * 1024;
        String baseFilePath = "/test/path/" + fileGroupId + "_4-19-0_" + COMMIT_TIME + ".parquet";
        String logFilePath = "/test/path/." + fileId + "_2025062515374131546.log.1_0-53-80";
        StoragePathInfo baseFileInfo = new StoragePathInfo(
                new StoragePath(baseFilePath), baseFileSize.toBytes(), false, (short) 0, blockSize, System.currentTimeMillis());
        StoragePathInfo logFileInfo = new StoragePathInfo(
                new StoragePath(logFilePath), logFileSize.isPresent() ? logFileSize.get().toBytes() : 0L,
                false, (short) 0, blockSize, System.currentTimeMillis());
        HoodieBaseFile baseFile = new HoodieBaseFile(baseFileInfo);
        return new FileSlice(fileGroupId, COMMIT_TIME, baseFile,
                logFileSize.isPresent() ? ImmutableList.of(new HoodieLogFile(logFileInfo)) : ImmutableList.of());
    }
}
