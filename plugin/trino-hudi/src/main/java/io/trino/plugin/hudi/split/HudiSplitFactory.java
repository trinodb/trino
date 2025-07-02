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
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.plugin.hudi.file.HudiLogFile;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop/overflow allowed in bytes per split while generating splits

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;
    private final DataSize targetSplitSize;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            DataSize targetSplitSize,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
        this.targetSplitSize = requireNonNull(targetSplitSize, "targetSplitSize is null");
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
    }

    public List<HudiSplit> createSplits(List<HivePartitionKey> partitionKeys, FileSlice fileSlice, String commitTime)
    {
        return createHudiSplits(hudiTableHandle, partitionKeys, fileSlice, commitTime, hudiSplitWeightProvider, targetSplitSize, cachingHostAddressProvider);
    }

    public static List<HudiSplit> createHudiSplits(
            HudiTableHandle hudiTableHandle,
            List<HivePartitionKey> partitionKeys,
            FileSlice fileSlice,
            String commitTime,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            DataSize targetSplitSize,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        if (fileSlice.isEmpty()) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, format("Not a valid file slice: %s", fileSlice.toString()));
        }

        // Handle MERGE_ON_READ tables to be read in read_optimized mode
        // IMPORTANT: These tables will have a COPY_ON_WRITE table type due to how `HudiTableTypeUtils#fromInputFormat`
        if (fileSlice.getLogFiles().findAny().isEmpty()
                || hudiTableHandle.getTableType().equals(HoodieTableType.COPY_ON_WRITE)) {
            // Base file only
            checkArgument(fileSlice.getBaseFile().isPresent(),
                    "Hudi base file must exist if there is no log file in the file slice");
            HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
            long fileSize = baseFile.getFileSize();
            List<HostAddress> addresses = cachingHostAddressProvider.getHosts(
                    baseFile.getPath(), ImmutableList.of());

            if (fileSize == 0) {
                return ImmutableList.of(new HudiSplit(
                        HudiBaseFile.of(baseFile),
                        Collections.emptyList(),
                        commitTime,
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(fileSize),
                        addresses));
            }

            ImmutableList.Builder<HudiSplit> splits = ImmutableList.builder();
            long targetSplitSizeInBytes = Math.max(targetSplitSize.toBytes(), baseFile.getPathInfo().getBlockSize());

            long bytesRemaining = fileSize;
            while (((double) bytesRemaining) / targetSplitSizeInBytes > SPLIT_SLOP) {
                splits.add(new HudiSplit(
                        HudiBaseFile.of(baseFile, fileSize - bytesRemaining, targetSplitSizeInBytes),
                        Collections.emptyList(),
                        commitTime,
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(targetSplitSizeInBytes),
                        addresses));
                bytesRemaining -= targetSplitSizeInBytes;
            }
            if (bytesRemaining > 0) {
                splits.add(new HudiSplit(
                        HudiBaseFile.of(baseFile, fileSize - bytesRemaining, bytesRemaining),
                        Collections.emptyList(),
                        commitTime,
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(bytesRemaining),
                        addresses));
            }
            return splits.build();
        }

        // Base and log files
        Option<HoodieBaseFile> baseFileOption = fileSlice.getBaseFile();
        return ImmutableList.of(new HudiSplit(
                baseFileOption.isPresent() ? HudiBaseFile.of(baseFileOption.get()) : null,
                fileSlice.getLogFiles().map(HudiLogFile::of).toList(),
                commitTime,
                hudiTableHandle.getRegularPredicates(),
                partitionKeys,
                hudiSplitWeightProvider.calculateSplitWeight(fileSlice.getTotalFileSize()),
                ImmutableList.of()));
    }
}
