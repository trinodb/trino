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
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.spi.TrinoException;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop/overflow allowed in bytes per split while generating splits
    private static final long MIN_BLOCK_SIZE = DataSize.of(32, MEGABYTE).toBytes();

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
    }

    public List<HudiSplit> createSplits(List<HivePartitionKey> partitionKeys, FileSlice fileSlice, String commitTime)
    {
        if (fileSlice.isEmpty()) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, String.format("Not a valid file slice: %s", fileSlice));
        }

        if (fileSlice.getLogFiles().findAny().isEmpty()) {
            // Base file only
            ValidationUtils.checkArgument(fileSlice.getBaseFile().isPresent(),
                    "Hudi base file must exist if there is no log file in the file slice");
            HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
            long fileSize = baseFile.getFileSize();

            if (fileSize == 0) {
                return ImmutableList.of(new HudiSplit(
                        Optional.of(HudiBaseFile.of(baseFile)),
                        Collections.emptyList(),
                        commitTime,
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(fileSize)
                ));
            }

            ImmutableList.Builder<HudiSplit> splits = ImmutableList.builder();
            long splitSize = Math.max(MIN_BLOCK_SIZE, baseFile.getPathInfo().getBlockSize());

            long bytesRemaining = fileSize;
            while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                splits.add(new HudiSplit(
                        Optional.of(HudiBaseFile.of(baseFile, fileSize - bytesRemaining, splitSize)),
                        Collections.emptyList(),
                        commitTime,
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(splitSize)));
                bytesRemaining -= splitSize;
            }
            if (bytesRemaining > 0) {
                splits.add(new HudiSplit(
                        Optional.of(HudiBaseFile.of(baseFile, fileSize - bytesRemaining, bytesRemaining)),
                        Collections.emptyList(),
                        commitTime,
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(bytesRemaining)));
            }
            return splits.build();
        }

        // Base and log files
        Option<HoodieBaseFile> baseFileOption = fileSlice.getBaseFile();
        return Collections.singletonList(
                new HudiSplit(
                        baseFileOption.isPresent()
                                ? Optional.of(HudiBaseFile.of(baseFileOption.get()))
                                : Optional.empty(),
                        fileSlice.getLogFiles().map(e -> e.getPath().toString()).toList(),
                        commitTime,
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(fileSlice.getTotalFileSize())));
    }
}
