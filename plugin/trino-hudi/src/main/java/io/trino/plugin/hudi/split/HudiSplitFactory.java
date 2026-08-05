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
import io.trino.filesystem.cache.SplitAffinityProvider;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.TrinoException;

import java.util.List;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop/overflow allowed in bytes per split while generating splits

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;
    private final SplitAffinityProvider splitAffinityProvider;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            SplitAffinityProvider splitAffinityProvider)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
        this.splitAffinityProvider = requireNonNull(splitAffinityProvider, "splitAffinityProvider is null");
    }

    public List<HudiSplit> createSplits(List<HivePartitionKey> partitionKeys, HudiFileStatus fileStatus)
    {
        if (fileStatus.isDirectory()) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, format("Not a valid location: %s", fileStatus.location()));
        }

        long fileSize = fileStatus.length();
        String location = fileStatus.location().toString();

        if (fileSize == 0) {
            return ImmutableList.of(new HudiSplit(
                    location,
                    0,
                    fileSize,
                    fileSize,
                    fileStatus.modificationTime(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(fileSize),
                    splitAffinityProvider.getKey(location, 0, fileSize)));
        }

        ImmutableList.Builder<HudiSplit> splits = ImmutableList.builder();
        long splitSize = fileStatus.blockSize();

        long bytesRemaining = fileSize;
        while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            long start = fileSize - bytesRemaining;
            splits.add(new HudiSplit(
                    location,
                    start,
                    splitSize,
                    fileSize,
                    fileStatus.modificationTime(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(splitSize),
                    splitAffinityProvider.getKey(location, start, splitSize)));
            bytesRemaining -= splitSize;
        }
        if (bytesRemaining > 0) {
            long start = fileSize - bytesRemaining;
            splits.add(new HudiSplit(
                    location,
                    start,
                    bytesRemaining,
                    fileSize,
                    fileStatus.modificationTime(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(bytesRemaining),
                    splitAffinityProvider.getKey(location, start, bytesRemaining)));
        }
        return splits.build();
    }
}
