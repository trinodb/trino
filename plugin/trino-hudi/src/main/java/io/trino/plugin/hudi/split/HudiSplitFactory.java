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
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.TrinoException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop/overflow allowed in bytes per split while generating splits

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
    }

    public Stream<HudiSplit> createSplits(List<HivePartitionKey> partitionKeys, HudiFileStatus fileStatus)
    {
        List<FileSplit> splits;
        try {
            splits = createSplits(fileStatus);
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, e);
        }

        return splits.stream()
                .map(fileSplit -> new HudiSplit(
                        fileSplit.getPath().toString(),
                        fileSplit.getStart(),
                        fileSplit.getLength(),
                        fileStatus.length(),
                        fileStatus.modificationTime(),
                        ImmutableList.of(),
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(fileSplit.getLength())));
    }

    private List<FileSplit> createSplits(HudiFileStatus fileStatus)
            throws IOException
    {
        if (fileStatus.isDirectory()) {
            throw new IOException("Not a file: " + fileStatus.path());
        }

        long length = fileStatus.length();

        if (length == 0) {
            return ImmutableList.of(new FileSplit(fileStatus.path(), 0, 0, new String[0]));
        }

        if (fileStatus.path() instanceof PathWithBootstrapFileStatus) {
            return ImmutableList.of(new FileSplit(fileStatus.path(), 0, length, (String[]) null));
        }

        ImmutableList.Builder<FileSplit> splits = ImmutableList.builder();
        long splitSize = fileStatus.blockSize();

        long bytesRemaining = length;
        while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            splits.add(new FileSplit(fileStatus.path(), length - bytesRemaining, splitSize, (String[]) null));
            bytesRemaining -= splitSize;
        }
        if (bytesRemaining != 0) {
            splits.add(new FileSplit(fileStatus.path(), length - bytesRemaining, bytesRemaining, (String[]) null));
        }
        return splits.build();
    }
}
