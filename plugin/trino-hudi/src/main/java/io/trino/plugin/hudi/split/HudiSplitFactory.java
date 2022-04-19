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
import io.trino.plugin.hudi.HudiErrorCode;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;
    private final FileSystem fileSystem;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            FileSystem fileSystem)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    public Stream<HudiSplit> createSplits(List<HivePartitionKey> partitionKeys, FileStatus fileStatus)
    {
        final List<FileSplit> splits;
        try {
            splits = getSplits(fileSystem, fileStatus);
        }
        catch (IOException e) {
            throw new TrinoException(HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT, e);
        }

        return splits.stream()
                .map(fileSplit -> new HudiSplit(
                        fileSplit.getPath().toString(),
                        fileSplit.getStart(),
                        fileSplit.getLength(),
                        fileStatus.getLen(),
                        ImmutableList.of(),
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.weightForSplitSizeInBytes(fileSplit.getLength())));
    }

    private static List<FileSplit> getSplits(FileSystem fs, FileStatus fileStatus)
            throws IOException
    {
        if (fileStatus.isDirectory()) {
            throw new IOException("Not a file: " + fileStatus.getPath());
        }

        Path path = fileStatus.getPath();
        long length = fileStatus.getLen();

        // generate splits
        List<FileSplit> splits = new ArrayList<>();
        if (length != 0) {
            BlockLocation[] blkLocations;
            if (fileStatus instanceof LocatedFileStatus) {
                blkLocations = ((LocatedFileStatus) fileStatus).getBlockLocations();
            }
            else {
                blkLocations = fs.getFileBlockLocations(fileStatus, 0, length);
            }
            if (isSplitable(path)) {
                long splitSize = fileStatus.getBlockSize();

                long bytesRemaining = length;
                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length - bytesRemaining);
                    splits.add(makeSplit(path, length - bytesRemaining, splitSize, splitHosts[0], splitHosts[1]));
                    bytesRemaining -= splitSize;
                }

                if (bytesRemaining != 0) {
                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length - bytesRemaining);
                    splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining, splitHosts[0], splitHosts[1]));
                }
            }
            else {
                String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, 0);
                splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
            }
        }
        else {
            //Create empty hosts array for zero length files
            splits.add(makeSplit(path, 0, length, new String[0]));
        }
        return splits;
    }

    private static boolean isSplitable(Path filename)
    {
        return !(filename instanceof PathWithBootstrapFileStatus);
    }

    private static FileSplit makeSplit(Path file, long start, long length, String[] hosts)
    {
        return new FileSplit(file, start, length, hosts);
    }

    private static FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts)
    {
        return new FileSplit(file, start, length, hosts, inMemoryHosts);
    }

    private static String[][] getSplitHostsAndCachedHosts(BlockLocation[] blkLocations, long offset)
            throws IOException
    {
        int startIndex = getBlockIndex(blkLocations, offset);

        return new String[][] {blkLocations[startIndex].getHosts(),
                blkLocations[startIndex].getCachedHosts()};
    }

    private static int getBlockIndex(BlockLocation[] blkLocations, long offset)
    {
        for (int i = 0; i < blkLocations.length; i++) {
            // is the offset inside this block?
            if ((blkLocations[i].getOffset() <= offset) &&
                    (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())) {
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length - 1];
        long fileLength = last.getOffset() + last.getLength() - 1;
        throw new IllegalArgumentException("Offset " + offset +
                " is outside of file (0.." +
                fileLength + ")");
    }
}
