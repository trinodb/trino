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
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.limit;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(IcebergSplitSource.class);
    private final CloseableIterable<CombinedScanTask> combinedScanIterable;
    private final Iterator<FileScanTask> fileScanIterator;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final boolean fetchSplitLocations;

    public IcebergSplitSource(
            CloseableIterable<CombinedScanTask> combinedScanIterable,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            boolean fetchSplitLocations)
    {
        this.combinedScanIterable = requireNonNull(combinedScanIterable, "combinedScanIterable is null");

        this.fileScanIterator = Streams.stream(combinedScanIterable)
                .map(CombinedScanTask::files)
                .flatMap(Collection::stream)
                .iterator();
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.fetchSplitLocations = fetchSplitLocations;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        // TODO: move this to a background thread
        List<ConnectorSplit> splits = new ArrayList<>();
        Iterator<FileScanTask> iterator = limit(fileScanIterator, maxSize);
        while (iterator.hasNext()) {
            FileScanTask task = iterator.next();
            splits.add(toIcebergSplit(task));
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public boolean isFinished()
    {
        return !fileScanIterator.hasNext();
    }

    @Override
    public void close()
    {
        try {
            combinedScanIterable.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ConnectorSplit toIcebergSplit(FileScanTask task)
    {
        // TODO: We should leverage residual expression and convert that to TupleDomain.
        //       The predicate here is used by readers for predicate push down at reader level,
        //       so when we do not use residual expression, we are just wasting CPU cycles
        //       on reader side evaluating a condition that we know will always be true.

        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().format(),
                getAddresses(task),
                getPartitionKeys(task));
    }

    private List<HostAddress> getAddresses(FileScanTask task)
    {
        if (fetchSplitLocations) {
            return blockLocations(task);
        }
        else {
            return ImmutableList.of();
        }
    }

    private List<HostAddress> blockLocations(FileScanTask task)
    {
        Set<HostAddress> locationSets = new HashSet<>();
        Path path = new Path(task.file().path().toString());
        try {
            FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
            for (BlockLocation blockLocation : fs.getFileBlockLocations(path, task.start(), task.length())) {
                locationSets.addAll(getHostAddresses(blockLocation));
            }
        }
        catch (IOException e) {
            log.warn("Failed to get block locations for path %s", path, e);
        }
        ArrayList<HostAddress> list = new ArrayList<>(locationSets);
        return list;
    }

    private static List<HostAddress> getHostAddresses(BlockLocation blockLocation)
    {
        // Hadoop FileSystem returns "localhost" as a default
        return Arrays.stream(getBlockHosts(blockLocation))
                .map(HostAddress::fromString)
                .filter(address -> !address.getHostText().equals("localhost"))
                .collect(toImmutableList());
    }

    private static String[] getBlockHosts(BlockLocation blockLocation)
    {
        try {
            return blockLocation.getHosts();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
