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
package io.trino.plugin.iceberg.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;

public class CopyOnWriteTableChangesSplitSource
        extends AbstractTableChangesSplitSource
{
    private final Table icebergTable;
    private final IncrementalChangelogScan tableScan;
    private final long targetSplitSize;
    private final Closer closer = Closer.create();

    private CloseableIterable<ChangelogScanTask> changelogScanIterable;
    private CloseableIterator<ChangelogScanTask> changelogScanIterator;
    private Iterator<? extends ChangelogScanTask> fileTasksIterator = emptyIterator();
    private final Queue<ConnectorSplit> splits = new ArrayDeque<>();

    private volatile boolean ready;

    public CopyOnWriteTableChangesSplitSource(
            Table icebergTable,
            IncrementalChangelogScan tableScan)
    {
        super(icebergTable);
        this.icebergTable = requireNonNull(icebergTable, "table is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.targetSplitSize = tableScan.targetSplitSize();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (changelogScanIterable == null) {
            try {
                this.changelogScanIterable = closer.register(tableScan.planFiles());
                this.changelogScanIterator = closer.register(changelogScanIterable.iterator());

                CompletableFuture.runAsync(this::drainChangelogScanIterator);
            }
            catch (UnsupportedOperationException e) {
                throw new TrinoException(NOT_SUPPORTED, "Table uses features which are not yet supported by the table_changes function", e);
            }
        }

        if (!ready) {
            return completedFuture(new ConnectorSplitBatch(ImmutableList.of(), isFinished()));
        }

        List<ConnectorSplit> resultSplits = new ArrayList<>(maxSize);
        while (resultSplits.size() < maxSize && !splits.isEmpty()) {
            resultSplits.add(splits.remove());
        }

        return completedFuture(new ConnectorSplitBatch(resultSplits, isFinished()));
    }

    private void drainChangelogScanIterator()
    {
        PartitionSpec spec = icebergTable.spec();
        Types.StructType partitionType = spec.partitionType();
        Map<PartitionWithOrdinal, List<ConnectorSplit>> changelogScanMap = new HashMap<>();

        while (fileTasksIterator.hasNext() || changelogScanIterator.hasNext()) {
            if (!fileTasksIterator.hasNext()) {
                ChangelogScanTask wholeFileTask = changelogScanIterator.next();
                fileTasksIterator = splitIfPossible(wholeFileTask, targetSplitSize);
                continue;
            }

            ChangelogScanTask next = fileTasksIterator.next();
            ContentScanTask<DataFile> scanTask = (ContentScanTask<DataFile>) next;
            StructLikeWrapper partition = StructLikeWrapper.forType(partitionType).set(scanTask.file().partition());

            changelogScanMap.computeIfAbsent(new PartitionWithOrdinal(partition, next.changeOrdinal()), _ -> new ArrayList<>())
                    .add(toIcebergSplit(next));
        }

        String partitionSchemaJson = PartitionSpecParser.toJson(spec);
        for (Map.Entry<PartitionWithOrdinal, List<ConnectorSplit>> entry : changelogScanMap.entrySet()) {
            PartitionWithOrdinal partitionWithOrdinal = entry.getKey();
            List<ConnectorSplit> changelogSplits = entry.getValue();
            splits.add(new TableChangesSplit(
                    COPY_ON_WRITE,
                    PartitionData.toJson(partitionWithOrdinal.partition().get()),
                    partitionSchemaJson,
                    ImmutableList.copyOf(changelogSplits)));
        }

        ready = true;
    }

    @Override
    public boolean isFinished()
    {
        return ready && splits.isEmpty();
    }

    @Override
    public void close()
    {
        try {
            splits.clear();
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record PartitionWithOrdinal(StructLikeWrapper partition, int changeOrdinal)
    {
        public PartitionWithOrdinal
        {
            requireNonNull(partition, "partition is null");
        }
    }
}
