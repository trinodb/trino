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

import com.google.common.io.Closer;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.type.DateTimeEncoding;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SplittableScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.Iterators.singletonIterator;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class TableChangesSplitSource
        implements ConnectorSplitSource
{
    private final Table icebergTable;
    private final IncrementalChangelogScan tableScan;
    private final long targetSplitSize;
    private final Closer closer = Closer.create();

    private CloseableIterable<ChangelogScanTask> changelogScanIterable;
    private CloseableIterator<ChangelogScanTask> changelogScanIterator;
    private Iterator<? extends ChangelogScanTask> fileTasksIterator = emptyIterator();

    public TableChangesSplitSource(
            Table icebergTable,
            IncrementalChangelogScan tableScan)
    {
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
            }
            catch (UnsupportedOperationException e) {
                throw new TrinoException(NOT_SUPPORTED, "Table uses features which are not yet supported by the table_changes function", e);
            }
        }

        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize && (fileTasksIterator.hasNext() || changelogScanIterator.hasNext())) {
            if (!fileTasksIterator.hasNext()) {
                ChangelogScanTask wholeFileTask = changelogScanIterator.next();
                fileTasksIterator = splitIfPossible(wholeFileTask, targetSplitSize);
                continue;
            }

            ChangelogScanTask next = fileTasksIterator.next();
            splits.add(toIcebergSplit(next));
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public boolean isFinished()
    {
        return changelogScanIterator != null && !changelogScanIterator.hasNext();
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Iterator<? extends ChangelogScanTask> splitIfPossible(ChangelogScanTask wholeFileScan, long targetSplitSize)
    {
        if (wholeFileScan instanceof AddedRowsScanTask) {
            return ((SplittableScanTask<AddedRowsScanTask>) wholeFileScan).split(targetSplitSize).iterator();
        }

        if (wholeFileScan instanceof DeletedDataFileScanTask) {
            return ((SplittableScanTask<DeletedDataFileScanTask>) wholeFileScan).split(targetSplitSize).iterator();
        }

        return singletonIterator(wholeFileScan);
    }

    private ConnectorSplit toIcebergSplit(ChangelogScanTask task)
    {
        // TODO: Support DeletedRowsScanTask (requires https://github.com/apache/iceberg/pull/6182)
        if (task instanceof AddedRowsScanTask) {
            return toSplit((AddedRowsScanTask) task);
        }
        else if (task instanceof DeletedDataFileScanTask) {
            return toSplit((DeletedDataFileScanTask) task);
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "ChangelogScanTask type is not supported:" + task);
        }
    }

    private TableChangesSplit toSplit(AddedRowsScanTask task)
    {
        return new TableChangesSplit(
                TableChangesSplit.ChangeType.ADDED_FILE,
                task.commitSnapshotId(),
                DateTimeEncoding.packDateTimeWithZone(icebergTable.snapshot(task.commitSnapshotId()).timestampMillis(), UTC_KEY),
                task.changeOrdinal(),
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().recordCount(),
                IcebergFileFormat.fromIceberg(task.file().format()),
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.file().partition()),
                SplitWeight.standard(),
                icebergTable.io().properties());
    }

    private TableChangesSplit toSplit(DeletedDataFileScanTask task)
    {
        return new TableChangesSplit(
                TableChangesSplit.ChangeType.DELETED_FILE,
                task.commitSnapshotId(),
                DateTimeEncoding.packDateTimeWithZone(icebergTable.snapshot(task.commitSnapshotId()).timestampMillis(), UTC_KEY),
                task.changeOrdinal(),
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().recordCount(),
                IcebergFileFormat.fromIceberg(task.file().format()),
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.file().partition()),
                SplitWeight.standard(),
                icebergTable.io().properties());
    }
}
