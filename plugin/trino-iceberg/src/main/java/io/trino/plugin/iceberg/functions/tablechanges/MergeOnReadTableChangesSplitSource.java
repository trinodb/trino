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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MergeOnReadTableChangesSplitSource
        extends AbstractTableChangesSplitSource
{
    private final IncrementalChangelogScan tableScan;
    private final long targetSplitSize;
    private final Closer closer = Closer.create();

    private CloseableIterable<ChangelogScanTask> changelogScanIterable;
    private CloseableIterator<ChangelogScanTask> changelogScanIterator;
    private Iterator<? extends ChangelogScanTask> fileTasksIterator = emptyIterator();

    public MergeOnReadTableChangesSplitSource(
            Table icebergTable,
            IncrementalChangelogScan tableScan)
    {
        super(icebergTable);
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
        return changelogScanIterator != null && !changelogScanIterator.hasNext() && !fileTasksIterator.hasNext();
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

    @Override
    protected ConnectorSplit toIcebergSplit(ChangelogScanTask task)
    {
        TableChangesInternalSplit icebergSplit = (TableChangesInternalSplit) super.toIcebergSplit(task);
        return new TableChangesSplit(
                RowLevelOperationMode.MERGE_ON_READ,
                icebergSplit.partitionDataJson(),
                icebergSplit.partitionSpecJson(),
                ImmutableList.of(icebergSplit));
    }
}
