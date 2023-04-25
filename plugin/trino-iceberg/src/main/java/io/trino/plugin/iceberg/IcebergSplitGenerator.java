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

import com.google.common.io.Closer;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Collections.emptyIterator;

class IcebergSplitGenerator<ThisT extends Scan<ThisT, ?, ?>>
        implements CloseableIterator<IcebergCommonScanTask>
{
    private CloseableIterable<IcebergCommonScanTask> fileScanTaskIterable;
    private CloseableIterator<IcebergCommonScanTask> fileScanTaskIterator;
    private Iterator<IcebergCommonScanTask> fileTasksIterator;
    private final ThisT scan;
    private final boolean asyncIcebergSplitProducer;

    private final Closer closer = Closer.create();

    public IcebergSplitGenerator(ThisT scan, boolean asyncIcebergSplitProducer)
    {
        this.scan = scan;
        this.asyncIcebergSplitProducer = asyncIcebergSplitProducer;
    }

    // Methods for IcebergSplitSource to do lazy initialization
    public void initialize(Expression filterExpression, boolean includeColumnStats)
    {
        ThisT updatedScan = scan;
        if (!asyncIcebergSplitProducer) {
            updatedScan = updatedScan.planWith(newDirectExecutorService());
        }

        updatedScan = updatedScan.filter(filterExpression);
        if (includeColumnStats) {
            updatedScan = updatedScan.includeColumnStats();
        }

        this.fileScanTaskIterable = CloseableIterable.transform(updatedScan.planFiles(), (f) -> IcebergCommonScanTask.createCommonScanTask(f));
        closer.register(fileScanTaskIterable);
        this.fileScanTaskIterator = fileScanTaskIterable.iterator();
        closer.register(fileScanTaskIterator);
        this.fileTasksIterator = emptyIterator();
    }

    public boolean initialized()
    {
        return fileScanTaskIterable != null;
    }

    public boolean isFinished()
    {
        return fileScanTaskIterator != null && !fileScanTaskIterator.hasNext() && !fileTasksIterator.hasNext();
    }

    public void finish()
    {
        close();
        this.fileScanTaskIterable = CloseableIterable.empty();
        this.fileScanTaskIterator = CloseableIterator.empty();
        this.fileTasksIterator = emptyIterator();
    }

    // Methods from Scan interface
    public Schema schema()
    {
        return scan.schema();
    }

    public long targetSplitSize()
    {
        return scan.targetSplitSize();
    }

    public boolean isEndOfFile()
    {
        return !fileTasksIterator.hasNext();
    }

    // CloseableIterator implementation
    @Override
    public boolean hasNext()
    {
        while (!fileTasksIterator.hasNext() && fileScanTaskIterator.hasNext()) {
            IcebergCommonScanTask wholeFileTask = fileScanTaskIterator.next();
            fileTasksIterator = wholeFileTask.split(targetSplitSize()).iterator();
            // In theory, .split() could produce empty iterator, so let's evaluate the outer loop condition again.
            continue;
        }
        return fileTasksIterator.hasNext();
    }

    @Override
    public IcebergCommonScanTask next()
    {
        return fileTasksIterator.next();
    }

    @Override
    public void remove()
    {
        if (hasNext()) {
            fileTasksIterator.remove();
        }
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

    static IcebergSplitGenerator createSplitGenerator(Table icebergTable, IcebergTableHandle tableHandle, boolean asyncIcebergSplitProducer)
    {
        if (tableHandle.getTableType() == TableType.CHANGES) {
            IncrementalChangelogScan appendScan = icebergTable.newIncrementalChangelogScan();
            if (tableHandle.getStartSnapshotId().isPresent()) {
                appendScan = appendScan.fromSnapshotExclusive(tableHandle.getStartSnapshotId().get());
            }
            if (tableHandle.getSnapshotId().isPresent()) {
                appendScan = appendScan.toSnapshot(tableHandle.getSnapshotId().get());
            }
            return new IcebergSplitGenerator(appendScan, asyncIcebergSplitProducer);
        }
        else {
            TableScan tableScan = icebergTable.newScan();
            if (tableHandle.getSnapshotId().isPresent()) {
                tableScan = tableScan.useSnapshot(tableHandle.getSnapshotId().get());
            }
            return new IcebergSplitGenerator(tableScan, asyncIcebergSplitProducer);
        }
    }
}
