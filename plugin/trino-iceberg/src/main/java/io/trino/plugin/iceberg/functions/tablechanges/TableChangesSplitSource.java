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

import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Table;

import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.iceberg.IcebergUtil.rowLevelOperationMode;

public class TableChangesSplitSource
        implements ConnectorSplitSource
{
    private final ConnectorSplitSource delegate;

    public TableChangesSplitSource(
            Table icebergTable,
            IncrementalChangelogScan tableScan)
    {
        // TODO: handle the splits according to how data was written in each snapshot
        this.delegate = switch (rowLevelOperationMode(icebergTable)) {
            case COPY_ON_WRITE -> new CopyOnWriteTableChangesSplitSource(icebergTable, tableScan);
            case MERGE_ON_READ -> new MergeOnReadTableChangesSplitSource(icebergTable, tableScan);
        };
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return delegate.getNextBatch(maxSize);
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
