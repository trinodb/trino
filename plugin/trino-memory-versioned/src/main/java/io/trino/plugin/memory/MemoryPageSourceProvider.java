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
package io.trino.plugin.memory;

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class MemoryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final MemoryPagesStore pagesStore;
    private final MemoryVersionedPagesStore versionedPagesStore;

    @Inject
    public MemoryPageSourceProvider(MemoryPagesStore pagesStore, MemoryVersionedPagesStore versionedPagesStore)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.versionedPagesStore = requireNonNull(versionedPagesStore, "versionedPagesStore is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        MemorySplit memorySplit = (MemorySplit) split;
        long tableId = memorySplit.getTable();

        List<Integer> columnIndexes = columns.stream()
                .map(MemoryColumnHandle.class::cast)
                .map(MemoryColumnHandle::getColumnIndex).collect(toList());
        MemoryTableHandle tableHandle = (MemoryTableHandle) table;
        List<Page> pages = tableHandle.getVersions()
                .map(versions -> {
                    if (tableHandle.isDeletedRows()) {
                        if (columns.size() != 1) {
                            throw new TrinoException(NOT_SUPPORTED, "Single column must be fetched");
                        }
                        return versionedPagesStore.getDeletedRows(tableId, versions);
                    }
                    else {
                        return versionedPagesStore.getPages(tableId, versions, columnIndexes);
                    }
                })
                .orElseGet(() -> pagesStore.getPages(tableId, columnIndexes));

        ConnectorPageSource pageSource = new FixedPageSource(pages);
        return tableHandle.getUpdateVersion()
                .map(version -> (ConnectorPageSource) new MemoryUpdatablePageSource(new FixedPageSource(pages), versionedPagesStore, tableId, version))
                .orElse(pageSource);
    }
}
