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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.memory.MemoryErrorCode.MISSING_DATA;

@ThreadSafe
public class MemoryVersionedPagesStore
{
    private final Map<Long, TableData> tables = new HashMap<>();

    public synchronized void initialize(long tableId)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData());
        }
    }

    public synchronized void add(long tableId, long version, int keyColumnIndex, Page page)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        VersionData versionData = tables.get(tableId).getVersionedData(version);
        Block idBlock = page.getBlock(keyColumnIndex);
        for (int position = 0; position < page.getPositionCount(); ++position) {
            if (idBlock.isNull(position)) {
                throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "NULL row id is not supported");
            }
            versionData.insertRow(idBlock.getLong(position, 0), page.getSingleValuePage(position));
        }
    }

    public synchronized void delete(long tableId, long version, Block idBlock)
    {
        VersionData versionData = tables.get(tableId).getVersionedData(version);
        for (int position = 0; position < idBlock.getPositionCount(); ++position) {
            if (idBlock.isNull(position)) {
                throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "NULL row id is not supported");
            }
            versionData.deleteRow(idBlock.getLong(position, 0));
        }
    }

    public synchronized List<Page> getPages(
            long tableId,
            Set<Long> versions,
            List<Integer> columnIndexes)
    {
        if (!contains(tableId)) {
            return ImmutableList.of();
        }
        TableData tableData = tables.get(tableId);

        Map<Long, Page> rows = new HashMap<>();
        versions.stream()
                .sorted()
                .forEach(version -> {
                    VersionData versionData = tableData.getVersionedData(version);
                    versionData.getInsertedRows()
                            .forEach((id, row) -> rows.put(id, row.getColumns(Ints.toArray(columnIndexes))));
                    versionData.getRemovedRows()
                            .forEach(rows::remove);
                });

        return ImmutableList.copyOf(rows.values());
    }

    public synchronized boolean contains(long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when MemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which MemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we cannot determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                tableDataIterator.remove();
            }
        }
    }

    private static final class TableData
    {
        private final Map<Long, VersionData> versionedData = new HashMap<>();

        public VersionData getVersionedData(long version)
        {
            return versionedData.computeIfAbsent(version, ignored -> new VersionData());
        }
    }

    private static final class VersionData
    {
        private final Map<Long, Page> insertedRows = new HashMap<>();
        private final Set<Long> removedRows = new HashSet<>();

        public void insertRow(long rowId, Page row)
        {
            checkArgument(row.getPositionCount() == 1);
            insertedRows.put(rowId, row);
        }

        public void deleteRow(long rowId)
        {
            removedRows.add(rowId);
        }

        public Map<Long, Page> getInsertedRows()
        {
            return insertedRows;
        }

        public Set<Long> getRemovedRows()
        {
            return removedRows;
        }
    }
}
