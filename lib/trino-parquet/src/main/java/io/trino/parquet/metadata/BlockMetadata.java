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
package io.trino.parquet.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BlockMetadata
{
    private final List<ColumnChunkMetadata> columns = new ArrayList<>();
    private long rowCount;
    private long totalByteSize;
    private String path;
    private int ordinal;
    private long rowIndexOffset = -1;

    public void setPath(String path)
    {
        this.path = path;
    }

    public String getPath()
    {
        return path;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(long rowCount)
    {
        this.rowCount = rowCount;
    }

    public long getRowIndexOffset()
    {
        return rowIndexOffset;
    }

    public void setRowIndexOffset(long rowIndexOffset)
    {
        this.rowIndexOffset = rowIndexOffset;
    }

    public long getTotalByteSize()
    {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize)
    {
        this.totalByteSize = totalByteSize;
    }

    public void addColumn(ColumnChunkMetadata column)
    {
        columns.add(column);
    }

    public List<ColumnChunkMetadata> getColumns()
    {
        return Collections.unmodifiableList(columns);
    }

    public long getStartingPos()
    {
        return getColumns().getFirst().getStartingPos();
    }

    @Override
    public String toString()
    {
        String rowIndexOffsetString = "";
        if (rowIndexOffset != -1) {
            rowIndexOffsetString = ", rowIndexOffset = " + rowIndexOffset;
        }
        return "BlockMetaData{" + rowCount + ", " + totalByteSize + rowIndexOffsetString + " " + columns + "}";
    }

    public long getCompressedSize()
    {
        long totalSize = 0;
        for (ColumnChunkMetadata col : getColumns()) {
            totalSize += col.getTotalSize();
        }
        return totalSize;
    }

    public int getOrdinal()
    {
        return ordinal;
    }

    public void setOrdinal(int ordinal)
    {
        this.ordinal = ordinal;
    }
}
