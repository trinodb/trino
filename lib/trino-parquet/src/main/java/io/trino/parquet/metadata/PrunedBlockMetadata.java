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

import com.google.common.collect.ImmutableList;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class PrunedBlockMetadata
{
    private final long rowCount;
    private final ParquetDataSourceId dataSourceId;
    private final Map<ColumnPath, ColumnChunkMetadata> columnMetadataByPath;
    private final BlockMetadata blockMetadata;

    public PrunedBlockMetadata(long rowCount, ParquetDataSourceId dataSourceId, Map<ColumnPath, ColumnChunkMetadata> columnMetadataByPath)
    {
        this.rowCount = rowCount;
        this.dataSourceId = dataSourceId;
        this.columnMetadataByPath = columnMetadataByPath;
        this.blockMetadata = new BlockMetadata(rowCount, ImmutableList.copyOf(columnMetadataByPath.values()));
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public List<ColumnChunkMetadata> getColumns()
    {
        return ImmutableList.copyOf(columnMetadataByPath.values());
    }

    public BlockMetadata getBlockMetadata()
    {
        return blockMetadata;
    }

    public ColumnChunkMetadata getColumnChunkMetaData(ColumnDescriptor columnDescriptor)
            throws ParquetCorruptionException
    {
        ColumnChunkMetadata columnChunkMetadata = columnMetadataByPath.get(ColumnPath.get(columnDescriptor.getPath()));
        if (columnChunkMetadata == null) {
            throw new ParquetCorruptionException(dataSourceId, "Metadata is missing for column: %s", columnDescriptor);
        }
        return columnChunkMetadata;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount)
                .add("columnMetadataByPath", columnMetadataByPath)
                .add("blockMetadata", blockMetadata)
                .toString();
    }
}
