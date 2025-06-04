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
import com.google.common.collect.ImmutableMap;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;

public final class PrunedBlockMetadata
{
    /**
     * Stores only the necessary columns metadata from BlockMetadata and indexes them by path for efficient look-ups
     */
    public static PrunedBlockMetadata createPrunedColumnsMetadata(BlockMetadata blockMetadata, ParquetDataSourceId dataSourceId, Map<List<String>, ColumnDescriptor> descriptorsByPath)
            throws ParquetCorruptionException
    {
        Set<List<String>> requiredPaths = descriptorsByPath.keySet();
        Map<List<String>, ColumnChunkMetadata> columnMetadataByPath = blockMetadata.columns().stream()
                .collect(toImmutableMap(
                        column -> asList(column.getPath().toArray()),
                        identity(),
                        // Same column name may occur more than once when the file is written by case-sensitive tools
                        (oldValue, _) -> oldValue));
        ImmutableMap.Builder<List<String>, ColumnChunkMetadata> columnMetadataByPathBuilder = ImmutableMap.builderWithExpectedSize(requiredPaths.size());
        for (Map.Entry<List<String>, ColumnDescriptor> entry : descriptorsByPath.entrySet()) {
            List<String> requiredPath = entry.getKey();
            ColumnDescriptor columnDescriptor = entry.getValue();
            ColumnChunkMetadata columnChunkMetadata = columnMetadataByPath.get(requiredPath);
            if (columnChunkMetadata == null) {
                throw new ParquetCorruptionException(dataSourceId, "Metadata is missing for column: %s", columnDescriptor);
            }
            columnMetadataByPathBuilder.put(requiredPath, columnChunkMetadata);
        }
        return new PrunedBlockMetadata(blockMetadata.rowCount(), dataSourceId, columnMetadataByPathBuilder.buildOrThrow());
    }

    private final long rowCount;
    private final ParquetDataSourceId dataSourceId;
    private final Map<List<String>, ColumnChunkMetadata> columnMetadataByPath;

    private PrunedBlockMetadata(long rowCount, ParquetDataSourceId dataSourceId, Map<List<String>, ColumnChunkMetadata> columnMetadataByPath)
    {
        this.rowCount = rowCount;
        this.dataSourceId = dataSourceId;
        this.columnMetadataByPath = columnMetadataByPath;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public List<ColumnChunkMetadata> getColumns()
    {
        return ImmutableList.copyOf(columnMetadataByPath.values());
    }

    public ColumnChunkMetadata getColumnChunkMetaData(ColumnDescriptor columnDescriptor)
            throws ParquetCorruptionException
    {
        ColumnChunkMetadata columnChunkMetadata = columnMetadataByPath.get(asList(columnDescriptor.getPath()));
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
                .toString();
    }
}
