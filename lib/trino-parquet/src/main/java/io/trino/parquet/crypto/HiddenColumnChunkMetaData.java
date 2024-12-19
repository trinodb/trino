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
package io.trino.parquet.crypto;

import io.trino.parquet.metadata.ColumnChunkMetadata;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;

public class HiddenColumnChunkMetaData
        extends ColumnChunkMetadata
{
    private final ColumnPath path;
    private final String filePath;

    public HiddenColumnChunkMetaData(ColumnPath path, String filePath)
    {
        super(null, null);
        this.path = path;
        this.filePath = filePath;
    }

    public static boolean isHiddenColumn(ColumnChunkMetadata column)
    {
        return column instanceof HiddenColumnChunkMetaData;
    }

    @Override
    public long getFirstDataPageOffset()
    {
        throw new HiddenColumnException(this.path.toArray(), this.filePath);
    }

    @Override
    public long getDictionaryPageOffset()
    {
        throw new HiddenColumnException(this.path.toArray(), this.filePath);
    }

    @Override
    public long getValueCount()
    {
        throw new HiddenColumnException(this.path.toArray(), this.filePath);
    }

    @Override
    public long getTotalUncompressedSize()
    {
        throw new HiddenColumnException(this.path.toArray(), this.filePath);
    }

    @Override
    public long getTotalSize()
    {
        throw new HiddenColumnException(this.path.toArray(), this.filePath);
    }

    @Override
    public Statistics getStatistics()
    {
        throw new HiddenColumnException(this.path.toArray(), this.filePath);
    }
}
