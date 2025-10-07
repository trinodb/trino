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

import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.crypto.ParquetCryptoException;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import static java.util.Objects.requireNonNull;

public class HiddenColumnChunkMetadata
        extends ColumnChunkMetadata
{
    private final ParquetDataSourceId dataSourceId;
    private final ColumnPath path;

    public HiddenColumnChunkMetadata(ParquetDataSourceId dataSourceId, ColumnPath path)
    {
        super(null, null);
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        this.path = requireNonNull(path, "path is null");
    }

    @Override
    public ColumnPath getPath()
    {
        return path;
    }

    @Override
    public long getFirstDataPageOffset()
    {
        throw hiddenColumnException();
    }

    @Override
    public long getDictionaryPageOffset()
    {
        throw hiddenColumnException();
    }

    @Override
    public long getValueCount()
    {
        throw hiddenColumnException();
    }

    @Override
    public long getTotalUncompressedSize()
    {
        throw hiddenColumnException();
    }

    @Override
    public long getTotalSize()
    {
        throw hiddenColumnException();
    }

    @Override
    public Statistics getStatistics()
    {
        throw hiddenColumnException();
    }

    public static boolean isHiddenColumn(ColumnChunkMetadata column)
    {
        return column instanceof HiddenColumnChunkMetadata;
    }

    private ParquetCryptoException hiddenColumnException()
    {
        return new ParquetCryptoException(dataSourceId, "User does not have access to column: %s or column key does not exists", path);
    }

    @Override
    public String toString()
    {
        return "HiddenColumnChunkMetadata{dataSourceId=" + dataSourceId + ", path=" + path + "}";
    }
}
