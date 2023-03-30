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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.plugin.deltalake.DataFileInfo.DataFileType.DATA;

public class DeltaLakePageSink
        extends AbstractDeltaLakePageSink
{
    public DeltaLakePageSink(
            List<DeltaLakeColumnHandle> inputColumns,
            List<String> originalPartitionColumns,
            PageIndexerFactory pageIndexerFactory,
            TrinoFileSystemFactory fileSystemFactory,
            int maxOpenWriters,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            String tableLocation,
            ConnectorSession session,
            DeltaLakeWriterStats stats,
            String trinoVersion)
    {
        super(
                inputColumns,
                originalPartitionColumns,
                pageIndexerFactory,
                fileSystemFactory,
                maxOpenWriters,
                dataFileInfoCodec,
                tableLocation,
                tableLocation,
                session,
                stats,
                trinoVersion);
    }

    @Override
    protected void processSynthesizedColumn(DeltaLakeColumnHandle column)
    {
        throw new IllegalStateException("Unexpected column type: " + column.getColumnType());
    }

    @Override
    protected void addSpecialColumns(
            List<DeltaLakeColumnHandle> inputColumns,
            ImmutableList.Builder<DeltaLakeColumnHandle> dataColumnHandles,
            ImmutableList.Builder<Integer> dataColumnsInputIndex,
            ImmutableList.Builder<String> dataColumnNames,
            ImmutableList.Builder<Type> dataColumnTypes)
    {}

    @Override
    protected String getPathPrefix()
    {
        return "";
    }

    @Override
    protected DataFileInfo.DataFileType getDataFileType()
    {
        return DATA;
    }
}
