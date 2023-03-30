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
import java.util.OptionalInt;

import static io.trino.plugin.deltalake.DataFileInfo.DataFileType.CHANGE_DATA_FEED;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class DeltaLakeCdfPageSink
        extends AbstractDeltaLakePageSink
{
    public static final String CHANGE_TYPE_COLUMN_NAME = "_change_type";
    public static final String CHANGE_DATA_FOLDER_NAME = "_change_data";

    public DeltaLakeCdfPageSink(
            List<DeltaLakeColumnHandle> inputColumns,
            List<String> originalPartitionColumns,
            PageIndexerFactory pageIndexerFactory,
            TrinoFileSystemFactory fileSystemFactory,
            int maxOpenWriters,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            String outputPath,
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
                outputPath,
                session,
                stats,
                trinoVersion);
    }

    @Override
    protected void processSynthesizedColumn(DeltaLakeColumnHandle column) {}

    @Override
    protected void addSpecialColumns(
            List<DeltaLakeColumnHandle> inputColumns,
            ImmutableList.Builder<DeltaLakeColumnHandle> dataColumnHandles,
            ImmutableList.Builder<Integer> dataColumnsInputIndices,
            ImmutableList.Builder<String> dataColumnNames,
            ImmutableList.Builder<Type> dataColumnTypes)
    {
        dataColumnHandles.add(new DeltaLakeColumnHandle(
                CHANGE_TYPE_COLUMN_NAME,
                VARCHAR,
                OptionalInt.empty(),
                CHANGE_TYPE_COLUMN_NAME,
                VARCHAR,
                REGULAR));
        dataColumnsInputIndices.add(inputColumns.size());
        dataColumnNames.add(CHANGE_TYPE_COLUMN_NAME);
        dataColumnTypes.add(VARCHAR);
    }

    @Override
    protected String getPathPrefix()
    {
        return CHANGE_DATA_FOLDER_NAME + "/";
    }

    @Override
    protected DataFileInfo.DataFileType getDataFileType()
    {
        return CHANGE_DATA_FEED;
    }
}
