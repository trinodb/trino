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

import io.airlift.json.JsonCodec;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeOperators;

import java.util.List;

import static io.trino.plugin.deltalake.DataFileInfo.DataFileType.CHANGE_DATA_FEED;

public class DeltaLakeCdfPageSink
        extends AbstractDeltaLakePageSink
{
    public static final String CHANGE_TYPE_COLUMN_NAME = "_change_type";
    public static final String CHANGE_DATA_FOLDER_NAME = "_change_data";

    public DeltaLakeCdfPageSink(
            TypeOperators typeOperators,
            List<DeltaLakeColumnHandle> inputColumns,
            List<String> originalPartitionColumns,
            PageIndexerFactory pageIndexerFactory,
            TrinoFileSystemFactory fileSystemFactory,
            int maxOpenWriters,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            Location tableLocation,
            Location outputPath,
            ConnectorSession session,
            DeltaLakeWriterStats stats,
            String trinoVersion,
            DeltaLakeParquetSchemaMapping parquetSchemaMapping)
    {
        super(
                typeOperators,
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
                trinoVersion,
                parquetSchemaMapping);
    }

    @Override
    protected void processSynthesizedColumn(DeltaLakeColumnHandle column) {}

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
