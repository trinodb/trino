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
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class DeltaLakeCDFPageSink
        extends DeltaLakeAbstractPageSink
{
    public static final String CHANGE_TYPE_COLUMN_NAME = "_change_type";
    public static final String CHANGE_DATA_FOLDER_NAME = "_change_data/";

    private final String tableLocation;

    private final ImmutableList.Builder<DataFileInfo> dataFileInfos = ImmutableList.builder();

    public DeltaLakeCDFPageSink(
            List<DeltaLakeColumnHandle> inputColumns,
            List<String> originalPartitionColumns,
            PageIndexerFactory pageIndexerFactory,
            HdfsEnvironment hdfsEnvironment,
            TrinoFileSystemFactory fileSystemFactory,
            int maxOpenWriters,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            String outputPath,
            String tableLocation,
            ConnectorSession session,
            DeltaLakeWriterStats stats,
            TypeManager typeManager,
            String trinoVersion)
    {
        super(
                inputColumns,
                originalPartitionColumns,
                pageIndexerFactory,
                hdfsEnvironment,
                fileSystemFactory,
                maxOpenWriters,
                dataFileInfoCodec,
                outputPath,
                session,
                stats,
                typeManager,
                trinoVersion);

        this.tableLocation = tableLocation;
    }

    @Override
    protected void processSynthesizedColumn(DeltaLakeColumnHandle column) {}

    @Override
    protected void addSpecialColumns(
            List<DeltaLakeColumnHandle> inputColumns,
            ImmutableList.Builder<DeltaLakeColumnHandle> dataColumnHandles,
            ImmutableList.Builder<Integer> dataColumnsInputIndex,
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
        dataColumnsInputIndex.add(inputColumns.size());
        dataColumnNames.add(CHANGE_TYPE_COLUMN_NAME);
        dataColumnTypes.add(VARCHAR);
    }

    @Override
    protected String getRootTableLocation()
    {
        return tableLocation;
    }

    @Override
    protected String getPartitionPrefixPath()
    {
        return CHANGE_DATA_FOLDER_NAME;
    }

    @Override
    protected Collection<Slice> buildResult()
    {
        List<DataFileInfo> dataFilesInfo = dataFileInfos.build();
        return dataFilesInfo.stream()
                .map(file -> new DataFileInfo(file.getPath(), file.getSize(), file.getCreationTime(), file.getPartitionValues(), file.getStatistics(), true))
                .map(dataFileInfo -> wrappedBuffer(dataFileInfoCodec.toJsonBytes(dataFileInfo)))
                .collect(toImmutableList());
    }

    @Override
    protected void collectDataFileInfo(DataFileInfo dataFileInfo)
    {
        dataFileInfos.add(dataFileInfo);
    }
}
