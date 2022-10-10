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
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.Page;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakeCDFPageSink
        extends DeltaLakePageSink
{
    public static final String CHANGE_TYPE_COLUMN_NAME = "_change_type";

    private final JsonCodec<DeltaLakeUpdateResult> updateResultJsonCodec;

    private final String tableLocation;

    private final ImmutableList.Builder<DataFileInfo> dataFileInfos = ImmutableList.builder();

    public DeltaLakeCDFPageSink(
            List<DeltaLakeColumnHandle> inputColumns,
            List<String> originalPartitionColumns,
            PageIndexerFactory pageIndexerFactory,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenWriters,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeUpdateResult> updateResultJsonCodec,
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
                maxOpenWriters,
                dataFileInfoCodec,
                outputPath,
                session,
                stats,
                typeManager,
                trinoVersion);

        this.updateResultJsonCodec = requireNonNull(updateResultJsonCodec, "updateResultJsonCodec is null");

        this.tableLocation = tableLocation;
    }

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
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
                "_change_type",
                VARCHAR,
                OptionalInt.empty(),
                "_change_type",
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
        return "_data_change/";
    }

    @Override
    protected Collection<Slice> buildResult()
    {
        List<DataFileInfo> dataFilesInfo = dataFileInfos.build();
        return dataFilesInfo.stream()
                .map(dataFileInfo -> new DeltaLakeUpdateResult("", Optional.of(dataFileInfo), true))
                .map(deltaLakeUpdateResult -> wrappedBuffer(updateResultJsonCodec.toJsonBytes(deltaLakeUpdateResult)))
                .collect(toImmutableList());
    }

    @Override
    protected void collectDataFileInfo(DataFileInfo dataFileInfo)
    {
        dataFileInfos.add(dataFileInfo);
    }
}
