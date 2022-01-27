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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.util.Objects.requireNonNull;

public class HudiPageSource
        implements ConnectorPageSource
{
    private final List<HiveColumnHandle> columnHandles;
    private final ConnectorPageSource dataPageSource;
    private final Map<String, Block> partitionBlocks;

    public HudiPageSource(
            List<HiveColumnHandle> columnHandles,
            Map<String, Block> partitionBlocks,
            ConnectorPageSource dataPageSource)
    {
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.partitionBlocks = ImmutableMap.copyOf(requireNonNull(partitionBlocks, "partitionBlocks is null"));
        this.dataPageSource = requireNonNull(dataPageSource, "dataPageSource is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return dataPageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return dataPageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page page = dataPageSource.getNextPage();
            if (page == null) {
                return null;
            }
            int positionCount = page.getPositionCount();

            int dataColumnIndex = 0;
            int columnIndex = 0;
            Block[] blocksWithPartitionColumns = new Block[columnHandles.size()];
            for (HiveColumnHandle columnHandle : columnHandles) {
                if (columnHandle.isPartitionKey()) {
                    Block partitionValue = partitionBlocks.get(columnHandle.getName());
                    blocksWithPartitionColumns[columnIndex] = RunLengthEncodedBlock.create(partitionValue, positionCount);
                }
                else {
                    blocksWithPartitionColumns[columnIndex] = page.getBlock(dataColumnIndex);
                    dataColumnIndex++;
                }

                columnIndex++;
            }
            return new Page(positionCount, blocksWithPartitionColumns);
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, this);
            throw e;
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return dataPageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        dataPageSource.close();
    }
}
