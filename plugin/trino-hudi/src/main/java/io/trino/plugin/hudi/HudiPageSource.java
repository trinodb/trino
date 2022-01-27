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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static java.util.Objects.requireNonNull;

public class HudiPageSource
        implements ConnectorPageSource
{
    private final List<HiveColumnHandle> columnHandles;
    private final ConnectorPageSource pageSource;
    private final Map<String, Block> partitionBlocks;

    public HudiPageSource(
            List<HiveColumnHandle> columnHandles,
            Map<String, Block> partitionBlocks,
            ConnectorPageSource pageSource)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.partitionBlocks = requireNonNull(partitionBlocks, "partitionBlocks is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return pageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page page = pageSource.getNextPage();
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
                    blocksWithPartitionColumns[columnIndex++] = new RunLengthEncodedBlock(partitionValue, positionCount);
                }
                else {
                    blocksWithPartitionColumns[columnIndex++] = (page.getBlock(dataColumnIndex));
                    dataColumnIndex++;
                }
            }
            return new Page(positionCount, blocksWithPartitionColumns);
        }
        catch (TrinoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new TrinoException(HUDI_BAD_DATA, e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return pageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        pageSource.close();
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (Exception e) {
            // Self-suppression not permitted
            if (e != throwable) {
                throwable.addSuppressed(e);
            }
        }
    }
}
