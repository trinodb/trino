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
package io.trino.delta;

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.trino.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static io.trino.delta.DeltaErrorCode.DELTA_READ_DATA_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * {@link ConnectorPageSource} implementation for Delta tables that prefills
 * partition column blocks and combines them with regular column blocks returned
 * by the underlying file reader {@link ConnectorPageSource} implementation.
 */
public class DeltaPageSource
        implements ConnectorPageSource
{
    private final List<DeltaColumnHandle> columnHandles;
    private final ConnectorPageSource dataPageSource;
    private final Map<String, Block> partitionValues;

    /**
     * Create a DeltaPageSource
     *
     * @param columnHandles List of columns (includes partition and regular) in order for which data needed in output.
     * @param partitionValues Partition values (partition column -> partition value map).
     * @param dataPageSource Initialized underlying file reader which returns the data for regular columns.
     */
    public DeltaPageSource(
            List<DeltaColumnHandle> columnHandles,
            Map<String, Block> partitionValues,
            ConnectorPageSource dataPageSource)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.dataPageSource = requireNonNull(dataPageSource, "dataPageSource is null");
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = dataPageSource.getNextPage();
            if (dataPage == null) {
                return null; // reader is done
            }
            int positionCount = dataPage.getPositionCount();

            int dataColumnIndex = 0;
            int columnIndex = 0;
            Block[] blocksWithPartitionColumns = new Block[columnHandles.size()];
            for (DeltaColumnHandle columnHandle : columnHandles) {
                if (columnHandle.getColumnType() == PARTITION) {
                    Block partitionValue = partitionValues.get(columnHandle.getName());
                    blocksWithPartitionColumns[columnIndex++] = new RunLengthEncodedBlock(partitionValue, positionCount);
                }
                else {
                    blocksWithPartitionColumns[columnIndex++] = (dataPage.getBlock(dataColumnIndex));
                    dataColumnIndex++;
                }
            }
            return new Page(positionCount, blocksWithPartitionColumns);
        }
        catch (TrinoException e) {
            closeWithSuppression(e);
            throw e; // already properly handled exception - throw without any additional info
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new TrinoException(DELTA_READ_DATA_ERROR, e);
        }
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
    public long getSystemMemoryUsage()
    {
        return dataPageSource.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        dataPageSource.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return dataPageSource.isBlocked();
    }
}
