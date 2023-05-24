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
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_TYPE_SIGNATURE;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_TYPE_SIGNATURE;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_TYPE_SIGNATURE;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_TYPE;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Objects.requireNonNull;

public class HudiPageSource
        implements ConnectorPageSource
{
    private final Block[] prefilledBlocks;
    private final int[] delegateIndexes;
    private final ConnectorPageSource dataPageSource;

    public HudiPageSource(
            String partitionName,
            List<HiveColumnHandle> columnHandles,
            Map<String, Block> partitionBlocks,
            ConnectorPageSource dataPageSource,
            String path,
            long fileSize,
            long fileModifiedTime)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        this.dataPageSource = requireNonNull(dataPageSource, "dataPageSource is null");

        int size = columnHandles.size();
        this.prefilledBlocks = new Block[size];
        this.delegateIndexes = new int[size];

        int outputIndex = 0;
        int delegateIndex = 0;

        for (HiveColumnHandle column : columnHandles) {
            if (partitionBlocks.containsKey(column.getName())) {
                Block partitionValue = partitionBlocks.get(column.getName());
                prefilledBlocks[outputIndex] = partitionValue;
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(PARTITION_COLUMN_NAME)) {
                prefilledBlocks[outputIndex] = nativeValueToBlock(PARTITION_TYPE_SIGNATURE, utf8Slice(partitionName));
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(PATH_COLUMN_NAME)) {
                prefilledBlocks[outputIndex] = nativeValueToBlock(PATH_TYPE, utf8Slice(path));
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(FILE_SIZE_COLUMN_NAME)) {
                prefilledBlocks[outputIndex] = nativeValueToBlock(FILE_SIZE_TYPE_SIGNATURE, fileSize);
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                long packedTimestamp = packDateTimeWithZone(fileModifiedTime, UTC_KEY);
                prefilledBlocks[outputIndex] = nativeValueToBlock(FILE_MODIFIED_TIME_TYPE_SIGNATURE, packedTimestamp);
                delegateIndexes[outputIndex] = -1;
            }
            else {
                delegateIndexes[outputIndex] = delegateIndex;
                delegateIndex++;
            }
            outputIndex++;
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
    public Page getNextPage()
    {
        try {
            Page page = dataPageSource.getNextPage();
            if (page == null) {
                return null;
            }
            int positionCount = page.getPositionCount();
            Block[] blocks = new Block[prefilledBlocks.length];
            for (int i = 0; i < prefilledBlocks.length; i++) {
                if (prefilledBlocks[i] != null) {
                    blocks[i] = RunLengthEncodedBlock.create(prefilledBlocks[i], positionCount);
                }
                else {
                    blocks[i] = page.getBlock(delegateIndexes[i]);
                }
            }
            return new Page(positionCount, blocks);
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
