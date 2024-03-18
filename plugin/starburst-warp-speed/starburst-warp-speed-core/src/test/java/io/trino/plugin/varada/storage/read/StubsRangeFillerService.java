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
package io.trino.plugin.varada.storage.read;

import com.google.inject.Singleton;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.spi.connector.ConnectorPageSource;

import static io.trino.plugin.warp.gen.constants.RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL;

@Singleton
public class StubsRangeFillerService
        implements RangeFillerService
{
    public StubsRangeFillerService()
    {
    }

    // check if the current chunk is done
    @Override
    public boolean isCurrentChunkCompleted(RangeData rangeData, int chunkSize)
    {
        return true;
    }

    // return the number of rows collected in this round
    @Override
    public int add(int chunkIndex, int currentNumCollectedRows, StorageCollectorArgs storageCollectorArgs, boolean rangesRequired, RangeData rangeData)
    {
        return 0;
    }

    @Override
    public ConnectorPageSource.RowRanges reset(RangeData rangeData)
    {
        return ConnectorPageSource.RowRanges.EMPTY;
    }

    @Override
    public int getNumCollectedFromCurrentChunk(int chunkIndex, RangeData rangeData)
    {
        return 0;
    }

    // list type and size are kept as memebers
    // in type all we store the first row index in the byte array
    // in type all we store the part of the list we have not collected yet in the byte array
    @Override
    public StoreRowListResult storeRowList(StorageCollectorArgs storageCollectorArgs, RangeData rangeData)
    {
        return new StoreRowListResult(RECORD_INDEX_LIST_TYPE_ALL, 0);
    }

    @Override
    public void restoreRowList(RangeData rangeData, int storeRowListSize, RecordIndexListType storeRowListType, byte[] storeRowListBuff)
    {
    }

    @Override
    public ConnectorPageSource.RowRanges collectRanges(RangeData rangeData, int rowsLimit)
    {
        return ConnectorPageSource.RowRanges.EMPTY;
    }
}
