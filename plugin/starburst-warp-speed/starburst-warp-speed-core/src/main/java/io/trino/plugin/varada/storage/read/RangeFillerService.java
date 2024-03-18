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

import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.spi.connector.ConnectorPageSource;

public interface RangeFillerService
{
    // check if the current chunk is done
    boolean isCurrentChunkCompleted(RangeData rangeData, int chunkSize);

    // return the number of rows collected in this round
    int add(int chunkIndex, int currentNumCollectedRows, StorageCollectorArgs storageCollectorArgs, boolean rangesRequired, RangeData rangeData);

    ConnectorPageSource.RowRanges reset(RangeData rangeData);

    int getNumCollectedFromCurrentChunk(int chunkIndex, RangeData rangeData);

    // list type and size are kept as memebers
    // in type all we store the first row index in the byte array
    // in type all we store the part of the list we have not collected yet in the byte array
    StoreRowListResult storeRowList(StorageCollectorArgs storageCollectorArgs, RangeData rangeData);

    void restoreRowList(RangeData rangeData, int storeRowListSize, RecordIndexListType storeRowListType, byte[] storeRowListBuff);

    ConnectorPageSource.RowRanges collectRanges(RangeData rangeData, int rowsLimit);
}
