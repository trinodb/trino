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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.warp.gen.constants.RecordIndexListHeader;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.spi.connector.ConnectorPageSource;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

@Singleton
public class NativeRangeFillerService
        implements RangeFillerService
{
    private static final Logger logger = Logger.get(NativeRangeFillerService.class);

    private final BufferAllocator bufferAllocator;

    @Inject
    public NativeRangeFillerService(BufferAllocator bufferAllocator)
    {
        this.bufferAllocator = bufferAllocator;
    }

    // check if the current chunk is done
    @Override
    public boolean isCurrentChunkCompleted(RangeData rangeData, int chunkSize)
    {
        // if we have not started collecting the chunk we return false
        if (rangeData.getNumChunkRowsCollected() == 0) {
            return false;
        }
        // we started collecting the chunk so we check how many records are left to be collected, if its zero we return true
        ShortBuffer rowsBuff = bufferAllocator.ids2RowsBuff(rangeData.getRowsBuffId());
        return getTotalNumCollected(rowsBuff, chunkSize) <= rangeData.getNumChunkRowsCollected();
    }

    // return the number of rows collected in this round
    @Override
    public int add(int chunkIndex, int currentNumCollectedRows, StorageCollectorArgs storageCollectorArgs, boolean rangesRequired, RangeData rangeData)
    {
        advanceChunkIfNeeded(chunkIndex, rangeData);

        ShortBuffer rowsBuff = bufferAllocator.ids2RowsBuff(rangeData.getRowsBuffId());

        int numRows;
        if (storageCollectorArgs.queryParams().getNumCollectElements() > 0) {
            numRows = currentNumCollectedRows;
        }
        else {
            numRows = (getListTypeFromBuffer(rowsBuff) == RecordIndexListType.RECORD_INDEX_LIST_TYPE_FULL) ? storageCollectorArgs.chunkSize() : getTotalNumCollected(rowsBuff, storageCollectorArgs.chunkSize());
        }
        // in case collected count is zero, it means nothing was collected regardless of the type
        if (numRows == 0) {
            return 0;
        }

        // if we are here we have at least one row that was collected
        RecordIndexListType listType = getListTypeFromBuffer(rowsBuff);
        int baseRow = chunkIndex * storageCollectorArgs.chunkSize();
        switch (listType) {
            case RECORD_INDEX_LIST_TYPE_FULL -> {
                numRows = storageCollectorArgs.chunkSize();
                if (rangesRequired) {
                    rangeData.addLowerInclusive(baseRow);
                    rangeData.addUpperExclusive(baseRow + numRows);
                }
            }
            case RECORD_INDEX_LIST_TYPE_ALL -> {
                if (rangesRequired) {
                    // start row index lies in the first poition in the list
                    rowsBuff.position(getFirstIndexPosition());
                    // calculate the first row in  the all list is done in short and then transfer to int. its the diff between the last list reached (exclustive) and the size of the list collected
                    int min = baseRow + Short.toUnsignedInt((short) (rowsBuff.get() - currentNumCollectedRows));
                    long minValue = mergeRanges(min, rangeData);
                    rangeData.addLowerInclusive(minValue);
                    rangeData.addUpperExclusive(min + numRows);
                }
            }
            case RECORD_INDEX_LIST_TYPE_VALUES -> {
                if (rangesRequired) {
                    // we take the rows from where we stopped last time
                    rowsBuff.position(getFirstIndexPosition() + rangeData.getNumChunkRowsCollected());

                    int chunkRow = Short.toUnsignedInt(rowsBuff.get());
                    // handle the first row and check if it extends that last range we already have
                    int min = baseRow + chunkRow;
                    int max = min + 1;
                    min = (int) mergeRanges(min, rangeData);

                    // handle all the rest of the rows
                    for (int i = 1; i < numRows; i++) {
                        chunkRow = Short.toUnsignedInt(rowsBuff.get());
                        int row = baseRow + chunkRow;
                        // if we are consectuive - we are in the same range
                        if (row == max) {
                            max++;
                            continue;
                        }
                        // close and add the current range
                        rangeData.addLowerInclusive(min);
                        rangeData.addUpperExclusive(max);
                        // open the next range
                        min = row;
                        max = row + 1;
                    }

                    // add the last range we have
                    rangeData.addLowerInclusive(min);
                    rangeData.addUpperExclusive(max);
                }
            }
            default -> throw new RuntimeException("unknown list type " + listType);
        }

        // update number of rows collected from current chunk
        rangeData.incNumChunkRowsCollected(numRows);
        return numRows;
    }

    @Override
    public ConnectorPageSource.RowRanges reset(RangeData rangeData)
    {
        long[] lowerInclusive = rangeData.getLowerInclusiveAsArray();
        rangeData.clearLowerInclusive();
        long[] upperExclusive = rangeData.getUpperExclusiveAsArray();
        rangeData.clearUpperExclusive();
        return new ConnectorPageSource.RowRanges(lowerInclusive, upperExclusive, false);
    }

    @Override
    public int getNumCollectedFromCurrentChunk(int chunkIndex, RangeData rangeData)
    {
        advanceChunkIfNeeded(chunkIndex, rangeData);
        return rangeData.getNumChunkRowsCollected();
    }

    // list type and size are kept as memebers
    // in type all we store the first row index in the byte array
    // in type all we store the part of the list we have not collected yet in the byte array
    @Override
    public StoreRowListResult storeRowList(StorageCollectorArgs storageCollectorArgs, RangeData rangeData)
    {
        ShortBuffer rowsBuff = bufferAllocator.ids2RowsBuff(rangeData.getRowsBuffId());

        int currChunkIndex = storageCollectorArgs.chunksQueue().getCurrent();
        advanceChunkIfNeeded(currChunkIndex, rangeData);

        RecordIndexListType storeRowListType = getListTypeFromBuffer(rowsBuff);
        byte[] storeRowListBuff = storageCollectorArgs.storeRowListBuff();
        int storeRowListSize;
        switch (storeRowListType) {
            case RECORD_INDEX_LIST_TYPE_FULL:
                storeRowListSize = storageCollectorArgs.chunkSize();
                break;
            case RECORD_INDEX_LIST_TYPE_ALL:
                int posTypeAll = getFirstIndexPosition() * Short.BYTES;
                int totalNumCollectedTypeAll = getTotalNumCollected(rowsBuff, storageCollectorArgs.chunkSize());
                storeRowListSize = totalNumCollectedTypeAll - rangeData.getNumChunkRowsCollected();
                if (storeRowListCommon(storeRowListSize, posTypeAll, false, storageCollectorArgs.chunkSize(), rangeData, storeRowListBuff)) {
                    storeRowListType = RecordIndexListType.RECORD_INDEX_LIST_TYPE_FULL;
                }
                break;
            case RECORD_INDEX_LIST_TYPE_VALUES:
                int posListTypeValues = (getFirstIndexPosition() + rangeData.getNumChunkRowsCollected()) * Short.BYTES;
                int totalNumCollectedTypeValues = getTotalNumCollected(rowsBuff, storageCollectorArgs.chunkSize());
                storeRowListSize = totalNumCollectedTypeValues - rangeData.getNumChunkRowsCollected();
                if (storeRowListCommon(storeRowListSize, posListTypeValues, true, storageCollectorArgs.chunkSize(), rangeData, storeRowListBuff)) {
                    storeRowListType = RecordIndexListType.RECORD_INDEX_LIST_TYPE_FULL;
                }
                break;
            default:
                throw new RuntimeException("unknown list type " + storeRowListType);
        }
        logger.debug("storeRowList lastChunkIndex %d type %s size %d", rangeData.getLastChunkIndex(), storeRowListType, storeRowListSize);
        return new StoreRowListResult(storeRowListType, storeRowListSize);
    }

    private boolean storeRowListCommon(int storeRowListSize, int pos, boolean fullCopy, int chunkSize, RangeData rangeData, byte[] storeRowListBuf)
    {
        if (storeRowListSize == 0) {
            throw new RuntimeException("unexpected empty list to store");
        }
        if (storeRowListSize == chunkSize) {
            return true;
        }
        ByteBuffer bufferToCopy = bufferAllocator.id2ByteBuff(rangeData.getRowsBuffId()).slice().position(pos);
        bufferToCopy.get(storeRowListBuf, 0, (fullCopy ? storeRowListSize : 1) * Short.BYTES);
        return false;
    }

    @Override
    public void restoreRowList(RangeData rangeData, int storeRowListSize, RecordIndexListType storeRowListType, byte[] storeRowListBuff)
    {
        ShortBuffer rowsBuff = bufferAllocator.ids2RowsBuff(rangeData.getRowsBuffId());
        // list type and size was store as a member, we put it in the buffer
        // in case of all type we store in the buffer the first row
        // in case of values type we copy all the values to the buffer
        logger.debug("restoreRowList type %s size %d", storeRowListType, storeRowListSize);
        setListTypeInBuffer(rowsBuff, storeRowListType);
        switch (storeRowListType) {
            case RECORD_INDEX_LIST_TYPE_FULL:
                setTotalNumCollected(rowsBuff, storeRowListSize);
                break;
            case RECORD_INDEX_LIST_TYPE_ALL:
                restoreRowListCommon(rowsBuff, Short.BYTES, rangeData, storeRowListSize, storeRowListBuff);
                break;
            case RECORD_INDEX_LIST_TYPE_VALUES:
                restoreRowListCommon(rowsBuff, storeRowListSize * Short.BYTES, rangeData, storeRowListSize, storeRowListBuff);
                break;
            default:
                throw new RuntimeException("unknown list type " + storeRowListType);
        }
    }

    private void restoreRowListCommon(ShortBuffer rowsBuff, int sizeInBytes, RangeData rangeData, int storeRowListSize, byte[] storeRowListBuff)
    {
        setTotalNumCollected(rowsBuff, storeRowListSize);
        ByteBuffer targetByteBuffer = bufferAllocator.id2ByteBuff(rangeData.getRowsBuffId()).slice().position(getFirstIndexPosition() * Short.BYTES);
        targetByteBuffer.put(storeRowListBuff, 0, sizeInBytes);
    }

    private RecordIndexListType getListTypeFromBuffer(ShortBuffer rowsBuff)
    {
        return RecordIndexListType.values()[rowsBuff.get(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal())];
    }

    private void setListTypeInBuffer(ShortBuffer rowsBuff, RecordIndexListType listType)
    {
        rowsBuff.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) listType.ordinal());
    }

    private int getFirstIndexPosition()
    {
        return RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_NUM_OF.ordinal();
    }

    // since the size is a short, zero means a full chunk, we translate to integer here
    private int getTotalNumCollected(ShortBuffer rowsBuff, int chunkSize)
    {
        int total = Short.toUnsignedInt(rowsBuff.get(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TOTAL_SIZE.ordinal()));
        return (total > 0) ? total : chunkSize;
    }

    // set num collected can never be actual zero since we already collected something before we store the state of this chunk
    // thus, the integer passed here can be safely be casted to short
    private void setTotalNumCollected(ShortBuffer rowsBuff, int totalSize)
    {
        rowsBuff.put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TOTAL_SIZE.ordinal(), (short) totalSize);
    }

    private void advanceChunkIfNeeded(int chunkIndex, RangeData rangeData)
    {
        if (rangeData.getLastChunkIndex() != chunkIndex) {
            rangeData.setLastChunkIndex(chunkIndex);
            rangeData.resetNumChunkRowsCollected();
        }
    }

    // in case merge was successful, removes the previous range and returns its min, otherwise return the input min
    private long mergeRanges(long min, RangeData rangeData)
    {
        if (rangeData.getUpperExclusiveSize() > 0 && rangeData.getUpperExclusiveValue(rangeData.getUpperExclusiveSize() - 1) == min) {
            min = rangeData.removeLowerInclusive(rangeData.getLowerInclusiveSize() - 1);
            rangeData.removeUpperExclusive(rangeData.getUpperExclusiveSize() - 1);
        }
        return min;
    }

    /**
     * inclusive min, exclusive max (etc: Range = [4-6], need to take rows 4,5)
     *
     * @return rangesCount - ranges in juffer
     */
    @Override
    public ConnectorPageSource.RowRanges collectRanges(RangeData rangeData, int rowsLimit)
    {
        ConnectorPageSource.RowRanges ranges = reset(rangeData);
        if (rowsLimit == Integer.MAX_VALUE) {
            return ranges;
        }

        LongArrayList limitedLowerInclusive = new LongArrayList();
        LongArrayList limitedUpperExclusive = new LongArrayList();
        int sum = 0;
        int currRange = 0;

        while (currRange < ranges.getRangesCount() && (sum < rowsLimit)) {
            int min = (int) ranges.getLowerInclusive(currRange);
            int max = (int) ranges.getUpperExclusive(currRange);
            sum += max - min;
            if (sum > rowsLimit) {
                max -= (sum - rowsLimit);
                sum = rowsLimit;
            }
            limitedLowerInclusive.add(min);
            limitedUpperExclusive.add(max);
            currRange++;
        }
        return new ConnectorPageSource.RowRanges(limitedLowerInclusive.toLongArray(), limitedUpperExclusive.toLongArray(), false);
    }
}
