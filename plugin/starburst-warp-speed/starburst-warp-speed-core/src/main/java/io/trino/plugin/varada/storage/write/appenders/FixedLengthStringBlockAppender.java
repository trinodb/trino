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
package io.trino.plugin.varada.storage.write.appenders;

import io.airlift.slice.Slice;
import io.trino.plugin.varada.dictionary.WriteDictionary;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.function.Function;

/**
 * Called for CHARs and for VARCHAR(n) with n<=8
 */
public class FixedLengthStringBlockAppender
        extends DataBlockAppender
{
    private final StorageEngineConstants storageEngineConstants;
    private final int weRecTypeLength;
    private final Type filterType;

    public FixedLengthStringBlockAppender(WriteJuffersWarmUpElement juffersWE,
            StorageEngineConstants storageEngineConstants,
            int weRecTypeLength,
            Type filterType)
    {
        super(juffersWE);
        this.storageEngineConstants = storageEngineConstants;
        this.weRecTypeLength = weRecTypeLength;
        this.filterType = filterType;
    }

    @Override
    public AppendResult appendWithDictionary(BlockPosHolder blockPos, boolean stopAfterOneFlush, WriteDictionary writeDictionary, WarmupElementStats warmupElementStats)
    {
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        int stringLength = weRecTypeLength;
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(filterType,
                stringLength,
                true,
                true);
        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                if (blockPos.isNull()) {
                    buff.put(ZERO_BYTE_SIGNAL);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    Slice slice = blockPos.getSlice();
                    warmupElementStats.updateMinMax(slice);
                    writeValue(writeDictionary, buff, stringLength, sliceConverter.apply(slice));
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                Slice slice = blockPos.getSlice();
                warmupElementStats.updateMinMax(slice);
                writeValue(writeDictionary, buff, stringLength, sliceConverter.apply(slice));
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(WriteDictionary writeDictionary, ShortBuffer buff, int stringLength, Slice value)
    {
        short key = writeDictionary.get(value);
        juffersWE.updateRecordBufferProps(key, stringLength);
        buff.put(key);
        advanceNullBuff();
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats)
    {
        int nullsCount = 0;
        int recBuffSize = juffersWE.getRecBuffSize();
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        int stringLength = weRecTypeLength;
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(filterType,
                stringLength,
                true,
                true);
        int recordsCommitted = 0;
        if (blockPos.mayHaveNull()) {
            int nullsCountCommitted = 0;
            while (blockPos.inRange()) {
                paddPageEnd(buff, stringLength);
                boolean committed = commitWEIfNeeded(blockPos, buff, jufferPos, nullsCount - nullsCountCommitted, recBuffSize);
                if (committed) {
                    recordsCommitted = blockPos.getPos();
                    if (stopAfterOneFlush) {
                        break;
                    }
                    nullsCountCommitted = nullsCount;
                }
                if (blockPos.isNull()) {
                    padBuffer(buff, stringLength);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    Slice slice = blockPos.getSlice();
                    warmupElementStats.updateMinMax(slice);
                    writeValue(buff, stringLength, sliceConverter.apply(slice));
                }
                blockPos.advance();
            }
        }
        else {
            while (blockPos.inRange()) {
                paddPageEnd(buff, stringLength);
                boolean committed = commitWEIfNeeded(blockPos, buff, jufferPos, 0, recBuffSize);
                if (committed) {
                    recordsCommitted = blockPos.getPos();
                    if (stopAfterOneFlush) {
                        break;
                    }
                }
                Slice slice = blockPos.getSlice();
                warmupElementStats.updateMinMax(slice);
                writeValue(buff, stringLength, sliceConverter.apply(slice));
                blockPos.advance();
            }
        }
        return new AppendResult(nullsCount, recordsCommitted);
    }

    private void writeValue(ByteBuffer buff, int stringLength, Slice value)
    {
        ByteBuffer byteBuffer = value.toByteBuffer();
        juffersWE.updateRecordBufferProps(SliceUtils.calcStringValue(byteBuffer, value.length(), stringLength, false),
                byteBuffer, value.length(), buff.position());
        buff.put(byteBuffer);
        advanceNullBuff();
    }

    private void paddPageEnd(ByteBuffer buff, int len)
    {
        // we assume that all buffers are page aligned so even if the position here is global the page alignment is the same
        int currPos = buff.position();
        long endPos = (long) currPos + len;
        long currPage = currPos & storageEngineConstants.getPageSizeMask();
        long endPage = (endPos - 1) & storageEngineConstants.getPageSizeMask();

        // advance to beginning of next page if we are splitted - both char and varchar
        if (currPage != endPage) {
            int paddingSize = storageEngineConstants.getPageSize() - (currPos & storageEngineConstants.getPageOffsetMask());

            padBuffer(buff, paddingSize);
        }
    }
}
