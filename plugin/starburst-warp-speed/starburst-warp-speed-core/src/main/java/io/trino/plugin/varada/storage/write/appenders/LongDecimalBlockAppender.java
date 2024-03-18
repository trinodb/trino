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

import io.trino.plugin.varada.dictionary.WriteDictionary;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.spi.type.Int128;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

import static io.trino.spi.type.Int128.SIZE;

public class LongDecimalBlockAppender
        extends DataBlockAppender
{
    public LongDecimalBlockAppender(WriteJuffersWarmUpElement juffersWE)
    {
        super(juffersWE);
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats)
    {
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();

        int nullsCount = 0;
        int recBuffSize = juffersWE.getRecBuffSize();
        int recordsCommitted = 0;
        if (blockPos.mayHaveNull()) {
            int nullsCountCommitted = 0;
            while (blockPos.inRange()) {
                boolean committed = commitWEIfNeeded(blockPos, buff, jufferPos, nullsCount - nullsCountCommitted, recBuffSize);
                if (committed) {
                    recordsCommitted = blockPos.getPos();
                    if (stopAfterOneFlush) {
                        break;
                    }
                    nullsCountCommitted = nullsCount;
                }
                if (blockPos.isNull()) {
                    padBuffer(buff, SIZE);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    Int128 value = (Int128) blockPos.getObject();
                    writeValue(value, buff);
                }

                blockPos.advance();
            }
        }
        else {
            while (blockPos.inRange()) {
                boolean committed = commitWEIfNeeded(blockPos, buff, jufferPos, 0, recBuffSize);
                if (committed) {
                    recordsCommitted = blockPos.getPos();
                    if (stopAfterOneFlush) {
                        break;
                    }
                }
                Int128 value = (Int128) blockPos.getObject();
                writeValue(value, buff);
                blockPos.advance();
            }
        }
        return new AppendResult(nullsCount, recordsCommitted);
    }

    @Override
    public AppendResult appendWithDictionary(BlockPosHolder blockPos, boolean stopAfterOneFlush, WriteDictionary writeDictionary, WarmupElementStats warmupElementStats)
    {
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                if (blockPos.isNull()) {
                    buff.put(ZERO_BYTE_SIGNAL);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    short key = writeDictionary.get(blockPos.getSlice());
                    writeValue(key, buff);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                short key = writeDictionary.get(blockPos.getSlice());
                writeValue(key, buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(Int128 value, ByteBuffer buff)
    {
        juffersWE.updateRecordBufferProps(value.getHigh(), value, buff.position());
        buff.putLong(value.getHigh());
        buff.putLong(value.getLow());
        advanceNullBuff();
    }
}
