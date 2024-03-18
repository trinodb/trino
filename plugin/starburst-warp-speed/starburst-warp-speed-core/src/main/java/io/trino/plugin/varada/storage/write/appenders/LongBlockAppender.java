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

import java.nio.LongBuffer;
import java.nio.ShortBuffer;

public class LongBlockAppender
        extends DataBlockAppender
{
    public LongBlockAppender(WriteJuffersWarmUpElement juffersWE)
    {
        super(juffersWE);
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats)
    {
        LongBuffer longBuffer = (LongBuffer) juffersWE.getRecordBuffer();
        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                if (blockPos.isNull()) {
                    longBuffer.put(0);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    long val = blockPos.getLong();
                    warmupElementStats.updateMinMax(val);
                    writeValue(val, longBuffer);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                long val = blockPos.getLong();
                warmupElementStats.updateMinMax(val);
                writeValue(val, longBuffer);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(long val, LongBuffer longBuffer)
    {
        juffersWE.updateRecordBufferProps(val);
        longBuffer.put(val);
        advanceNullBuff();
    }

    @Override
    public AppendResult appendWithDictionary(BlockPosHolder blockPos,
            boolean stopAfterOneFlush, WriteDictionary writeDictionary, WarmupElementStats warmupElementStats)
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
                    long val = blockPos.getLong();
                    warmupElementStats.updateMinMax(val);
                    short key = writeDictionary.get(val);
                    writeValue(key, buff);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                long val = blockPos.getLong();
                warmupElementStats.updateMinMax(val);
                short key = writeDictionary.get(val);
                writeValue(key, buff);
            }
        }
        return new AppendResult(nullsCount);
    }
}
