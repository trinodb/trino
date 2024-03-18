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

import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.spi.type.Int128;

public class CrcLongDecimalBlockAppender
        extends CrcBlockAppender
{
    public CrcLongDecimalBlockAppender(WriteJuffersWarmUpElement juffersWE)
    {
        super(juffersWE);
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats)
    {
        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            while (blockPos.inRange()) {
                if (blockPos.isNull()) {
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    Int128 value = (Int128) blockPos.getObject();
                    writeValue(value, blockPos, jufferPos);
                }
                blockPos.advance();
            }
        }
        else {
            while (blockPos.inRange()) {
                Int128 value = (Int128) blockPos.getObject();
                writeValue(value, blockPos, jufferPos);
                blockPos.advance();
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(Int128 value, BlockPosHolder blockPos, int jufferPos)
    {
        int crcJufferOffset = crcJuffers.put(value, jufferPos, blockPos);
        juffersWE.updateRecordBufferProps(
                value.getHigh(), // MSB used for min-max
                value,
                crcJufferOffset);      // position of the value in case of single value
        advanceNullBuff();
    }
}
