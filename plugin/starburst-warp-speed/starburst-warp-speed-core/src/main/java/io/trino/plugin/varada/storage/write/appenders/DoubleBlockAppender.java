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

import java.nio.LongBuffer;

import static java.util.Objects.requireNonNull;

public class DoubleBlockAppender
        extends DataBlockAppender
{
    public DoubleBlockAppender(WriteJuffersWarmUpElement juffersWE)
    {
        super(juffersWE);
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats)
    {
        LongBuffer buff = requireNonNull((LongBuffer) juffersWE.getRecordBuffer());
        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                if (blockPos.isNull()) {
                    buff.put(0);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    double val = blockPos.getDouble();
                    warmupElementStats.updateMinMax(val);
                    writeValue(val, buff);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                double val = blockPos.getDouble();
                warmupElementStats.updateMinMax(val);
                writeValue(val, buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(double val, LongBuffer buff)
    {
        juffersWE.updateRecordBufferProps(val);
        buff.put(Double.doubleToLongBits(val));
        advanceNullBuff();
    }
}
