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
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class CrcStringBlockAppender
        extends CrcBlockAppender
{
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final Type filterType;
    private final boolean isFixedLength;

    public CrcStringBlockAppender(WriteJuffersWarmUpElement juffersWE,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            Type filterType,
            boolean isFixedLength)
    {
        super(juffersWE);

        this.storageEngineConstants = storageEngineConstants;
        this.bufferAllocator = bufferAllocator;
        this.filterType = filterType;
        this.isFixedLength = isFixedLength;
    }

    @Override
    public AppendResult appendWithoutDictionary(
            int jufferPos,
            BlockPosHolder blockPos,
            boolean stopAfterOneFlush,
            WarmUpElement warmUpElement,
            WarmupElementStats warmupElementStats)
    {
        int nullsCount = 0;
        // string length must be taken form type since the warm up type length represents the index length (maximum is 8)
        int stringLength = TypeUtils.getTypeLength(filterType, storageEngineConstants.getVarcharMaxLen());

        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(filterType,
                stringLength,
                isFixedLength,
                false);

        for (; blockPos.inRange(); blockPos.advance()) {
            if (blockPos.isNull()) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            else {
                Slice slice = getSlice(blockPos.getSlice());

                if (slice == null) {
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    warmupElementStats.updateMinMax(slice);
                    Slice value = sliceConverter.apply(slice);
                    writeValue(blockPos, jufferPos, stringLength, value);
                }
            }
        }
        return new AppendResult(nullsCount);
    }

    protected Slice getSlice(Slice slice)
    {
        return slice;
    }

    @Override
    protected AppendResult appendFromMapBlock(BlockPosHolder blockPos,
            int jufferPos,
            Object key)
    {
        int stringLength = TypeUtils.getTypeLength(filterType, storageEngineConstants.getVarcharMaxLen());
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(filterType,
                stringLength,
                isFixedLength,
                false);
        int nullsCount = 0;
        Type valueType = ((MapType) blockPos.getType()).getValueType();
        for (; blockPos.inRange(); blockPos.advance()) {
            SqlMap elementBlock = (SqlMap) blockPos.getObject();
            int pos = elementBlock.seekKey(key);
            if (pos == -1 || elementBlock.getRawValueBlock().isNull(elementBlock.getRawOffset() + pos)) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            else {
                Slice slice = valueType.getSlice(elementBlock.getRawValueBlock(), elementBlock.getRawOffset() + pos);
                Slice value = sliceConverter.apply(slice);
                writeValue(blockPos, jufferPos, stringLength, value);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(BlockPosHolder blockPos,
            int jufferPos,
            int stringLength,
            Slice value)
    {
        ByteBuffer byteBuffer = value.toByteBuffer();
        long stringVal = SliceUtils.calcStringValue(byteBuffer, value.length(), stringLength, false);

        int crcJufferOffset;
        int valLength = stringLength;
        long crc;
        // check for optimization of power of two length char
        if (stringLength == 1) {
            byte val = byteBuffer.get();
            crcJufferOffset = crcJuffers.put(val, jufferPos, blockPos);
            crc = val;
        }
        else if (stringLength == 2) {
            short val = bufferAllocator.createBuffView(byteBuffer).getShort();
            crcJufferOffset = crcJuffers.put(val, jufferPos, blockPos);
            crc = val;
        }
        else if (stringLength == 4) {
            int val = bufferAllocator.createBuffView(byteBuffer).getInt();
            crcJufferOffset = crcJuffers.put(val, jufferPos, blockPos);
            crc = val;
        }
        else {
            crc = SliceUtils.calcCrc(byteBuffer, value.length());
            crcJufferOffset = crcJuffers.put(crc, jufferPos, blockPos);
            valLength = Long.BYTES;
        }
        juffersWE.updateRecordBufferProps(stringVal, crc, valLength, crcJufferOffset);

        advanceNullBuff();
    }
}
