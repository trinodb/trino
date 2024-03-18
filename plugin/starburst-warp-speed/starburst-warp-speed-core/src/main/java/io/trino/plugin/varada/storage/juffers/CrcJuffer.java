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
package io.trino.plugin.varada.storage.juffers;

import io.airlift.log.Logger;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.spi.type.Int128;

import java.lang.foreign.MemorySegment;

import static java.lang.Double.doubleToLongBits;

/**
 * buffer for marking crc values
 */
public class CrcJuffer
        extends BaseWriteJuffer
{
    protected static final Logger logger = Logger.get(CrcJuffer.class);

    public CrcJuffer(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator, JuffersType.CRC);
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        this.baseBuffer = createGenericBuffer(bufferAllocator.memorySegment2CrcBuff(buffs));
        this.wrappedBuffer = this.baseBuffer;
    }

    // return the original position in the crc juffer where we put the value
    public int put(Int128 value, int startPos, BlockPosHolder blockPos)
    {
        int crcJufferPos = baseBuffer.position();
        baseBuffer.putLong(value.getHigh());
        baseBuffer.putLong(value.getLow());
        appendPosition((short) (startPos + blockPos.getPos()));
        value.toBigEndianBytes();
        return crcJufferPos;
    }

    // return the original position in the crc juffer where we put the value
    public int put(byte val, int startPos, BlockPosHolder blockPos)
    {
        int crcJufferPos = baseBuffer.position();
        baseBuffer.put(val);
        appendPosition((short) (startPos + blockPos.getPos()));
        return crcJufferPos;
    }

    // return the original position in the crc juffer where we put the value
    public int put(short val, int startPos, BlockPosHolder blockPos)
    {
        int crcJufferPos = baseBuffer.position();
        baseBuffer.putShort(val);
        appendPosition((short) (startPos + blockPos.getPos()));
        return crcJufferPos;
    }

    // return the original position in the crc juffer where we put the value
    public int put(int val, int startPos, BlockPosHolder blockPos)
    {
        int crcJufferPos = baseBuffer.position();
        baseBuffer.putInt(val);
        appendPosition((short) (startPos + blockPos.getPos()));
        return crcJufferPos;
    }

    // return the original position in the crc juffer where we put the value
    public int put(long val, int startPos, BlockPosHolder blockPos)
    {
        int crcJufferPos = baseBuffer.position();
        baseBuffer.putLong(val);
        appendPosition((short) (startPos + blockPos.getPos()));
        return crcJufferPos;
    }

    // return the original position in the crc juffer where we put the value
    public int put(double val, int startPos, BlockPosHolder blockPos)
    {
        int crcJufferPos = baseBuffer.position();
        baseBuffer.putLong(doubleToLongBits(val));
        appendPosition((short) (startPos + blockPos.getPos()));
        return crcJufferPos;
    }

    private void appendPosition(short value)
    {
        baseBuffer.putShort(value);
    }
}
