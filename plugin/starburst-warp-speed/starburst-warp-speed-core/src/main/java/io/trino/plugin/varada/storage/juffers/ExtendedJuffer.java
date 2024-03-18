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

import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.WarmUpElementAllocationParams;
import io.trino.plugin.varada.storage.engine.StorageEngine;

import java.lang.foreign.MemorySegment;

/**
 * buffer for marking extended strings
 */
public class ExtendedJuffer
        extends BaseWriteJuffer
{
    private final WarmUpElementAllocationParams allocParams;
    private final StorageEngine storageEngine;
    private final long weCookie;
    private int extWESize;                          // extended recs buffer size
    private int extRecordFirstOffset;               // offset of the first extended entry we encountered
    private int extRecordLastPos;                   // extended records last entry address

    public ExtendedJuffer(BufferAllocator bufferAllocator,
            WarmUpElementAllocationParams allocParams,
            StorageEngine storageEngine,
            long weCookie)
    {
        super(bufferAllocator, JuffersType.EXTENDED_REC);
        this.allocParams = allocParams;
        this.storageEngine = storageEngine;
        this.weCookie = weCookie;
        extRecordFirstOffset = -1;
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        this.baseBuffer = createGenericBuffer(bufferAllocator.memorySegment2ExtRecsBuff(buffs));
        this.wrappedBuffer = this.baseBuffer;
        this.extWESize = allocParams.extRecBuffSize();
    }

    protected void commitAndResetExtRecordBuffer(int numExtBytes)
    {
        if (numExtBytes > 0) {
            storageEngine.commitExtRecordBuffer(weCookie, extRecordFirstOffset, numExtBytes);
            resetExtBuf();
        }
    }

    public void commitAndResetExtRecordBuffer()
    {
        if (wrappedBuffer.position() > 0) {
            storageEngine.commitExtRecordBuffer(weCookie, extRecordFirstOffset, wrappedBuffer.position());
        }
        resetExtBuf();
    }

    public void resetExtBuf()
    {
        wrappedBuffer.position(0);
        extRecordFirstOffset = -1;
        extRecordLastPos = 0; // native layer will start looking from the next commit buffer after the invalid
    }

    public int getExtWESize()
    {
        return extWESize;
    }

    public void advancedExtRecordLastPos(int newPosition)
    {
        extRecordLastPos = newPosition;
    }

    public void updateExtRecordFirstOffset(int value)
    {
        if (extRecordFirstOffset == -1) {
            extRecordFirstOffset = value;
        }
    }

    public int getExtRecordLastPos()
    {
        return extRecordLastPos;
    }
}
