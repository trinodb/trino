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
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;

import java.lang.foreign.MemorySegment;

import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_CODE;
import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_LENGTH;

public class RecordWriteJuffer
        extends BaseWriteJuffer
{
    private final WarmUpElementAllocationParams allocParams;
    private final StorageEngine storageEngine;
    private final long weCookie;
    private int recordBufferEntrySize;            // size of one record, one if its a byte buffer
    private boolean isDictionaryValid;

    public RecordWriteJuffer(BufferAllocator bufferAllocator,
            WarmUpElementAllocationParams allocParams,
            StorageEngine storageEngine,
            long weCookie)
    {
        super(bufferAllocator, JuffersType.RECORD);
        this.allocParams = allocParams;
        this.storageEngine = storageEngine;
        this.weCookie = weCookie;
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        RecTypeCode recTypeCode;
        int recTypeLength;
        baseBuffer = bufferAllocator.memorySegment2RecBuff(buffs);
        if (isDictionaryValid) {
            recTypeCode = DICTIONARY_REC_TYPE_CODE;
            recTypeLength = DICTIONARY_REC_TYPE_LENGTH;
        }
        else {
            recTypeCode = allocParams.recTypeCode();
            recTypeLength = allocParams.recTypeLength();
        }
        this.isDictionaryValid = isDictionaryValid;
        this.wrappedBuffer = createWrapperBuffer(baseBuffer, recTypeCode, recTypeLength, true, isDictionaryValid);
        this.recordBufferEntrySize = calcRecordBufferEntrySize(recTypeCode, recTypeLength);
    }

    public int getRecordBufferEntrySize()
    {
        return recordBufferEntrySize;
    }

    public void resetSingleRecordBufferPos()
    {
        wrappedBuffer.position(0);
    }

    private int calcRecordBufferEntrySize(RecTypeCode recTypeCode, int recTypeLength)
    {
        if (!TypeUtils.isStr(recTypeCode) && ((recTypeLength == Short.BYTES) || (recTypeLength == Integer.BYTES) || (recTypeLength == Long.BYTES))) {
            return recTypeLength;
        }
        return 1;
    }

    public void commitAndResetWE(int numRecs, int nullsCount, int numBytes, long min, long max, int singleOffset)
    {
        // no need to add to chunk map as we are not closing the chunk
        storageEngine.commitRecordBuffer(weCookie, numRecs, nullsCount, numBytes, min, max, singleOffset, false, null);
        resetSingleRecordBufferPos();
    }

    public boolean isDictionaryValid()
    {
        return isDictionaryValid;
    }
}
