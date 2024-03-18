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

import java.lang.foreign.MemorySegment;
import java.nio.IntBuffer;

/**
 * string varlen skiplist
 */
public class VarlenMdJuffer
        extends BaseWriteJuffer
{
    public VarlenMdJuffer(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator, JuffersType.VARLENMD);
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        IntBuffer intBuffer = bufferAllocator.memorySegment2VarlenMdBuff(buffs);
        intBuffer.position(0);
        intBuffer.put(0, 0);
        wrappedBuffer = intBuffer;
    }

    @Override
    public void reset()
    {
        if (wrappedBuffer != null) {
            IntBuffer varlenMdBuff = (IntBuffer) wrappedBuffer;
            varlenMdBuff.position(0);
            varlenMdBuff.put(0, 0);
        }
    }
}
