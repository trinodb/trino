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

/**
 * buffer for marking null values
 */
public class ChunksMapJuffer
        extends BaseWriteJuffer
{
    public ChunksMapJuffer(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator, JuffersType.CHUNKS_MAP);
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        this.baseBuffer = createGenericBuffer(bufferAllocator.memorySegment2ChunksMapBuff(buffs));
        this.wrappedBuffer = this.baseBuffer;
    }
}
