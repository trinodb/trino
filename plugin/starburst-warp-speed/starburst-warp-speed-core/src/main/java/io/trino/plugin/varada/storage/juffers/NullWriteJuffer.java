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
public class NullWriteJuffer
        extends BaseWriteJuffer
{
    private int nullsCount;

    public NullWriteJuffer(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator, JuffersType.NULL);
    }

    @Override
    public void createBuffer(MemorySegment[] buffs, boolean isDictionaryValid)
    {
        this.baseBuffer = createGenericBuffer(bufferAllocator.memorySegment2NullBuff(buffs));
        this.wrappedBuffer = this.baseBuffer;
    }

    @Override
    public void reset()
    {
        super.reset();
        nullsCount = 0;
    }

    public void increaseNullsCount(int num)
    {
        nullsCount += num;
    }

    public int getNullsCount()
    {
        return nullsCount;
    }
}
