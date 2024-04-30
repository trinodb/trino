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
package io.trino.plugin.bigquery.arrow;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class ArenaAllocationManager
        extends AllocationManager
{
    private static final long BYTE_ALIGNMENT = 8;

    private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP,
            null,
            0,
            MemorySegment.NULL.address());

    public static final AllocationManager.Factory FACTORY = new Factory()
    {
        @Override
        public AllocationManager create(BufferAllocator accountingAllocator, long size)
        {
            return new ArenaAllocationManager(accountingAllocator, size);
        }

        @Override
        public ArrowBuf empty()
        {
            return EMPTY;
        }
    };

    private final Arena arena;
    private final long size;
    private final long memoryAddress;

    private ArenaAllocationManager(BufferAllocator accountingAllocator, long requestedSize)
    {
        super(accountingAllocator);
        arena = Arena.ofShared();
        MemorySegment segment = arena.allocate(requestedSize, BYTE_ALIGNMENT);
        memoryAddress = segment.address();
        size = segment.byteSize();
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    protected long memoryAddress()
    {
        return memoryAddress;
    }

    @Override
    protected void release0()
    {
        arena.close();
    }
}
