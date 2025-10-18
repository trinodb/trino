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
package io.trino.lance.file.v2.reader;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class BinaryBuffer
{
    private final List<Slice> chunks;
    private final int[] offsets;

    public BinaryBuffer(int valueCount)
    {
        this(new ArrayList<>(), new int[valueCount + 1]);
    }

    private BinaryBuffer(List<Slice> chunks, int[] offsets)
    {
        this.offsets = requireNonNull(offsets, "offsets is null");
        this.chunks = requireNonNull(chunks, "chunks is null");
    }

    public void add(byte[] source, int offset)
    {
        add(Slices.wrappedBuffer(source), offset);
    }

    public void add(Slice slice, int offset)
    {
        chunks.add(slice);
        offsets[offset + 1] = offsets[offset] + slice.length();
    }

    public Slice get(int index)
    {
        return chunks.get(index);
    }

    public void addChunk(Slice slice)
    {
        chunks.add(slice);
    }

    public Slice getSlice()
    {
        return asSlice();
    }

    public Slice asSlice()
    {
        if (chunks.size() == 1) {
            return chunks.getFirst();
        }
        int totalLength = 0;
        for (Slice chunk : chunks) {
            totalLength += chunk.length();
        }
        Slice slice = Slices.allocate(totalLength);
        int offset = 0;
        for (Slice chunk : chunks) {
            slice.setBytes(offset, chunk);
            offset += chunk.length();
        }
        chunks.clear();
        chunks.add(slice);
        return slice;
    }

    public int[] getOffsets()
    {
        return offsets;
    }

    public int getValueCount()
    {
        return offsets.length - 1;
    }

    public long getRetainedSize()
    {
        long chunksSizeInBytes = 0;
        for (Slice slice : chunks) {
            chunksSizeInBytes += slice.getRetainedSize();
        }
        return sizeOf(offsets) + chunksSizeInBytes;
    }
}
