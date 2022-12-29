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
package io.trino.parquet.reader.flat;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * A structure holding lazily populated binary data and offsets array.
 * <p>
 * The data is stored as a list of Slices that are joined together when needed.
 * This approach performs better if the slices are as big as possible. That is
 * why the cases when the size of the resulting array is known beforehand it is
 * more performant to add a single big byte array to this object rather than
 * add slices one by one.
 * The offset array is compatible with VariableWidthBlock.offsets field, i.e., the
 * first value is always 0 and the last (number of positions + 1) is equal to
 * the last offset + lest position length.
 * <p>
 */
public class BinaryBuffer
{
    private final List<Slice> chunks;
    private final int[] offsets;

    public BinaryBuffer(int valueCount)
    {
        this(new int[valueCount + 1], new ArrayList<>());
    }

    private BinaryBuffer(int[] offsets, List<Slice> chunks)
    {
        this.offsets = requireNonNull(offsets, "offsets is null");
        this.chunks = requireNonNull(chunks, "chunks is null");
    }

    /**
     * Returns a shallow copy of this buffer with empty offsets array.
     * The first offset is set to the value of the last one in the original buffer.
     * It can be used to add data to the original object while offsets land in temporary array
     */
    public BinaryBuffer withTemporaryOffsets(int offset, int offsetCount)
    {
        int[] tmpOffsets = new int[offsetCount + 1];
        tmpOffsets[0] = offsets[offset];
        return new BinaryBuffer(tmpOffsets, chunks);
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

    public void addChunk(Slice slice)
    {
        chunks.add(slice);
    }

    public Slice asSlice()
    {
        if (chunks.size() == 1) {
            return chunks.get(0);
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
