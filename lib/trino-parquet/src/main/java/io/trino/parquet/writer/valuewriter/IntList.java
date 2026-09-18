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
package io.trino.parquet.writer.valuewriter;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.lang.Math.min;

/**
 * Append-only list of ints backed by a growing list of slabs. Slabs never move once allocated, so
 * appends never copy existing data. Slab size doubles from {@code INITIAL_SLAB_SIZE} to
 * {@code MAX_SLAB_SIZE} so small writers stay compact while large writers append into long
 * contiguous runs. Supports append and forward segment iteration only.
 * Based on org.apache.parquet.column.values.dictionary.IntList.
 */
final class IntList
{
    private static final int INSTANCE_SIZE = instanceSize(IntList.class);
    private static final int INITIAL_SLAB_SIZE = 1024;
    private static final int MAX_SLAB_SIZE = 16 * 1024;

    // filled slabs in append order; every entry is fully populated
    private final List<int[]> slabs = new ArrayList<>();
    // slab currently being appended to; null until the first value is added
    private int[] currentSlab;
    private int currentSlabOffset;
    private int currentSlabSize = INITIAL_SLAB_SIZE;
    private int size;

    public void add(int value)
    {
        if (currentSlab == null) {
            currentSlab = new int[currentSlabSize];
        }
        else if (currentSlabOffset == currentSlab.length) {
            slabs.add(currentSlab);
            currentSlabSize = min(currentSlabSize * 2, MAX_SLAB_SIZE);
            currentSlab = new int[currentSlabSize];
            currentSlabOffset = 0;
        }
        currentSlab[currentSlabOffset] = value;
        currentSlabOffset++;
        size++;
    }

    public int size()
    {
        return size;
    }

    /**
     * Invokes {@code consumer} once per slab, in append order, over the exact populated range.
     */
    public void forEachSegment(SegmentConsumer consumer)
    {
        for (int[] slab : slabs) {
            consumer.accept(slab, 0, slab.length);
        }
        if (currentSlabOffset > 0) {
            consumer.accept(currentSlab, 0, currentSlabOffset);
        }
    }

    public long sizeOf()
    {
        long slabsSize = 0;
        for (int[] slab : slabs) {
            slabsSize += sizeOfIntArray(slab.length);
        }
        if (currentSlab != null) {
            slabsSize += sizeOfIntArray(currentSlab.length);
        }
        return INSTANCE_SIZE + sizeOfObjectArray(slabs.size()) + slabsSize;
    }

    public interface SegmentConsumer
    {
        // consumes a contiguous run of length elements starting at offset within segment
        void accept(int[] segment, int offset, int length);
    }
}
