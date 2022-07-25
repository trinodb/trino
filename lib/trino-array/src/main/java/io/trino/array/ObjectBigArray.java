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
package io.trino.array;

import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.trino.array.BigArrays.INITIAL_SEGMENTS;
import static io.trino.array.BigArrays.SEGMENT_SIZE;
import static io.trino.array.BigArrays.offset;
import static io.trino.array.BigArrays.segment;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
public final class ObjectBigArray<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ObjectBigArray.class).instanceSize();
    private static final long SIZE_OF_SEGMENT = sizeOfObjectArray(SEGMENT_SIZE);

    private final Object initialValue;

    private Object[][] array;
    private long capacity;
    private int segments;

    /**
     * Creates a new big array containing one initial segment
     */
    public ObjectBigArray()
    {
        this(null);
    }

    public ObjectBigArray(Object initialValue)
    {
        this.initialValue = initialValue;
        array = new Object[INITIAL_SEGMENTS][];
        allocateNewSegment();
    }

    /**
     * Returns the current available capacity in this array
     */
    public long getCapacity()
    {
        return capacity;
    }

    /**
     * Returns the size of this big array in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(array) + (segments * SIZE_OF_SEGMENT);
    }

    /**
     * Returns the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return the element of this big array at the specified position.
     */
    @SuppressWarnings("unchecked")
    public T get(long index)
    {
        return (T) array[segment(index)][offset(index)];
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return true if the previous value was null
     */
    public void set(long index, T value)
    {
        array[segment(index)][offset(index)] = value;
    }

    /**
     * Replaces the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return true if the previous value was not null
     */
    public boolean replace(long index, T value)
    {
        Object[] segment = array[segment(index)];

        boolean existed = segment[offset(index)] != null;
        segment[offset(index)] = value;

        return existed;
    }

    /**
     * Ensures this big array is at least the specified length.  If the array is smaller, segments
     * are added until the array is larger then the specified length.
     */
    public void ensureCapacity(long length)
    {
        if (capacity > length) {
            return;
        }

        grow(length);
    }

    /**
     * Fills the entire big array with the specified value.
     */
    public void fill(T value)
    {
        for (Object[] segment : array) {
            if (segment == null) {
                return;
            }
            Arrays.fill(segment, value);
        }
    }

    /**
     * Copies this array, beginning at the specified sourceIndex, to the specified destinationIndex of
     * the destination array. A subsequence of this array's components are copied to the destination
     * array referenced by {@code destination}. The number of components copied is equal to the
     * {@code length} argument. The components at positions {@code sourceIndex} through
     * {@code sourceIndex+length-1} in this array are copied into positions {@code destinationIndex}
     * through {@code destinationIndex+length-1}, respectively, of the destination array.
     */
    public void copyTo(long sourceIndex, ObjectBigArray<T> destination, long destinationIndex, long length)
    {
        while (length > 0) {
            int startSegment = segment(sourceIndex);
            int startOffset = offset(sourceIndex);
            int destinationStartSegment = segment(destinationIndex);
            int destinationStartOffset = offset(destinationIndex);

            int copyLength = Math.min(SEGMENT_SIZE - startOffset, SEGMENT_SIZE - destinationStartOffset);
            copyLength = Math.min(copyLength, length > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) length);

            System.arraycopy(
                    array[startSegment],
                    startOffset,
                    destination.array[destinationStartSegment],
                    destinationStartOffset,
                    copyLength);

            sourceIndex += copyLength;
            destinationIndex += copyLength;
            length -= copyLength;
        }
    }

    private void grow(long length)
    {
        // how many segments are required to get to the length?
        int requiredSegments = segment(length) + 1;

        // grow base array if necessary
        if (array.length < requiredSegments) {
            array = Arrays.copyOf(array, requiredSegments);
        }

        // add new segments
        while (segments < requiredSegments) {
            allocateNewSegment();
        }
    }

    private void allocateNewSegment()
    {
        Object[] newSegment = new Object[SEGMENT_SIZE];
        if (initialValue != null) {
            Arrays.fill(newSegment, initialValue);
        }
        array[segments] = newSegment;
        capacity += SEGMENT_SIZE;
        segments++;
    }
}
