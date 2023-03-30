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
package io.trino.util;

import io.trino.array.BigArrays;
import io.trino.array.LongBigArray;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongPriorityQueue;

import java.util.NoSuchElementException;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;

/**
 * A type-specific array-based FIFO queue, supporting also deque operations.
 *
 * <p>
 * Instances of this class represent a FIFO queue using a backing array in a
 * circular way. The array is enlarged and shrunk as needed. You can use the
 * {@link #trim()} method to reduce its memory usage, if necessary.
 *
 * <p>
 * This class provides additional methods that implement a <em>deque</em>
 * (double-ended queue).
 */
// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/) LongArrayFIFOQueue
// and mimics that code style.
// Copyright (C) 2010-2019 Sebastiano Vigna
public class LongBigArrayFIFOQueue
        implements LongPriorityQueue
{
    private static final long INSTANCE_SIZE = instanceSize(LongBigArrayFIFOQueue.class);
    /**
     * The standard initial capacity of a queue.
     */
    public static final long INITIAL_CAPACITY = BigArrays.SEGMENT_SIZE;
    /**
     * The backing array.
     */
    protected LongBigArray array;
    /**
     * The current (cached) length of {@link #array}.
     */
    protected long length;
    /**
     * The start position in {@link #array}. It is always strictly smaller than
     * {@link #length}.
     */
    protected long start;
    /**
     * The end position in {@link #array}. It is always strictly smaller than
     * {@link #length}. Might be actually smaller than {@link #start} because
     * {@link #array} is used cyclically.
     */
    protected long end;

    /**
     * Creates a new empty queue with given capacity.
     *
     * @param capacity the initial capacity of this queue.
     */

    public LongBigArrayFIFOQueue(final long capacity)
    {
        if (capacity < 0) {
            throw new IllegalArgumentException("Initial capacity (" + capacity + ") is negative");
        }
        array = new LongBigArray();
        length = Math.max(INITIAL_CAPACITY, capacity); // Never build a queue smaller than INITIAL_CAPACITY
        array.ensureCapacity(length);
    }

    /**
     * Creates a new empty queue with standard {@linkplain #INITIAL_CAPACITY initial
     * capacity}.
     */
    public LongBigArrayFIFOQueue()
    {
        this(INITIAL_CAPACITY);
    }

    public long sizeOf()
    {
        return INSTANCE_SIZE + array.sizeOf();
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation returns {@code null} (FIFO queues have no comparator).
     */
    @Override
    public LongComparator comparator()
    {
        return null;
    }

    @Override
    public long dequeueLong()
    {
        if (start == end) {
            throw new NoSuchElementException();
        }
        final long t = array.get(start);
        if (++start == length) {
            start = 0;
        }
        reduce();
        return t;
    }

    /**
     * Dequeues the {@linkplain PriorityQueue#last() last} element from the queue.
     *
     * @return the dequeued element.
     * @throws NoSuchElementException if the queue is empty.
     */
    public long dequeueLastLong()
    {
        if (start == end) {
            throw new NoSuchElementException();
        }
        if (end == 0) {
            end = length;
        }
        final long t = array.get(--end);
        reduce();
        return t;
    }

    private void resize(final long size, final long newLength)
    {
        final LongBigArray newArray = new LongBigArray();
        newArray.ensureCapacity(newLength);
        if (start >= end) {
            if (size != 0) {
                array.copyTo(start, newArray, 0, length - start);
                array.copyTo(0, newArray, length - start, end);
            }
        }
        else {
            array.copyTo(start, newArray, 0, end - start);
        }
        start = 0;
        end = size;
        array = newArray;
        length = newLength;
    }

    private void expand()
    {
        resize(length, 2L * length);
    }

    private void reduce()
    {
        final long size = longSize();
        if (length > INITIAL_CAPACITY && size <= length / 4) {
            resize(size, length / 2);
        }
    }

    @Override
    public void enqueue(long x)
    {
        array.set(end++, x);
        if (end == length) {
            end = 0;
        }
        if (end == start) {
            expand();
        }
    }

    /**
     * Enqueues a new element as the first element (in dequeuing order) of the
     * queue.
     *
     * @param x the element to enqueue.
     */
    public void enqueueFirst(long x)
    {
        if (start == 0) {
            start = length;
        }
        array.set(--start, x);
        if (end == start) {
            expand();
        }
    }

    @Override
    public long firstLong()
    {
        if (start == end) {
            throw new NoSuchElementException();
        }
        return array.get(start);
    }

    @Override
    public long lastLong()
    {
        if (start == end) {
            throw new NoSuchElementException();
        }
        return array.get((end == 0 ? length : end) - 1);
    }

    @Override
    public void clear()
    {
        end = 0;
        start = 0;
    }

    /**
     * Trims the queue to the smallest possible size.
     */
    public void trim()
    {
        final long size = longSize();
        final LongBigArray newArray = new LongBigArray();
        newArray.ensureCapacity(size + 1);
        if (start <= end) {
            array.copyTo(start, newArray, 0, end - start);
        }
        else {
            array.copyTo(start, newArray, 0, length - start);
            array.copyTo(0, newArray, length - start, end);
        }
        start = 0;
        end = size;
        length = size + 1;
        array = newArray;
    }

    @Override
    public int size()
    {
        return toIntExact(longSize());
    }

    public long longSize()
    {
        final long apparentLength = end - start;
        return apparentLength >= 0 ? apparentLength : length + apparentLength;
    }

    @Override
    public boolean isEmpty()
    {
        return end == start;
    }
}
