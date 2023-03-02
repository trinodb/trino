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
package io.trino.operator.window.matcher;

import io.airlift.slice.SizeOf;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;

class IntMultimap
{
    private static final long INSTANCE_SIZE = instanceSize(IntMultimap.class);

    private IntList[] values;
    private final int capacity;
    private final int listCapacity;
    private long valuesSize;

    public IntMultimap(int capacity, int listCapacity)
    {
        this.values = new IntList[capacity];
        this.capacity = capacity;
        this.listCapacity = listCapacity;
        this.valuesSize = 0L;
    }

    public void add(int key, int value)
    {
        boolean expanded = ensureCapacity(key);
        long listSizeBefore;
        if (expanded || values[key] == null) {
            listSizeBefore = 0L;
            values[key] = new IntList(listCapacity);
        }
        else {
            listSizeBefore = values[key].getSizeInBytes();
        }
        values[key].add(value);
        valuesSize += values[key].getSizeInBytes() - listSizeBefore;
    }

    public void release(int key)
    {
        if (values[key] != null) {
            valuesSize -= values[key].getSizeInBytes();
            values[key] = null;
        }
    }

    public void copy(int parent, int child)
    {
        boolean expanded = ensureCapacity(child);
        if (expanded || values[child] == null) {
            if (values[parent] != null) {
                values[child] = values[parent].copy();
                valuesSize += values[child].getSizeInBytes();
            }
        }
        else if (values[parent] != null) {
            long listSizeBefore = values[child].getSizeInBytes();
            values[child] = values[parent].copy();
            valuesSize += values[child].getSizeInBytes() - listSizeBefore;
        }
        else {
            valuesSize -= values[child].getSizeInBytes();
            values[child] = null;
        }
    }

    public ArrayView getArrayView(int key)
    {
        if (values[key] == null) {
            return ArrayView.EMPTY;
        }
        return values[key].toArrayView();
    }

    public void clear()
    {
        values = new IntList[capacity];
        valuesSize = 0L;
    }

    // returns true if the array was expanded; otherwise returns false
    private boolean ensureCapacity(int key)
    {
        if (key >= values.length) {
            values = Arrays.copyOf(values, Math.max(values.length * 2, key + 1));
            return true;
        }

        return false;
    }

    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(values) + valuesSize;
    }
}
