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

import static io.airlift.slice.SizeOf.instanceSize;

// TODO: optimize by
//   - reference counting and copy on write
//   - reuse allocated arrays
class Captures
{
    private static final int INSTANCE_SIZE = instanceSize(Captures.class);

    private final IntMultimap captures;
    private final IntMultimap labels;

    public Captures(int initialCapacity, int slotCount, int labelCount)
    {
        this.captures = new IntMultimap(initialCapacity, slotCount);
        this.labels = new IntMultimap(initialCapacity, labelCount);
    }

    public void save(int threadId, int value)
    {
        captures.add(threadId, value);
    }

    public void saveLabel(int threadId, int value)
    {
        labels.add(threadId, value);
    }

    public void copy(int parent, int child)
    {
        captures.copy(parent, child);
        labels.copy(parent, child);
    }

    public ArrayView getCaptures(int threadId)
    {
        return captures.getArrayView(threadId);
    }

    public ArrayView getLabels(int threadId)
    {
        return labels.getArrayView(threadId);
    }

    public void release(int threadId)
    {
        captures.release(threadId);
        labels.release(threadId);
    }

    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + captures.getSizeInBytes() + labels.getSizeInBytes();
    }
}
