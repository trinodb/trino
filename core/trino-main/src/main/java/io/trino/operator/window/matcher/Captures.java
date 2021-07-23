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

import java.util.Arrays;

// TODO: optimize by
//   - reference counting and copy on write
//   - reuse allocated arrays
class Captures
{
    private final int[][] captures;
    private final int[] usedSlots;
    private final int minSlotCount;

    private final int[][] labels;
    private final int[] usedLabelsSlots;
    private final int minLabelCount;

    public Captures(int threadCount, int slotCount, int labelCount)
    {
        captures = new int[threadCount][];
        usedSlots = new int[threadCount];
        minSlotCount = slotCount;

        labels = new int[threadCount][];
        usedLabelsSlots = new int[threadCount];
        minLabelCount = labelCount;
    }

    public void allocate(int threadId)
    {
        captures[threadId] = new int[minSlotCount];
        labels[threadId] = new int[minLabelCount];
    }

    public void save(int threadId, int value)
    {
        ensureCapturesCapacity(threadId);
        captures[threadId][usedSlots[threadId]] = value;
        usedSlots[threadId]++;
    }

    public void saveLabel(int threadId, int value)
    {
        ensureLabelsCapacity(threadId);
        labels[threadId][usedLabelsSlots[threadId]] = value;
        usedLabelsSlots[threadId]++;
    }

    public void copy(int parent, int child)
    {
        captures[child] = captures[parent].clone();
        usedSlots[child] = usedSlots[parent];
        labels[child] = labels[parent].clone();
        usedLabelsSlots[child] = usedLabelsSlots[parent];
    }

    public ArrayView getCaptures(int threadId)
    {
        return new ArrayView(captures[threadId], usedSlots[threadId]);
    }

    public ArrayView getLabels(int threadId)
    {
        return new ArrayView(labels[threadId], usedLabelsSlots[threadId]);
    }

    public void release(int threadId)
    {
        captures[threadId] = null;
        usedSlots[threadId] = 0;
        labels[threadId] = null;
        usedLabelsSlots[threadId] = 0;
    }

    private void ensureCapturesCapacity(int threadId)
    {
        if (usedSlots[threadId] < captures[threadId].length) {
            return;
        }

        captures[threadId] = Arrays.copyOf(captures[threadId], captures[threadId].length * 2);
    }

    private void ensureLabelsCapacity(int threadId)
    {
        if (usedLabelsSlots[threadId] < labels[threadId].length) {
            return;
        }

        labels[threadId] = Arrays.copyOf(labels[threadId], labels[threadId].length * 2);
    }
}
