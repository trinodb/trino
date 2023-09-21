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
package io.trino.execution.executor.scheduler;

import io.trino.annotation.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@NotThreadSafe
final class PriorityQueue<T>
{
    // The tree is ordered by priorities in this map, so any operations on the data
    // structures needs to consider the importance of the relative order of the operations.
    // For instance, removing an entry from the tree before the corresponding entry in the
    // queue is removed will lead to NPEs.
    private final Map<T, Priority> priorities = new HashMap<>();
    private final TreeSet<T> queue;

    private long sequence;

    public PriorityQueue()
    {
        queue = new TreeSet<>((a, b) -> {
            Priority first = priorities.get(a);
            Priority second = priorities.get(b);

            int result = Long.compare(first.priority(), second.priority());
            if (result == 0) {
                result = Long.compare(first.sequence(), second.sequence());
            }
            return result;
        });
    }

    public void add(T value, long priority)
    {
        checkArgument(!priorities.containsKey(value), "Value already in queue: %s", value);
        priorities.put(value, new Priority(priority, nextSequence()));
        queue.add(value);
    }

    public void addOrReplace(T value, long priority)
    {
        if (priorities.containsKey(value)) {
            queue.remove(value);
            priorities.put(value, new Priority(priority, nextSequence()));
            queue.add(value);
        }
        else {
            add(value, priority);
        }
    }

    public T takeOrThrow()
    {
        T result = poll();
        checkState(result != null, "Queue is empty");
        return result;
    }

    public T poll()
    {
        T result = queue.pollFirst();
        if (result != null) {
            priorities.remove(result);
        }

        return result;
    }

    public void remove(T value)
    {
        checkArgument(priorities.containsKey(value), "Value not in queue: %s", value);
        queue.remove(value);
        priorities.remove(value);
    }

    public void removeIfPresent(T value)
    {
        if (priorities.containsKey(value)) {
            queue.remove(value);
            priorities.remove(value);
        }
    }

    public boolean contains(T value)
    {
        return priorities.containsKey(value);
    }

    public boolean isEmpty()
    {
        return priorities.isEmpty();
    }

    public Set<T> values()
    {
        return priorities.keySet();
    }

    public long nextPriority()
    {
        checkState(!queue.isEmpty(), "Queue is empty");
        return priorities.get(queue.first()).priority();
    }

    public T peek()
    {
        if (queue.isEmpty()) {
            return null;
        }
        return queue.first();
    }

    public int size()
    {
        return queue.size();
    }

    @Override
    public String toString()
    {
        return queue.toString();
    }

    private long nextSequence()
    {
        return sequence++;
    }

    private record Priority(long priority, long sequence) {}
}
