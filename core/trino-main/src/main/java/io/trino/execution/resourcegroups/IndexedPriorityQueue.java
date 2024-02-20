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
package io.trino.execution.resourcegroups;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.transform;
import static io.trino.execution.resourcegroups.IndexedPriorityQueue.PriorityOrdering.HIGH_TO_LOW;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

/**
 * A priority queue with constant time contains(E) and log time remove(E)
 * Ties are broken by insertion order
 */
public final class IndexedPriorityQueue<E>
        implements UpdateablePriorityQueue<E>
{
    public enum PriorityOrdering {
        LOW_TO_HIGH,
        HIGH_TO_LOW
    }

    private final Map<E, Entry<E>> index = new HashMap<>();
    private final Set<Entry<E>> queue;

    private long generation;

    public IndexedPriorityQueue()
    {
        this(HIGH_TO_LOW);
    }

    public IndexedPriorityQueue(PriorityOrdering priorityOrdering)
    {
        queue = switch (priorityOrdering) {
            case LOW_TO_HIGH -> new TreeSet<>(
                    comparingLong((Entry<E> entry) -> entry.getPriority())
                            .thenComparingLong(Entry::getGeneration));
            case HIGH_TO_LOW -> new TreeSet<>((entry1, entry2) -> {
                int priorityComparison = Long.compare(entry2.getPriority(), entry1.getPriority());
                if (priorityComparison != 0) {
                    return priorityComparison;
                }
                return Long.compare(entry1.getGeneration(), entry2.getGeneration());
            });
        };
    }

    @Override
    public boolean addOrUpdate(E element, long priority)
    {
        Entry<E> entry = index.get(element);
        if (entry != null) {
            if (entry.getPriority() == priority) {
                return false;
            }
            queue.remove(entry);
            Entry<E> newEntry = new Entry<>(element, priority, entry.getGeneration());
            queue.add(newEntry);
            index.put(element, newEntry);
            return false;
        }
        Entry<E> newEntry = new Entry<>(element, priority, generation);
        generation++;
        queue.add(newEntry);
        index.put(element, newEntry);
        return true;
    }

    @Override
    public boolean contains(E element)
    {
        return index.containsKey(element);
    }

    @Override
    public boolean remove(E element)
    {
        Entry<E> entry = index.remove(element);
        if (entry != null) {
            queue.remove(entry);
            return true;
        }
        return false;
    }

    @Override
    public E poll()
    {
        Entry<E> entry = pollEntry();
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    public Prioritized<E> getPrioritized(E element)
    {
        Entry<E> entry = index.get(element);
        if (entry == null) {
            return null;
        }

        return new Prioritized<>(entry.getValue(), entry.getPriority());
    }

    public Prioritized<E> pollPrioritized()
    {
        Entry<E> entry = pollEntry();
        if (entry == null) {
            return null;
        }
        return new Prioritized<>(entry.getValue(), entry.getPriority());
    }

    private Entry<E> pollEntry()
    {
        Iterator<Entry<E>> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<E> entry = iterator.next();
        iterator.remove();
        checkState(index.remove(entry.getValue()) != null, "Failed to remove entry from index");
        return entry;
    }

    @Override
    public E peek()
    {
        Entry<E> entry = peekEntry();
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    public Prioritized<E> peekPrioritized()
    {
        Entry<E> entry = peekEntry();
        if (entry == null) {
            return null;
        }
        return new Prioritized<>(entry.getValue(), entry.getPriority());
    }

    public Entry<E> peekEntry()
    {
        Iterator<Entry<E>> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    @Override
    public int size()
    {
        return queue.size();
    }

    @Override
    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    @Override
    public Iterator<E> iterator()
    {
        return transform(queue.iterator(), Entry::getValue);
    }

    private static final class Entry<E>
    {
        private final E value;
        private final long priority;
        private final long generation;

        private Entry(E value, long priority, long generation)
        {
            this.value = requireNonNull(value, "value is null");
            this.priority = priority;
            this.generation = generation;
        }

        public E getValue()
        {
            return value;
        }

        public long getPriority()
        {
            return priority;
        }

        public long getGeneration()
        {
            return generation;
        }
    }

    public static class Prioritized<V>
    {
        private final V value;
        private final long priority;

        public Prioritized(V value, long priority)
        {
            this.value = requireNonNull(value, "value is null");
            this.priority = priority;
        }

        public V getValue()
        {
            return value;
        }

        public long getPriority()
        {
            return priority;
        }
    }
}
