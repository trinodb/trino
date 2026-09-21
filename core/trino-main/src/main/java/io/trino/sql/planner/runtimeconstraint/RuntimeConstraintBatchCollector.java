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
package io.trino.sql.planner.runtimeconstraint;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class RuntimeConstraintBatchCollector<K, V>
{
    private final long generation;
    private final Function<V, K> keyFunction;
    private final Map<K, SequencedValue<V>> pending = new HashMap<>();
    private long sequence;
    private long retainedBytes;

    public RuntimeConstraintBatchCollector(long generation, Function<V, K> keyFunction)
    {
        checkArgument(generation >= 0, "generation is negative");
        this.generation = generation;
        this.keyFunction = requireNonNull(keyFunction, "keyFunction is null");
    }

    public synchronized boolean update(V value, long valueRetainedBytes)
    {
        requireNonNull(value, "value is null");
        checkArgument(valueRetainedBytes >= 0, "valueRetainedBytes is negative");
        K key = requireNonNull(keyFunction.apply(value), "value key is null");
        SequencedValue<V> current = pending.get(key);
        if (current != null && current.value().equals(value)) {
            return false;
        }
        long nextSequence = ++sequence;
        if (current != null) {
            retainedBytes -= current.retainedBytes();
        }
        pending.put(key, new SequencedValue<>(nextSequence, value, valueRetainedBytes));
        retainedBytes += valueRetainedBytes;
        return true;
    }

    public synchronized void acknowledge(long acknowledgedSequence)
    {
        checkArgument(acknowledgedSequence >= 0, "acknowledgedSequence is negative");
        pending.values().removeIf(value -> {
            if (value.sequence() > acknowledgedSequence) {
                return false;
            }
            retainedBytes -= value.retainedBytes();
            return true;
        });
    }

    public synchronized List<V> getPendingValues()
    {
        return getPendingBatch(Long.MAX_VALUE, Integer.MAX_VALUE).values();
    }

    public synchronized Batch<V> getPendingBatch(long maxRetainedBytes, int maxValues)
    {
        checkArgument(maxRetainedBytes >= 0, "maxRetainedBytes is negative");
        checkArgument(maxValues > 0, "maxValues must be positive");
        List<SequencedValue<V>> values = pending.values().stream()
                .sorted((left, right) -> Long.compare(left.sequence(), right.sequence()))
                .collect(toImmutableList());
        ImmutableList.Builder<V> batch = ImmutableList.builder();
        long batchRetainedBytes = 0;
        long batchSequence = 0;
        int batchSize = 0;
        for (SequencedValue<V> value : values) {
            if (batchSize >= maxValues || (batchSize > 0 && value.retainedBytes() > maxRetainedBytes - batchRetainedBytes)) {
                break;
            }
            batch.add(value.value());
            batchRetainedBytes += value.retainedBytes();
            batchSequence = value.sequence();
            batchSize++;
        }
        return new Batch<>(batchSequence, batch.build(), batchSize < values.size());
    }

    public synchronized long getSequence()
    {
        return sequence;
    }

    public long getGeneration()
    {
        return generation;
    }

    public synchronized long getRetainedBytes()
    {
        return retainedBytes;
    }

    private record SequencedValue<V>(long sequence, V value, long retainedBytes) {}

    public record Batch<V>(long sequence, List<V> values, boolean hasMore)
    {
        public Batch
        {
            checkArgument(sequence >= 0, "sequence is negative");
            values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
            checkArgument(sequence > 0 || values.isEmpty(), "initial batch contains values");
        }
    }
}
