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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Throttler<K, V>
{
    private final Map<K, Element<V>> map = new ConcurrentHashMap<>();
    private final Queue<K> pendingQueue = new LinkedList<>();
    private final AtomicInteger runningCount = new AtomicInteger();
    private final int limit;

    public Throttler(int limit)
    {
        this.limit = limit;
    }

    public ListenableFuture<V> registerTask(K key, V value)
    {
        Element<V> res = map.compute(key, (k, v) -> {
            Element<V> element = v;
            if (v == null) {
                pendingQueue.add(k);
                element = new Element<>(value);
            }
            element.counter().incrementAndGet();
            return element;
        });

        process();
        return res.future();
    }

    public void done(K key)
    {
        Element<V> element = map.computeIfPresent(key, (_, value) -> {
            if (value.counter().decrementAndGet() == 0) {
                runningCount.decrementAndGet();
                return null;
            }
            return value;
        });

        if (element == null) {
            process();
        }
    }

    private synchronized void process()
    {
        if (runningCount.get() < limit && !pendingQueue.isEmpty()) {
            map.get(pendingQueue.poll()).ready();
            runningCount.incrementAndGet();
        }
    }

    private record Element<T>(
            AtomicInteger counter,
            SettableFuture<T> future,
            T value)
    {
        public Element(T value)
        {
            this(new AtomicInteger(), SettableFuture.create(), value);
        }

        public void ready()
        {
            this.future.set(value);
        }
    }
}
