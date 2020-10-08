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
package io.prestosql.plugin.hive.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ObjectPool<K, T extends Closeable>
        implements Closeable
{
    private final LoadingCache<K, PoolQueue<T>> poolCache;
    private final CacheLoader<K, T> loader;

    public ObjectPool(CacheLoader<K, T> loader, long maximumSize, Duration ttl)
    {
        this.loader = requireNonNull(loader, "loader is null");

        poolCache = CacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterAccess(ttl.toMillis(), TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<K, PoolQueue<T>>) notification -> notification.getValue().close())
                .build(new CacheLoader<>()
                {
                    @Override
                    public PoolQueue<T> load(K key)
                    {
                        return new PoolQueue<>();
                    }
                });
    }

    public Lease<T> get(K key)
    {
        PoolQueue<T> queue = getQueue(key);
        return new Lease<>(queue, queue.pool().orElseGet(() -> load(key)));
    }

    private T load(K key)
    {
        try {
            return loader.load(key);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PoolQueue<T> getQueue(K key)
    {
        try {
            return poolCache.get(key);
        }
        catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public void close()
    {
        poolCache.invalidateAll();
    }

    private static class PoolQueue<T extends Closeable>
            implements Closeable
    {
        @GuardedBy("this")
        private final Queue<T> queue = new ArrayDeque<>();
        private boolean closed;

        public synchronized Optional<T> pool()
        {
            return Optional.ofNullable(queue.poll());
        }

        public synchronized void push(T object)
        {
            if (closed) {
                try {
                    object.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            else {
                queue.add(object);
            }
        }

        @Override
        public synchronized void close()
        {
            closed = true;
            try (Closer closer = Closer.create()) {
                queue.forEach(closer::register);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class Lease<T extends Closeable>
            implements AutoCloseable
    {
        private final PoolQueue<T> queue;
        private final T object;
        private boolean closed;

        public Lease(PoolQueue<T> queue, T object)
        {
            this.queue = requireNonNull(queue, "queue is null");
            this.object = requireNonNull(object, "object is null");
        }

        public T get()
        {
            return object;
        }

        public void invalidate()
        {
            checkState(!closed, "Object is already closed, cannot be invlidated");
            closed = true;
            try {
                object.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close()
        {
            if (!closed) {
                queue.push(object);
            }
            closed = true;
        }
    }
}
