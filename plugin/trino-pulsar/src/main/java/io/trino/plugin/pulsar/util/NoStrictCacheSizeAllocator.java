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
package io.trino.plugin.pulsar.util;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link CacheSizeAllocator} that will ease cache limit under certain circumstance.
 */
public class NoStrictCacheSizeAllocator
        implements CacheSizeAllocator
{
    private final long maxCacheSize;
    private final LongAdder availableCacheSize;
    private final ReentrantLock lock;

    public NoStrictCacheSizeAllocator(long maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        this.availableCacheSize = new LongAdder();
        this.availableCacheSize.add(maxCacheSize);
        this.lock = new ReentrantLock();
    }

    @Override
    public long getAvailableCacheSize()
    {
        if (availableCacheSize.longValue() < 0) {
            return 0;
        }
        return availableCacheSize.longValue();
    }

    /**
     * This operation will consume available cache size.
     * if the request size exceed the available size, it should be allowed,
     * because maybe single entry size exceed the available cache size and the query should still finish successfully.
     *
     * @param size allocate size
     */
    @Override
    public void allocate(long size)
    {
        try {
            lock.lock();
            availableCacheSize.add(-size);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * This method will release used cache size..
     *
     * @param size release size
     */
    @Override
    public void release(long size)
    {
        try {
            lock.lock();
            availableCacheSize.add(size);
            if (availableCacheSize.longValue() > maxCacheSize) {
                availableCacheSize.reset();
                availableCacheSize.add(maxCacheSize);
            }
        }
        finally {
            lock.unlock();
        }
    }
}
