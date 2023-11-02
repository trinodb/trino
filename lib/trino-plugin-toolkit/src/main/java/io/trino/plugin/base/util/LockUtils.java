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
package io.trino.plugin.base.util;

import java.util.concurrent.locks.Lock;

import static java.util.Objects.requireNonNull;

/**
 * LockUtils.closeable allows locks to be used in try-with-resources blocks:
 *
 * <pre>{@code
 *      ReentrantResourceLock lock = new ReentrantResourceLock();
 *      try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
 *          // critical section
 *      }
 * }</pre>
 */
public final class LockUtils
{
    private LockUtils() {}

    public static <T extends Lock> CloseableLock<T> closeable(T lock)
    {
        lock.lock();
        return new CloseableLock<>(lock);
    }

    public record CloseableLock<T extends Lock>(T lock)
            implements AutoCloseable
    {
        public CloseableLock
        {
            requireNonNull(lock, "lock is null");
        }

        @Override
        public void close()
        {
            lock.unlock();
        }
    }
}
