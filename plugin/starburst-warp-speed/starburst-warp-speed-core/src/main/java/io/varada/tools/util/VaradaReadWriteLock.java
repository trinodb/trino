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
package io.varada.tools.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class VaradaReadWriteLock
{
    /* Two mutually exclusive semaphores */
    private final Semaphore writeSemaphore = new VaradaSemaphore(1);

    /* Number of read threads */
    private final AtomicInteger count = new AtomicInteger();

    public VaradaReadWriteLock()
    {
    }

    /**
     * Get read lock
     */
    public boolean readLock()
    {
        boolean ret = true;
        synchronized (writeSemaphore) {
            if (count.incrementAndGet() == 1) {
                ret = writeSemaphore.tryAcquire();
                if (!ret) {
                    count.decrementAndGet();
                }
            }
        }
        return ret;
    }

    /**
     * Release read lock
     */
    public void readUnLock()
    {
        synchronized (writeSemaphore) {
            if ((count.get() > 0) && (count.decrementAndGet() == 0)) {
                writeSemaphore.release();
            }
        }
    }

    /**
     * Get write lock
     */
    public void writeLock()
            throws InterruptedException
    {
        writeSemaphore.acquire();
    }

    /**
     * Release write lock
     */
    public void writeUnlock()
    {
        writeSemaphore.release();
    }

    public int getCount()
    {
        return count.get();
    }
}
