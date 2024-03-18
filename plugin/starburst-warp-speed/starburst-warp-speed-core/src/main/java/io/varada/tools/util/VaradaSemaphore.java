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

import io.airlift.log.Logger;

import java.util.concurrent.Semaphore;

/**
 * same as original semaphore, only we limit the number of permits to the originl amount
 * see java.util.concurrent.Semaphore
 */

public class VaradaSemaphore
        extends Semaphore
{
    private static final Logger logger = Logger.get(VaradaSemaphore.class);

    private final int maxPermits;

    public VaradaSemaphore(int permits)
    {
        super(permits);
        this.maxPermits = permits;
    }

    public VaradaSemaphore(int permits, boolean fair)
    {
        super(permits, fair);
        this.maxPermits = permits;
    }

    @Override
    public void release()
    {
        super.release();
        protectOverflow();
    }

    @Override
    public void release(int permits)
    {
        super.release(permits);
        protectOverflow();
    }

    private void protectOverflow()
    {
        int over = availablePermits() - maxPermits;
        while (over > 0) {
            logger.error("semaphore exceeded max permits %d by %d", maxPermits, over);
            super.tryAcquire(over);
            over = availablePermits() - maxPermits;
        }
    }
}
