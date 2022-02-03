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
package io.trino.execution.executor;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class DriverLimitPerQuery
{
    private static final String ACCESS_TO_UNREFERENCED_INSTANCE_ERROR_MESSAGE = "Attempt to access unreferenced instance";

    private final int maxDriversPerQuery;
    private final AtomicInteger numberOfDrivers;
    private final AtomicInteger referenceCount;

    public DriverLimitPerQuery(int maxDriversPerQuery)
    {
        this.maxDriversPerQuery = maxDriversPerQuery;
        numberOfDrivers = new AtomicInteger(0);
        referenceCount = new AtomicInteger(0);
    }

    public void increase()
    {
        checkSanity();
        checkState(numberOfDrivers.getAndIncrement() >= 0, "numberOfDrivers is a negative number");
    }

    public void decrease()
    {
        checkSanity();
        checkState(numberOfDrivers.decrementAndGet() >= 0, "numberOfDrivers turned into a negative number");
    }

    public void subtract(int delta)
    {
        checkSanity();
        checkArgument(delta > 0, "delta is equal to or less than zero");
        checkState(numberOfDrivers.addAndGet(delta * -1) >= 0, "numberOfDrivers turned into a negative number");
    }

    public boolean isFull()
    {
        checkSanity();
        return numberOfDrivers.get() >= maxDriversPerQuery;
    }

    public void addInitialReference()
    {
        checkState(referenceCount.getAndIncrement() == 0, "referenceCount is non-zero when initialize");
    }

    public void addReference()
    {
        checkState(referenceCount.getAndIncrement() > 0, ACCESS_TO_UNREFERENCED_INSTANCE_ERROR_MESSAGE);
    }

    public boolean dereference()
    {
        int currentCount = referenceCount.decrementAndGet();
        if (currentCount < 0) {
            throw new IllegalStateException(ACCESS_TO_UNREFERENCED_INSTANCE_ERROR_MESSAGE);
        }
        // is unreferenced?
        return currentCount == 0;
    }

    private void checkSanity()
    {
        checkState(referenceCount.get() > 0, ACCESS_TO_UNREFERENCED_INSTANCE_ERROR_MESSAGE);
    }
}
