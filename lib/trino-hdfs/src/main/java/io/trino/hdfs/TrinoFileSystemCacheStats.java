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
package io.trino.hdfs;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

public class TrinoFileSystemCacheStats
{
    private final CounterStat getCalls = new CounterStat();
    private final CounterStat getUniqueCalls = new CounterStat();
    private final CounterStat getCallsFailed = new CounterStat();
    private final CounterStat removeCalls = new CounterStat();
    private final LongSupplier cacheSize;

    TrinoFileSystemCacheStats(LongSupplier cacheSize)
    {
        this.cacheSize = requireNonNull(cacheSize, "cacheSize is null");
    }

    @Managed
    public long getCacheSize()
    {
        return cacheSize.getAsLong();
    }

    @Managed
    @Nested
    public CounterStat getGetCalls()
    {
        return getCalls;
    }

    @Managed
    @Nested
    public CounterStat getGetCallsFailed()
    {
        return getCallsFailed;
    }

    @Managed
    @Nested
    public CounterStat getGetUniqueCalls()
    {
        return getUniqueCalls;
    }

    @Managed
    @Nested
    public CounterStat getRemoveCalls()
    {
        return removeCalls;
    }

    public void newGetCall()
    {
        getCalls.update(1);
    }

    public void newGetUniqueCall()
    {
        getUniqueCalls.update(1);
    }

    public void newGetCallFailed()
    {
        getCallsFailed.update(1);
    }

    public void newRemoveCall()
    {
        removeCalls.update(1);
    }
}
