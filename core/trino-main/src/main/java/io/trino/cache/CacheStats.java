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
package io.trino.cache;

import com.google.errorprone.annotations.MustBeClosed;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import io.airlift.stats.TimeStat.BlockTimer;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class CacheStats
{
    private final CounterStat cacheHits = new CounterStat();
    private final CounterStat cacheMiss = new CounterStat();
    private final DistributionStat cachedData = new DistributionStat();
    private final TimeStat revokeMemoryTime = new TimeStat();

    @Managed
    @Nested
    public CounterStat getCacheHits()
    {
        return cacheHits;
    }

    @Managed
    @Nested
    public CounterStat getCacheMiss()
    {
        return cacheMiss;
    }

    @Managed
    @Nested
    public DistributionStat getCachedData()
    {
        return cachedData;
    }

    @Managed
    @Nested
    public TimeStat getRevokeMemoryTime()
    {
        return revokeMemoryTime;
    }

    public void recordCacheMiss(int count)
    {
        cacheMiss.update(count);
    }

    public void recordCacheHit(int count)
    {
        cacheHits.update(count);
    }

    public void recordCacheData(long memoryUsage)
    {
        cachedData.add(memoryUsage);
    }

    @MustBeClosed
    public BlockTimer recordTime()
    {
        return revokeMemoryTime.time();
    }
}
