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
package io.trino.blob.cache.memory;

import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

@ThreadSafe
public class MemoryBlobCacheStats
{
    private final CounterStat hits = new CounterStat();
    private final CounterStat misses = new CounterStat();
    private final CounterStat largeFilesSkipped = new CounterStat();

    @Managed
    @Nested
    public CounterStat getHits()
    {
        return hits;
    }

    @Managed
    @Nested
    public CounterStat getMisses()
    {
        return misses;
    }

    @Managed
    @Nested
    public CounterStat getLargeFilesSkipped()
    {
        return largeFilesSkipped;
    }

    /**
     * Fraction of lookups served from the cache, over the cache's lifetime.
     */
    @Managed
    public double getHitRatio()
    {
        long hitCount = hits.getTotalCount();
        long total = hitCount + misses.getTotalCount();
        if (total == 0) {
            return 0;
        }
        return (double) hitCount / total;
    }

    void recordHit()
    {
        hits.update(1);
    }

    void recordMiss()
    {
        misses.update(1);
    }

    void recordLargeFileSkipped()
    {
        largeFilesSkipped.update(1);
    }
}
