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
package io.prestosql.plugin.hive.orc.acid;

import com.google.common.cache.CacheStats;
import org.weakref.jmx.Managed;

public class DeletedRowsCacheStats
{
    @Managed
    public double getHitRate()
    {
        return getCacheStats().hitRate();
    }

    @Managed
    public double getMissRate()
    {
        return getCacheStats().missRate();
    }

    @Managed
    public long getEvictionCount()
    {
        return getCacheStats().evictionCount();
    }

    @Managed
    public double getLoadPenaltySeconds()
    {
        return getCacheStats().averageLoadPenalty() / Math.pow(10, 9);
    }

    @Managed
    public long getHitCount()
    {
        return getCacheStats().hitCount();
    }

    @Managed
    public long getMissCount()
    {
        return getCacheStats().missCount();
    }

    @Managed
    public long getCacheSizeInBytes()
    {
        return DeletedRowsRegistry.getCacheSize();
    }

    private CacheStats getCacheStats()
    {
        return DeletedRowsRegistry.getCacheStats();
    }
}
