
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
package io.trino.plugin.warp.gen.stats;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.metrics.VaradaStatType;
import io.trino.plugin.varada.metrics.VaradaStatsBase;
import org.weakref.jmx.Managed;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName"})
public final class VaradaStatsStoragecache
        extends VaradaStatsBase
{
    /* This class file is auto-generated from storagecache xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsStoragecache(@JsonProperty("cache_free_slots") long cache_free_slots, @JsonProperty("cache_stats_hit") long cache_stats_hit, @JsonProperty("cache_stats_miss") long cache_stats_miss, @JsonProperty("cache_stats_total") long cache_stats_total)
    {
        this();
        longStruct.put(0, cache_free_slots);
        longStruct.put(1, cache_stats_hit);
        longStruct.put(2, cache_stats_miss);
        longStruct.put(3, cache_stats_total);
    }

    public VaradaStatsStoragecache()
    {
        super("storagecache", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("cache_free_slots")
    @Managed
    public long getcache_free_slots()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("cache_stats_hit")
    @Managed
    public long getcache_stats_hit()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("cache_stats_miss")
    @Managed
    public long getcache_stats_miss()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("cache_stats_total")
    @Managed
    public long getcache_stats_total()
    {
        return longStruct.get(3);
    }

    @Override
    public void reset()
    {
        longStruct.put(0, 0);
        longStruct.put(1, 0);
        longStruct.put(2, 0);
        longStruct.put(3, 0);
    }

    @Override
    public void merge(VaradaStatsBase varadaStatsBase)
    {
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        return new HashMap<>();
    }

    @Override
    protected Map<String, Object> statePrintFields()
    {
        return new HashMap<>();
    }

    public int getNumberOfMetrics()
    {
        return 4;
    }

    private native ByteBuffer initNative(long limit);
}
