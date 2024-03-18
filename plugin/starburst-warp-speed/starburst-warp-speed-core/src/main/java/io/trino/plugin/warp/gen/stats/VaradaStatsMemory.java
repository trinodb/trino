
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
public final class VaradaStatsMemory
        extends VaradaStatsBase
{
    /* This class file is auto-generated from memory xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsMemory(@JsonProperty("mem_used_bytes") long mem_used_bytes, @JsonProperty("mem_total_bytes") long mem_total_bytes, @JsonProperty("max_mem_alloc_size") long max_mem_alloc_size, @JsonProperty("min_mem_alloc_size") long min_mem_alloc_size)
    {
        this();
        longStruct.put(0, mem_used_bytes);
        longStruct.put(1, mem_total_bytes);
        longStruct.put(2, max_mem_alloc_size);
        longStruct.put(3, min_mem_alloc_size);
    }

    public VaradaStatsMemory()
    {
        super("memory", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("mem_used_bytes")
    @Managed
    public long getmem_used_bytes()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("mem_total_bytes")
    @Managed
    public long getmem_total_bytes()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("max_mem_alloc_size")
    @Managed
    public long getmax_mem_alloc_size()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("min_mem_alloc_size")
    @Managed
    public long getmin_mem_alloc_size()
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
