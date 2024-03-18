
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
public final class VaradaStatsCollecttime
        extends VaradaStatsBase
{
    /* This class file is auto-generated from collecttime xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsCollecttime(@JsonProperty("collect_time_total") long collect_time_total, @JsonProperty("collect_time_1_open") long collect_time_1_open, @JsonProperty("collect_time_2_collect") long collect_time_2_collect, @JsonProperty("collect_time_2_1_decide_op") long collect_time_2_1_decide_op, @JsonProperty("collect_time_2_2_cache_pages") long collect_time_2_2_cache_pages, @JsonProperty("collect_time_2_3_data") long collect_time_2_3_data, @JsonProperty("collect_time_2_4_done") long collect_time_2_4_done, @JsonProperty("collect_time_3_close") long collect_time_3_close)
    {
        this();
        longStruct.put(0, collect_time_total);
        longStruct.put(1, collect_time_1_open);
        longStruct.put(2, collect_time_2_collect);
        longStruct.put(3, collect_time_2_1_decide_op);
        longStruct.put(4, collect_time_2_2_cache_pages);
        longStruct.put(5, collect_time_2_3_data);
        longStruct.put(6, collect_time_2_4_done);
        longStruct.put(7, collect_time_3_close);
    }

    public VaradaStatsCollecttime()
    {
        super("collecttime", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("collect_time_total")
    @Managed
    public long getcollect_time_total()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("collect_time_1_open")
    @Managed
    public long getcollect_time_1_open()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("collect_time_2_collect")
    @Managed
    public long getcollect_time_2_collect()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("collect_time_2_1_decide_op")
    @Managed
    public long getcollect_time_2_1_decide_op()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("collect_time_2_2_cache_pages")
    @Managed
    public long getcollect_time_2_2_cache_pages()
    {
        return longStruct.get(4);
    }

    @JsonIgnore
    @JsonProperty("collect_time_2_3_data")
    @Managed
    public long getcollect_time_2_3_data()
    {
        return longStruct.get(5);
    }

    @JsonIgnore
    @JsonProperty("collect_time_2_4_done")
    @Managed
    public long getcollect_time_2_4_done()
    {
        return longStruct.get(6);
    }

    @JsonIgnore
    @JsonProperty("collect_time_3_close")
    @Managed
    public long getcollect_time_3_close()
    {
        return longStruct.get(7);
    }

    @Override
    public void reset()
    {
        longStruct.put(0, 0);
        longStruct.put(1, 0);
        longStruct.put(2, 0);
        longStruct.put(3, 0);
        longStruct.put(4, 0);
        longStruct.put(5, 0);
        longStruct.put(6, 0);
        longStruct.put(7, 0);
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
        return 8;
    }

    private native ByteBuffer initNative(long limit);
}
