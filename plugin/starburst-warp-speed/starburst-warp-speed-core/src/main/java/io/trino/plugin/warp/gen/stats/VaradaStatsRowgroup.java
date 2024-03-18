
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
public final class VaradaStatsRowgroup
        extends VaradaStatsBase
{
    /* This class file is auto-generated from rowgroup xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsRowgroup(@JsonProperty("warm_stats_add_row_group") long warm_stats_add_row_group, @JsonProperty("warm_stats_update_row_group") long warm_stats_update_row_group, @JsonProperty("warm_stats_delete_row_group") long warm_stats_delete_row_group, @JsonProperty("query_stats_query_row_group") long query_stats_query_row_group, @JsonProperty("warm_stats_add_warm_up_element") long warm_stats_add_warm_up_element, @JsonProperty("warm_stats_delete_warm_up_element") long warm_stats_delete_warm_up_element)
    {
        this();
        longStruct.put(0, warm_stats_add_row_group);
        longStruct.put(1, warm_stats_update_row_group);
        longStruct.put(2, warm_stats_delete_row_group);
        longStruct.put(3, query_stats_query_row_group);
        longStruct.put(4, warm_stats_add_warm_up_element);
        longStruct.put(5, warm_stats_delete_warm_up_element);
    }

    public VaradaStatsRowgroup()
    {
        super("rowgroup", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("warm_stats_add_row_group")
    @Managed
    public long getwarm_stats_add_row_group()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_update_row_group")
    @Managed
    public long getwarm_stats_update_row_group()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_delete_row_group")
    @Managed
    public long getwarm_stats_delete_row_group()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("query_stats_query_row_group")
    @Managed
    public long getquery_stats_query_row_group()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_add_warm_up_element")
    @Managed
    public long getwarm_stats_add_warm_up_element()
    {
        return longStruct.get(4);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_delete_warm_up_element")
    @Managed
    public long getwarm_stats_delete_warm_up_element()
    {
        return longStruct.get(5);
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
        return 6;
    }

    private native ByteBuffer initNative(long limit);
}
