
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
public final class VaradaStatsQueryresult
        extends VaradaStatsBase
{
    /* This class file is auto-generated from queryresult xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsQueryresult(@JsonProperty("query_stats_raw_to_raw") long query_stats_raw_to_raw, @JsonProperty("query_stats_raw_to_bool") long query_stats_raw_to_bool, @JsonProperty("query_stats_raw_to_single") long query_stats_raw_to_single, @JsonProperty("query_stats_raw_to_singlenonull") long query_stats_raw_to_singlenonull, @JsonProperty("query_stats_raw_to_allnull") long query_stats_raw_to_allnull, @JsonProperty("query_stats_total_raw_to") long query_stats_total_raw_to, @JsonProperty("query_stats_single_to_raw") long query_stats_single_to_raw, @JsonProperty("query_stats_single_to_single") long query_stats_single_to_single, @JsonProperty("query_stats_single_to_singlenonull") long query_stats_single_to_singlenonull, @JsonProperty("query_stats_total_single_to") long query_stats_total_single_to, @JsonProperty("query_stats_singlenonull_to_raw") long query_stats_singlenonull_to_raw, @JsonProperty("query_stats_singlenonull_to_single") long query_stats_singlenonull_to_single, @JsonProperty("query_stats_singlenonull_to_singlenonull") long query_stats_singlenonull_to_singlenonull, @JsonProperty("query_stats_total_singlenonull_to") long query_stats_total_singlenonull_to, @JsonProperty("query_stats_allnull_to_raw") long query_stats_allnull_to_raw, @JsonProperty("query_stats_allnull_to_single") long query_stats_allnull_to_single, @JsonProperty("query_stats_allnull_to_allnull") long query_stats_allnull_to_allnull, @JsonProperty("query_stats_total_allnull_to") long query_stats_total_allnull_to)
    {
        this();
        longStruct.put(0, query_stats_raw_to_raw);
        longStruct.put(1, query_stats_raw_to_bool);
        longStruct.put(2, query_stats_raw_to_single);
        longStruct.put(3, query_stats_raw_to_singlenonull);
        longStruct.put(4, query_stats_raw_to_allnull);
        longStruct.put(5, query_stats_total_raw_to);
        longStruct.put(6, query_stats_single_to_raw);
        longStruct.put(7, query_stats_single_to_single);
        longStruct.put(8, query_stats_single_to_singlenonull);
        longStruct.put(9, query_stats_total_single_to);
        longStruct.put(10, query_stats_singlenonull_to_raw);
        longStruct.put(11, query_stats_singlenonull_to_single);
        longStruct.put(12, query_stats_singlenonull_to_singlenonull);
        longStruct.put(13, query_stats_total_singlenonull_to);
        longStruct.put(14, query_stats_allnull_to_raw);
        longStruct.put(15, query_stats_allnull_to_single);
        longStruct.put(16, query_stats_allnull_to_allnull);
        longStruct.put(17, query_stats_total_allnull_to);
    }

    public VaradaStatsQueryresult()
    {
        super("queryresult", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("query_stats_raw_to_raw")
    @Managed
    public long getquery_stats_raw_to_raw()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("query_stats_raw_to_bool")
    @Managed
    public long getquery_stats_raw_to_bool()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("query_stats_raw_to_single")
    @Managed
    public long getquery_stats_raw_to_single()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("query_stats_raw_to_singlenonull")
    @Managed
    public long getquery_stats_raw_to_singlenonull()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("query_stats_raw_to_allnull")
    @Managed
    public long getquery_stats_raw_to_allnull()
    {
        return longStruct.get(4);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_raw_to")
    @Managed
    public long getquery_stats_total_raw_to()
    {
        return longStruct.get(5);
    }

    @JsonIgnore
    @JsonProperty("query_stats_single_to_raw")
    @Managed
    public long getquery_stats_single_to_raw()
    {
        return longStruct.get(6);
    }

    @JsonIgnore
    @JsonProperty("query_stats_single_to_single")
    @Managed
    public long getquery_stats_single_to_single()
    {
        return longStruct.get(7);
    }

    @JsonIgnore
    @JsonProperty("query_stats_single_to_singlenonull")
    @Managed
    public long getquery_stats_single_to_singlenonull()
    {
        return longStruct.get(8);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_single_to")
    @Managed
    public long getquery_stats_total_single_to()
    {
        return longStruct.get(9);
    }

    @JsonIgnore
    @JsonProperty("query_stats_singlenonull_to_raw")
    @Managed
    public long getquery_stats_singlenonull_to_raw()
    {
        return longStruct.get(10);
    }

    @JsonIgnore
    @JsonProperty("query_stats_singlenonull_to_single")
    @Managed
    public long getquery_stats_singlenonull_to_single()
    {
        return longStruct.get(11);
    }

    @JsonIgnore
    @JsonProperty("query_stats_singlenonull_to_singlenonull")
    @Managed
    public long getquery_stats_singlenonull_to_singlenonull()
    {
        return longStruct.get(12);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_singlenonull_to")
    @Managed
    public long getquery_stats_total_singlenonull_to()
    {
        return longStruct.get(13);
    }

    @JsonIgnore
    @JsonProperty("query_stats_allnull_to_raw")
    @Managed
    public long getquery_stats_allnull_to_raw()
    {
        return longStruct.get(14);
    }

    @JsonIgnore
    @JsonProperty("query_stats_allnull_to_single")
    @Managed
    public long getquery_stats_allnull_to_single()
    {
        return longStruct.get(15);
    }

    @JsonIgnore
    @JsonProperty("query_stats_allnull_to_allnull")
    @Managed
    public long getquery_stats_allnull_to_allnull()
    {
        return longStruct.get(16);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_allnull_to")
    @Managed
    public long getquery_stats_total_allnull_to()
    {
        return longStruct.get(17);
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
        longStruct.put(8, 0);
        longStruct.put(9, 0);
        longStruct.put(10, 0);
        longStruct.put(11, 0);
        longStruct.put(12, 0);
        longStruct.put(13, 0);
        longStruct.put(14, 0);
        longStruct.put(15, 0);
        longStruct.put(16, 0);
        longStruct.put(17, 0);
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
        return 18;
    }

    private native ByteBuffer initNative(long limit);
}
