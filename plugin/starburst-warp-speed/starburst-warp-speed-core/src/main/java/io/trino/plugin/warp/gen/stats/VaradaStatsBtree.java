
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
public final class VaradaStatsBtree
        extends VaradaStatsBase
{
    /* This class file is auto-generated from btree xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsBtree(@JsonProperty("query_stats_total_skip_chunks") long query_stats_total_skip_chunks, @JsonProperty("query_stats_total_chunks") long query_stats_total_chunks, @JsonProperty("query_time_read_root") long query_time_read_root, @JsonProperty("query_time_match_buckets") long query_time_match_buckets, @JsonProperty("query_time_match_records") long query_time_match_records)
    {
        this();
        longStruct.put(0, query_stats_total_skip_chunks);
        longStruct.put(1, query_stats_total_chunks);
        longStruct.put(2, query_time_read_root);
        longStruct.put(3, query_time_match_buckets);
        longStruct.put(4, query_time_match_records);
    }

    public VaradaStatsBtree()
    {
        super("btree", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_skip_chunks")
    @Managed
    public long getquery_stats_total_skip_chunks()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_chunks")
    @Managed
    public long getquery_stats_total_chunks()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("query_time_read_root")
    @Managed
    public long getquery_time_read_root()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("query_time_match_buckets")
    @Managed
    public long getquery_time_match_buckets()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("query_time_match_records")
    @Managed
    public long getquery_time_match_records()
    {
        return longStruct.get(4);
    }

    @Managed
    public double getavg_query_time_read_root()
    {
        if (longStruct.get(5) == 0) {
            return 0;
        }
        return ((double) longStruct.get(6)) / ((double) longStruct.get(5));
    }

    @Managed
    public double getavg_query_time_match_buckets()
    {
        if (longStruct.get(7) == 0) {
            return 0;
        }
        return ((double) longStruct.get(8)) / ((double) longStruct.get(7));
    }

    @Managed
    public double getavg_query_time_match_records()
    {
        if (longStruct.get(9) == 0) {
            return 0;
        }
        return ((double) longStruct.get(10)) / ((double) longStruct.get(9));
    }

    @Override
    public void reset()
    {
        longStruct.put(0, 0);
        longStruct.put(1, 0);
        longStruct.put(2, 0);
        longStruct.put(3, 0);
        longStruct.put(4, 0);
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
        return 5;
    }

    private native ByteBuffer initNative(long limit);
}
