
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
public final class VaradaStatsMatchtime
        extends VaradaStatsBase
{
    /* This class file is auto-generated from matchtime xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsMatchtime(@JsonProperty("match_time_total") long match_time_total, @JsonProperty("match_time_1_open") long match_time_1_open, @JsonProperty("match_time_2_match") long match_time_2_match, @JsonProperty("match_time_2_1_agg") long match_time_2_1_agg, @JsonProperty("match_time_2_2_multi") long match_time_2_2_multi, @JsonProperty("match_time_3_close") long match_time_3_close)
    {
        this();
        longStruct.put(0, match_time_total);
        longStruct.put(1, match_time_1_open);
        longStruct.put(2, match_time_2_match);
        longStruct.put(3, match_time_2_1_agg);
        longStruct.put(4, match_time_2_2_multi);
        longStruct.put(5, match_time_3_close);
    }

    public VaradaStatsMatchtime()
    {
        super("matchtime", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("match_time_total")
    @Managed
    public long getmatch_time_total()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("match_time_1_open")
    @Managed
    public long getmatch_time_1_open()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("match_time_2_match")
    @Managed
    public long getmatch_time_2_match()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("match_time_2_1_agg")
    @Managed
    public long getmatch_time_2_1_agg()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("match_time_2_2_multi")
    @Managed
    public long getmatch_time_2_2_multi()
    {
        return longStruct.get(4);
    }

    @JsonIgnore
    @JsonProperty("match_time_3_close")
    @Managed
    public long getmatch_time_3_close()
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
