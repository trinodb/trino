
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
public final class VaradaStatsDatacompression
        extends VaradaStatsBase
{
    /* This class file is auto-generated from datacompression xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsDatacompression(@JsonProperty("warm_data_stats_compressed_pages") long warm_data_stats_compressed_pages, @JsonProperty("warm_data_stats_raw_pages") long warm_data_stats_raw_pages)
    {
        this();
        longStruct.put(0, warm_data_stats_compressed_pages);
        longStruct.put(1, warm_data_stats_raw_pages);
    }

    public VaradaStatsDatacompression()
    {
        super("datacompression", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("warm_data_stats_compressed_pages")
    @Managed
    public long getwarm_data_stats_compressed_pages()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("warm_data_stats_raw_pages")
    @Managed
    public long getwarm_data_stats_raw_pages()
    {
        return longStruct.get(1);
    }

    @Override
    public void reset()
    {
        longStruct.put(0, 0);
        longStruct.put(1, 0);
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
        return 2;
    }

    private native ByteBuffer initNative(long limit);
}
