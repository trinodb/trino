
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
public final class VaradaStatsBloom
        extends VaradaStatsBase
{
    /* This class file is auto-generated from bloom xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsBloom(@JsonProperty("warm_stats_total_compressed_pages") long warm_stats_total_compressed_pages, @JsonProperty("warm_stats_total_uncompressed_pages") long warm_stats_total_uncompressed_pages, @JsonProperty("warm_stats_chunk_pages_histogram_1") long warm_stats_chunk_pages_histogram_1, @JsonProperty("warm_stats_chunk_pages_histogram_2") long warm_stats_chunk_pages_histogram_2, @JsonProperty("warm_stats_chunk_pages_histogram_3") long warm_stats_chunk_pages_histogram_3, @JsonProperty("warm_stats_chunk_pages_histogram_4") long warm_stats_chunk_pages_histogram_4, @JsonProperty("warm_stats_chunk_pages_histogram_5") long warm_stats_chunk_pages_histogram_5, @JsonProperty("warm_stats_chunk_pages_histogram_6") long warm_stats_chunk_pages_histogram_6, @JsonProperty("warm_stats_chunk_pages_histogram_7") long warm_stats_chunk_pages_histogram_7, @JsonProperty("warm_stats_chunk_pages_histogram_8") long warm_stats_chunk_pages_histogram_8, @JsonProperty("warm_stats_chunk_pages_histogram_9plus") long warm_stats_chunk_pages_histogram_9plus)
    {
        this();
        longStruct.put(0, warm_stats_total_compressed_pages);
        longStruct.put(1, warm_stats_total_uncompressed_pages);
        longStruct.put(2, warm_stats_chunk_pages_histogram_1);
        longStruct.put(3, warm_stats_chunk_pages_histogram_2);
        longStruct.put(4, warm_stats_chunk_pages_histogram_3);
        longStruct.put(5, warm_stats_chunk_pages_histogram_4);
        longStruct.put(6, warm_stats_chunk_pages_histogram_5);
        longStruct.put(7, warm_stats_chunk_pages_histogram_6);
        longStruct.put(8, warm_stats_chunk_pages_histogram_7);
        longStruct.put(9, warm_stats_chunk_pages_histogram_8);
        longStruct.put(10, warm_stats_chunk_pages_histogram_9plus);
    }

    public VaradaStatsBloom()
    {
        super("bloom", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("warm_stats_total_compressed_pages")
    @Managed
    public long getwarm_stats_total_compressed_pages()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_total_uncompressed_pages")
    @Managed
    public long getwarm_stats_total_uncompressed_pages()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_1")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_1()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_2")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_2()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_3")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_3()
    {
        return longStruct.get(4);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_4")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_4()
    {
        return longStruct.get(5);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_5")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_5()
    {
        return longStruct.get(6);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_6")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_6()
    {
        return longStruct.get(7);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_7")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_7()
    {
        return longStruct.get(8);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_8")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_8()
    {
        return longStruct.get(9);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_chunk_pages_histogram_9plus")
    @Managed
    public long getwarm_stats_chunk_pages_histogram_9plus()
    {
        return longStruct.get(10);
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
        return 11;
    }

    private native ByteBuffer initNative(long limit);
}
