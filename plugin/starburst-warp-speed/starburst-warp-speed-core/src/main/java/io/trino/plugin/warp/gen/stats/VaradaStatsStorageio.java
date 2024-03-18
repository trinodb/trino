
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
public final class VaradaStatsStorageio
        extends VaradaStatsBase
{
    /* This class file is auto-generated from storageio xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsStorageio(@JsonProperty("io_stats_error") long io_stats_error, @JsonProperty("io_stats_timeout") long io_stats_timeout)
    {
        this();
        longStruct.put(0, io_stats_error);
        longStruct.put(1, io_stats_timeout);
    }

    public VaradaStatsStorageio()
    {
        super("storageio", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("io_stats_error")
    @Managed
    public long getio_stats_error()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("io_stats_timeout")
    @Managed
    public long getio_stats_timeout()
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
