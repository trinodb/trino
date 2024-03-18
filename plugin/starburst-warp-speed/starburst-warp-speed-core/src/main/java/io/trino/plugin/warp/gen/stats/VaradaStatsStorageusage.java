
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
public final class VaradaStatsStorageusage
        extends VaradaStatsBase
{
    /* This class file is auto-generated from storageusage xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsStorageusage(@JsonProperty("page_histogram_chunk") long page_histogram_chunk, @JsonProperty("page_histogram_extrecs") long page_histogram_extrecs, @JsonProperty("page_histogram_datamd") long page_histogram_datamd, @JsonProperty("page_histogram_chunkrepo") long page_histogram_chunkrepo, @JsonProperty("page_histogram_btree") long page_histogram_btree, @JsonProperty("page_histogram_bucket") long page_histogram_bucket, @JsonProperty("page_histogram_nulls") long page_histogram_nulls, @JsonProperty("page_histogram_lucene") long page_histogram_lucene, @JsonProperty("page_histogram_bloom") long page_histogram_bloom, @JsonProperty("page_histogram_rowgrp") long page_histogram_rowgrp, @JsonProperty("page_histogram_total") long page_histogram_total)
    {
        this();
        longStruct.put(0, page_histogram_chunk);
        longStruct.put(1, page_histogram_extrecs);
        longStruct.put(2, page_histogram_datamd);
        longStruct.put(3, page_histogram_chunkrepo);
        longStruct.put(4, page_histogram_btree);
        longStruct.put(5, page_histogram_bucket);
        longStruct.put(6, page_histogram_nulls);
        longStruct.put(7, page_histogram_lucene);
        longStruct.put(8, page_histogram_bloom);
        longStruct.put(9, page_histogram_rowgrp);
        longStruct.put(10, page_histogram_total);
    }

    public VaradaStatsStorageusage()
    {
        super("storageusage", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("page_histogram_chunk")
    @Managed
    public long getpage_histogram_chunk()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_extrecs")
    @Managed
    public long getpage_histogram_extrecs()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_datamd")
    @Managed
    public long getpage_histogram_datamd()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_chunkrepo")
    @Managed
    public long getpage_histogram_chunkrepo()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_btree")
    @Managed
    public long getpage_histogram_btree()
    {
        return longStruct.get(4);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_bucket")
    @Managed
    public long getpage_histogram_bucket()
    {
        return longStruct.get(5);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_nulls")
    @Managed
    public long getpage_histogram_nulls()
    {
        return longStruct.get(6);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_lucene")
    @Managed
    public long getpage_histogram_lucene()
    {
        return longStruct.get(7);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_bloom")
    @Managed
    public long getpage_histogram_bloom()
    {
        return longStruct.get(8);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_rowgrp")
    @Managed
    public long getpage_histogram_rowgrp()
    {
        return longStruct.get(9);
    }

    @JsonIgnore
    @JsonProperty("page_histogram_total")
    @Managed
    public long getpage_histogram_total()
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
