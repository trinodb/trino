
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
public final class VaradaStatsFastwarming
        extends VaradaStatsBase
{
    /* This class file is auto-generated from fastwarming xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsFastwarming(@JsonProperty("import_stats_num_bufs_histogram_1") long import_stats_num_bufs_histogram_1, @JsonProperty("import_stats_num_bufs_histogram_2") long import_stats_num_bufs_histogram_2, @JsonProperty("import_stats_num_bufs_histogram_3") long import_stats_num_bufs_histogram_3, @JsonProperty("import_stats_num_bufs_histogram_4plus") long import_stats_num_bufs_histogram_4plus, @JsonProperty("import_stats_no_space_chunk_repo") long import_stats_no_space_chunk_repo, @JsonProperty("import_stats_no_space_chunk_header") long import_stats_no_space_chunk_header, @JsonProperty("import_stats_no_space_chunk") long import_stats_no_space_chunk, @JsonProperty("export_stats_num_bufs_histogram_1") long export_stats_num_bufs_histogram_1, @JsonProperty("export_stats_num_bufs_histogram_2") long export_stats_num_bufs_histogram_2, @JsonProperty("export_stats_num_bufs_histogram_3") long export_stats_num_bufs_histogram_3, @JsonProperty("export_stats_num_bufs_histogram_4plus") long export_stats_num_bufs_histogram_4plus, @JsonProperty("export_error_no_space_chunk_repo") long export_error_no_space_chunk_repo, @JsonProperty("export_error_no_space_chunk_header") long export_error_no_space_chunk_header, @JsonProperty("export_error_no_space_chunk") long export_error_no_space_chunk)
    {
        this();
        longStruct.put(0, import_stats_num_bufs_histogram_1);
        longStruct.put(1, import_stats_num_bufs_histogram_2);
        longStruct.put(2, import_stats_num_bufs_histogram_3);
        longStruct.put(3, import_stats_num_bufs_histogram_4plus);
        longStruct.put(4, import_stats_no_space_chunk_repo);
        longStruct.put(5, import_stats_no_space_chunk_header);
        longStruct.put(6, import_stats_no_space_chunk);
        longStruct.put(7, export_stats_num_bufs_histogram_1);
        longStruct.put(8, export_stats_num_bufs_histogram_2);
        longStruct.put(9, export_stats_num_bufs_histogram_3);
        longStruct.put(10, export_stats_num_bufs_histogram_4plus);
        longStruct.put(11, export_error_no_space_chunk_repo);
        longStruct.put(12, export_error_no_space_chunk_header);
        longStruct.put(13, export_error_no_space_chunk);
    }

    public VaradaStatsFastwarming()
    {
        super("fastwarming", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("import_stats_num_bufs_histogram_1")
    @Managed
    public long getimport_stats_num_bufs_histogram_1()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("import_stats_num_bufs_histogram_2")
    @Managed
    public long getimport_stats_num_bufs_histogram_2()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("import_stats_num_bufs_histogram_3")
    @Managed
    public long getimport_stats_num_bufs_histogram_3()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("import_stats_num_bufs_histogram_4plus")
    @Managed
    public long getimport_stats_num_bufs_histogram_4plus()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("import_stats_no_space_chunk_repo")
    @Managed
    public long getimport_stats_no_space_chunk_repo()
    {
        return longStruct.get(4);
    }

    @JsonIgnore
    @JsonProperty("import_stats_no_space_chunk_header")
    @Managed
    public long getimport_stats_no_space_chunk_header()
    {
        return longStruct.get(5);
    }

    @JsonIgnore
    @JsonProperty("import_stats_no_space_chunk")
    @Managed
    public long getimport_stats_no_space_chunk()
    {
        return longStruct.get(6);
    }

    @JsonIgnore
    @JsonProperty("export_stats_num_bufs_histogram_1")
    @Managed
    public long getexport_stats_num_bufs_histogram_1()
    {
        return longStruct.get(7);
    }

    @JsonIgnore
    @JsonProperty("export_stats_num_bufs_histogram_2")
    @Managed
    public long getexport_stats_num_bufs_histogram_2()
    {
        return longStruct.get(8);
    }

    @JsonIgnore
    @JsonProperty("export_stats_num_bufs_histogram_3")
    @Managed
    public long getexport_stats_num_bufs_histogram_3()
    {
        return longStruct.get(9);
    }

    @JsonIgnore
    @JsonProperty("export_stats_num_bufs_histogram_4plus")
    @Managed
    public long getexport_stats_num_bufs_histogram_4plus()
    {
        return longStruct.get(10);
    }

    @JsonIgnore
    @JsonProperty("export_error_no_space_chunk_repo")
    @Managed
    public long getexport_error_no_space_chunk_repo()
    {
        return longStruct.get(11);
    }

    @JsonIgnore
    @JsonProperty("export_error_no_space_chunk_header")
    @Managed
    public long getexport_error_no_space_chunk_header()
    {
        return longStruct.get(12);
    }

    @JsonIgnore
    @JsonProperty("export_error_no_space_chunk")
    @Managed
    public long getexport_error_no_space_chunk()
    {
        return longStruct.get(13);
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
        return 14;
    }

    private native ByteBuffer initNative(long limit);
}
