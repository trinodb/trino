
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
public final class VaradaStatsLucene
        extends VaradaStatsBase
{
    /* This class file is auto-generated from lucene xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public VaradaStatsLucene(@JsonProperty("warm_stats_total_write_failures") long warm_stats_total_write_failures, @JsonProperty("warm_stats_write_failures_histogram_sfile_too_big") long warm_stats_write_failures_histogram_sfile_too_big, @JsonProperty("warm_stats_write_failures_histogram_bfile_too_big") long warm_stats_write_failures_histogram_bfile_too_big, @JsonProperty("warm_stats_write_failures_histogram_zero_file_length") long warm_stats_write_failures_histogram_zero_file_length, @JsonProperty("query_stats_total_read_failures") long query_stats_total_read_failures, @JsonProperty("query_stats_read_failures_histogram_sfile_too_big") long query_stats_read_failures_histogram_sfile_too_big, @JsonProperty("query_stats_read_failures_histogram_bfile_too_big") long query_stats_read_failures_histogram_bfile_too_big, @JsonProperty("query_stats_read_failures_histogram_bfile_does_not_exist") long query_stats_read_failures_histogram_bfile_does_not_exist, @JsonProperty("query_stats_read_failures_histogram_sfile_memory") long query_stats_read_failures_histogram_sfile_memory, @JsonProperty("query_stats_read_failures_histogram_bfile_memory") long query_stats_read_failures_histogram_bfile_memory, @JsonProperty("warm_stats_total_bfiles") long warm_stats_total_bfiles, @JsonProperty("warm_stats_bfile_size_histogram_0m_10m") long warm_stats_bfile_size_histogram_0m_10m, @JsonProperty("warm_stats_bfile_size_histogram_10m_20m") long warm_stats_bfile_size_histogram_10m_20m, @JsonProperty("warm_stats_bfile_size_histogram_20m_30m") long warm_stats_bfile_size_histogram_20m_30m, @JsonProperty("warm_stats_bfile_size_histogram_30m_40m") long warm_stats_bfile_size_histogram_30m_40m, @JsonProperty("warm_stats_bfile_size_histogram_40m_50m") long warm_stats_bfile_size_histogram_40m_50m, @JsonProperty("warm_stats_bfile_size_histogram_50mplus") long warm_stats_bfile_size_histogram_50mplus, @JsonProperty("query_stats_total_reads_from_file") long query_stats_total_reads_from_file, @JsonProperty("query_stats_read_from_file_size_histogram_0k_1k") long query_stats_read_from_file_size_histogram_0k_1k, @JsonProperty("query_stats_read_from_file_size_histogram_1k_2k") long query_stats_read_from_file_size_histogram_1k_2k, @JsonProperty("query_stats_read_from_file_size_histogram_2kplus") long query_stats_read_from_file_size_histogram_2kplus, @JsonProperty("query_stats_total_page_reads_from_bfile") long query_stats_total_page_reads_from_bfile, @JsonProperty("query_stats_total_match_failures") long query_stats_total_match_failures, @JsonProperty("query_stats_match_failures_histogram_error") long query_stats_match_failures_histogram_error, @JsonProperty("query_stats_match_failures_histogram_no_index") long query_stats_match_failures_histogram_no_index)
    {
        this();
        longStruct.put(0, warm_stats_total_write_failures);
        longStruct.put(1, warm_stats_write_failures_histogram_sfile_too_big);
        longStruct.put(2, warm_stats_write_failures_histogram_bfile_too_big);
        longStruct.put(3, warm_stats_write_failures_histogram_zero_file_length);
        longStruct.put(4, query_stats_total_read_failures);
        longStruct.put(5, query_stats_read_failures_histogram_sfile_too_big);
        longStruct.put(6, query_stats_read_failures_histogram_bfile_too_big);
        longStruct.put(7, query_stats_read_failures_histogram_bfile_does_not_exist);
        longStruct.put(8, query_stats_read_failures_histogram_sfile_memory);
        longStruct.put(9, query_stats_read_failures_histogram_bfile_memory);
        longStruct.put(10, warm_stats_total_bfiles);
        longStruct.put(11, warm_stats_bfile_size_histogram_0m_10m);
        longStruct.put(12, warm_stats_bfile_size_histogram_10m_20m);
        longStruct.put(13, warm_stats_bfile_size_histogram_20m_30m);
        longStruct.put(14, warm_stats_bfile_size_histogram_30m_40m);
        longStruct.put(15, warm_stats_bfile_size_histogram_40m_50m);
        longStruct.put(16, warm_stats_bfile_size_histogram_50mplus);
        longStruct.put(17, query_stats_total_reads_from_file);
        longStruct.put(18, query_stats_read_from_file_size_histogram_0k_1k);
        longStruct.put(19, query_stats_read_from_file_size_histogram_1k_2k);
        longStruct.put(20, query_stats_read_from_file_size_histogram_2kplus);
        longStruct.put(21, query_stats_total_page_reads_from_bfile);
        longStruct.put(22, query_stats_total_match_failures);
        longStruct.put(23, query_stats_match_failures_histogram_error);
        longStruct.put(24, query_stats_match_failures_histogram_no_index);
    }

    public VaradaStatsLucene()
    {
        super("lucene", VaradaStatType.Worker);

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("warm_stats_total_write_failures")
    @Managed
    public long getwarm_stats_total_write_failures()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_write_failures_histogram_sfile_too_big")
    @Managed
    public long getwarm_stats_write_failures_histogram_sfile_too_big()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_write_failures_histogram_bfile_too_big")
    @Managed
    public long getwarm_stats_write_failures_histogram_bfile_too_big()
    {
        return longStruct.get(2);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_write_failures_histogram_zero_file_length")
    @Managed
    public long getwarm_stats_write_failures_histogram_zero_file_length()
    {
        return longStruct.get(3);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_read_failures")
    @Managed
    public long getquery_stats_total_read_failures()
    {
        return longStruct.get(4);
    }

    @JsonIgnore
    @JsonProperty("query_stats_read_failures_histogram_sfile_too_big")
    @Managed
    public long getquery_stats_read_failures_histogram_sfile_too_big()
    {
        return longStruct.get(5);
    }

    @JsonIgnore
    @JsonProperty("query_stats_read_failures_histogram_bfile_too_big")
    @Managed
    public long getquery_stats_read_failures_histogram_bfile_too_big()
    {
        return longStruct.get(6);
    }

    @JsonIgnore
    @JsonProperty("query_stats_read_failures_histogram_bfile_does_not_exist")
    @Managed
    public long getquery_stats_read_failures_histogram_bfile_does_not_exist()
    {
        return longStruct.get(7);
    }

    @JsonIgnore
    @JsonProperty("query_stats_read_failures_histogram_sfile_memory")
    @Managed
    public long getquery_stats_read_failures_histogram_sfile_memory()
    {
        return longStruct.get(8);
    }

    @JsonIgnore
    @JsonProperty("query_stats_read_failures_histogram_bfile_memory")
    @Managed
    public long getquery_stats_read_failures_histogram_bfile_memory()
    {
        return longStruct.get(9);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_total_bfiles")
    @Managed
    public long getwarm_stats_total_bfiles()
    {
        return longStruct.get(10);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_bfile_size_histogram_0m_10m")
    @Managed
    public long getwarm_stats_bfile_size_histogram_0m_10m()
    {
        return longStruct.get(11);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_bfile_size_histogram_10m_20m")
    @Managed
    public long getwarm_stats_bfile_size_histogram_10m_20m()
    {
        return longStruct.get(12);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_bfile_size_histogram_20m_30m")
    @Managed
    public long getwarm_stats_bfile_size_histogram_20m_30m()
    {
        return longStruct.get(13);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_bfile_size_histogram_30m_40m")
    @Managed
    public long getwarm_stats_bfile_size_histogram_30m_40m()
    {
        return longStruct.get(14);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_bfile_size_histogram_40m_50m")
    @Managed
    public long getwarm_stats_bfile_size_histogram_40m_50m()
    {
        return longStruct.get(15);
    }

    @JsonIgnore
    @JsonProperty("warm_stats_bfile_size_histogram_50mplus")
    @Managed
    public long getwarm_stats_bfile_size_histogram_50mplus()
    {
        return longStruct.get(16);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_reads_from_file")
    @Managed
    public long getquery_stats_total_reads_from_file()
    {
        return longStruct.get(17);
    }

    @JsonIgnore
    @JsonProperty("query_stats_read_from_file_size_histogram_0k_1k")
    @Managed
    public long getquery_stats_read_from_file_size_histogram_0k_1k()
    {
        return longStruct.get(18);
    }

    @JsonIgnore
    @JsonProperty("query_stats_read_from_file_size_histogram_1k_2k")
    @Managed
    public long getquery_stats_read_from_file_size_histogram_1k_2k()
    {
        return longStruct.get(19);
    }

    @JsonIgnore
    @JsonProperty("query_stats_read_from_file_size_histogram_2kplus")
    @Managed
    public long getquery_stats_read_from_file_size_histogram_2kplus()
    {
        return longStruct.get(20);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_page_reads_from_bfile")
    @Managed
    public long getquery_stats_total_page_reads_from_bfile()
    {
        return longStruct.get(21);
    }

    @JsonIgnore
    @JsonProperty("query_stats_total_match_failures")
    @Managed
    public long getquery_stats_total_match_failures()
    {
        return longStruct.get(22);
    }

    @JsonIgnore
    @JsonProperty("query_stats_match_failures_histogram_error")
    @Managed
    public long getquery_stats_match_failures_histogram_error()
    {
        return longStruct.get(23);
    }

    @JsonIgnore
    @JsonProperty("query_stats_match_failures_histogram_no_index")
    @Managed
    public long getquery_stats_match_failures_histogram_no_index()
    {
        return longStruct.get(24);
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
        longStruct.put(18, 0);
        longStruct.put(19, 0);
        longStruct.put(20, 0);
        longStruct.put(21, 0);
        longStruct.put(22, 0);
        longStruct.put(23, 0);
        longStruct.put(24, 0);
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
        return 25;
    }

    private native ByteBuffer initNative(long limit);
}
