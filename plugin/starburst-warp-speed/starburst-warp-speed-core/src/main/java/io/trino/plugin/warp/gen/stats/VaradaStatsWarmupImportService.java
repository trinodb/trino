
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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.metrics.VaradaStatType;
import io.trino.plugin.varada.metrics.VaradaStatsBase;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.LongAdder;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.ANY)
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName"})
public final class VaradaStatsWarmupImportService
        extends VaradaStatsBase
{
    /* This class file is auto-generated from warmupImportService xml file for statistics and counters */
    private final String group;

    private final LongAdder import_row_group_count_started = new LongAdder();
    private final LongAdder import_row_group_count_accomplished = new LongAdder();
    private final LongAdder import_row_group_count_failed = new LongAdder();
    private final LongAdder hiveWarmTime = new LongAdder();
    private final LongAdder import_we_group_download_started = new LongAdder();
    private final LongAdder import_we_group_download_failed = new LongAdder();
    private final LongAdder import_we_group_download_accomplished = new LongAdder();
    private final LongAdder import_we_group_footer_validation = new LongAdder();
    private final LongAdder import_elements_started = new LongAdder();
    private final LongAdder import_elements_failed = new LongAdder();
    private final LongAdder import_elements_accomplished = new LongAdder();
    private final LongAdder import_element_1st_footer_validation = new LongAdder();
    private final LongAdder import_element_2nd_footer_validation = new LongAdder();
    private final LongAdder import_row_group_total_time_Count = new LongAdder();
    private final LongAdder import_row_group_total_time = new LongAdder();

    @JsonCreator
    public VaradaStatsWarmupImportService(@JsonProperty("group") String group)
    {
        super(createKey(group), VaradaStatType.Worker);

        this.group = group;
    }

    @JsonProperty
    @Managed
    public String getGroup()
    {
        return group;
    }

    @JsonIgnore
    @Managed
    public long getimport_row_group_count_started()
    {
        return import_row_group_count_started.longValue();
    }

    public void incimport_row_group_count_started()
    {
        import_row_group_count_started.increment();
    }

    public void addimport_row_group_count_started(long val)
    {
        import_row_group_count_started.add(val);
    }

    public void setimport_row_group_count_started(long val)
    {
        import_row_group_count_started.reset();
        addimport_row_group_count_started(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_row_group_count_accomplished()
    {
        return import_row_group_count_accomplished.longValue();
    }

    public void incimport_row_group_count_accomplished()
    {
        import_row_group_count_accomplished.increment();
    }

    public void addimport_row_group_count_accomplished(long val)
    {
        import_row_group_count_accomplished.add(val);
    }

    public void setimport_row_group_count_accomplished(long val)
    {
        import_row_group_count_accomplished.reset();
        addimport_row_group_count_accomplished(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_row_group_count_failed()
    {
        return import_row_group_count_failed.longValue();
    }

    public void incimport_row_group_count_failed()
    {
        import_row_group_count_failed.increment();
    }

    public void addimport_row_group_count_failed(long val)
    {
        import_row_group_count_failed.add(val);
    }

    public void setimport_row_group_count_failed(long val)
    {
        import_row_group_count_failed.reset();
        addimport_row_group_count_failed(val);
    }

    @JsonIgnore
    @Managed
    public long gethiveWarmTime()
    {
        return hiveWarmTime.longValue();
    }

    public void inchiveWarmTime()
    {
        hiveWarmTime.increment();
    }

    public void addhiveWarmTime(long val)
    {
        hiveWarmTime.add(val);
    }

    public void sethiveWarmTime(long val)
    {
        hiveWarmTime.reset();
        addhiveWarmTime(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_we_group_download_started()
    {
        return import_we_group_download_started.longValue();
    }

    public void incimport_we_group_download_started()
    {
        import_we_group_download_started.increment();
    }

    public void addimport_we_group_download_started(long val)
    {
        import_we_group_download_started.add(val);
    }

    public void setimport_we_group_download_started(long val)
    {
        import_we_group_download_started.reset();
        addimport_we_group_download_started(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_we_group_download_failed()
    {
        return import_we_group_download_failed.longValue();
    }

    public void incimport_we_group_download_failed()
    {
        import_we_group_download_failed.increment();
    }

    public void addimport_we_group_download_failed(long val)
    {
        import_we_group_download_failed.add(val);
    }

    public void setimport_we_group_download_failed(long val)
    {
        import_we_group_download_failed.reset();
        addimport_we_group_download_failed(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_we_group_download_accomplished()
    {
        return import_we_group_download_accomplished.longValue();
    }

    public void incimport_we_group_download_accomplished()
    {
        import_we_group_download_accomplished.increment();
    }

    public void addimport_we_group_download_accomplished(long val)
    {
        import_we_group_download_accomplished.add(val);
    }

    public void setimport_we_group_download_accomplished(long val)
    {
        import_we_group_download_accomplished.reset();
        addimport_we_group_download_accomplished(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_we_group_footer_validation()
    {
        return import_we_group_footer_validation.longValue();
    }

    public void incimport_we_group_footer_validation()
    {
        import_we_group_footer_validation.increment();
    }

    public void addimport_we_group_footer_validation(long val)
    {
        import_we_group_footer_validation.add(val);
    }

    public void setimport_we_group_footer_validation(long val)
    {
        import_we_group_footer_validation.reset();
        addimport_we_group_footer_validation(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_elements_started()
    {
        return import_elements_started.longValue();
    }

    public void incimport_elements_started()
    {
        import_elements_started.increment();
    }

    public void addimport_elements_started(long val)
    {
        import_elements_started.add(val);
    }

    public void setimport_elements_started(long val)
    {
        import_elements_started.reset();
        addimport_elements_started(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_elements_failed()
    {
        return import_elements_failed.longValue();
    }

    public void incimport_elements_failed()
    {
        import_elements_failed.increment();
    }

    public void addimport_elements_failed(long val)
    {
        import_elements_failed.add(val);
    }

    public void setimport_elements_failed(long val)
    {
        import_elements_failed.reset();
        addimport_elements_failed(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_elements_accomplished()
    {
        return import_elements_accomplished.longValue();
    }

    public void incimport_elements_accomplished()
    {
        import_elements_accomplished.increment();
    }

    public void addimport_elements_accomplished(long val)
    {
        import_elements_accomplished.add(val);
    }

    public void setimport_elements_accomplished(long val)
    {
        import_elements_accomplished.reset();
        addimport_elements_accomplished(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_element_1st_footer_validation()
    {
        return import_element_1st_footer_validation.longValue();
    }

    public void incimport_element_1st_footer_validation()
    {
        import_element_1st_footer_validation.increment();
    }

    public void addimport_element_1st_footer_validation(long val)
    {
        import_element_1st_footer_validation.add(val);
    }

    public void setimport_element_1st_footer_validation(long val)
    {
        import_element_1st_footer_validation.reset();
        addimport_element_1st_footer_validation(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_element_2nd_footer_validation()
    {
        return import_element_2nd_footer_validation.longValue();
    }

    public void incimport_element_2nd_footer_validation()
    {
        import_element_2nd_footer_validation.increment();
    }

    public void addimport_element_2nd_footer_validation(long val)
    {
        import_element_2nd_footer_validation.add(val);
    }

    public void setimport_element_2nd_footer_validation(long val)
    {
        import_element_2nd_footer_validation.reset();
        addimport_element_2nd_footer_validation(val);
    }

    @JsonIgnore
    @Managed
    public long getimport_row_group_total_time_Count()
    {
        return import_row_group_total_time_Count.longValue();
    }

    @Managed
    public long getimport_row_group_total_time_Average()
    {
        if (import_row_group_total_time_Count.longValue() == 0) {
            return 0;
        }
        return import_row_group_total_time.longValue() / import_row_group_total_time_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getimport_row_group_total_time()
    {
        return import_row_group_total_time.longValue();
    }

    public void addimport_row_group_total_time(long val)
    {
        import_row_group_total_time.add(val);
        import_row_group_total_time_Count.add(1);
    }

    public static VaradaStatsWarmupImportService create(String group)
    {
        return new VaradaStatsWarmupImportService(group);
    }

    public static String createKey(String group)
    {
        return new StringJoiner(".").add(group).toString();
    }

    @Override
    public void merge(VaradaStatsBase varadaStatsBase)
    {
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("import_row_group_count_started", import_row_group_count_started);
        ret.put("import_row_group_count_accomplished", import_row_group_count_accomplished);
        ret.put("import_row_group_count_failed", import_row_group_count_failed);
        ret.put("hiveWarmTime", hiveWarmTime);
        ret.put("import_we_group_download_started", import_we_group_download_started);
        ret.put("import_we_group_download_failed", import_we_group_download_failed);
        ret.put("import_we_group_download_accomplished", import_we_group_download_accomplished);
        ret.put("import_we_group_footer_validation", import_we_group_footer_validation);
        ret.put("import_elements_started", import_elements_started);
        ret.put("import_elements_failed", import_elements_failed);
        ret.put("import_elements_accomplished", import_elements_accomplished);
        ret.put("import_element_1st_footer_validation", import_element_1st_footer_validation);
        ret.put("import_element_2nd_footer_validation", import_element_2nd_footer_validation);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsWarmupImportService other = (VaradaStatsWarmupImportService) varadaStatsBase;
        this.import_row_group_count_started.add(other.import_row_group_count_started.longValue());
        this.import_row_group_count_accomplished.add(other.import_row_group_count_accomplished.longValue());
        this.import_row_group_count_failed.add(other.import_row_group_count_failed.longValue());
        this.hiveWarmTime.add(other.hiveWarmTime.longValue());
        this.import_we_group_download_started.add(other.import_we_group_download_started.longValue());
        this.import_we_group_download_failed.add(other.import_we_group_download_failed.longValue());
        this.import_we_group_download_accomplished.add(other.import_we_group_download_accomplished.longValue());
        this.import_we_group_footer_validation.add(other.import_we_group_footer_validation.longValue());
        this.import_elements_started.add(other.import_elements_started.longValue());
        this.import_elements_failed.add(other.import_elements_failed.longValue());
        this.import_elements_accomplished.add(other.import_elements_accomplished.longValue());
        this.import_element_1st_footer_validation.add(other.import_element_1st_footer_validation.longValue());
        this.import_element_2nd_footer_validation.add(other.import_element_2nd_footer_validation.longValue());
        this.import_row_group_total_time.add(other.import_row_group_total_time.longValue());
        this.import_row_group_total_time_Count.add(other.import_row_group_total_time_Count.longValue());
    }

    @Override
    public void reset()
    {
        import_row_group_count_started.reset();
        import_row_group_count_accomplished.reset();
        import_row_group_count_failed.reset();
        hiveWarmTime.reset();
        import_we_group_download_started.reset();
        import_we_group_download_failed.reset();
        import_we_group_download_accomplished.reset();
        import_we_group_footer_validation.reset();
        import_elements_started.reset();
        import_elements_failed.reset();
        import_elements_accomplished.reset();
        import_element_1st_footer_validation.reset();
        import_element_2nd_footer_validation.reset();
        import_row_group_total_time.reset();
        import_row_group_total_time_Count.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":import_row_group_count_started", import_row_group_count_started.longValue());
        res.put(getJmxKey() + ":import_row_group_count_accomplished", import_row_group_count_accomplished.longValue());
        res.put(getJmxKey() + ":import_row_group_count_failed", import_row_group_count_failed.longValue());
        res.put(getJmxKey() + ":hiveWarmTime", hiveWarmTime.longValue());
        res.put(getJmxKey() + ":import_we_group_download_started", import_we_group_download_started.longValue());
        res.put(getJmxKey() + ":import_we_group_download_failed", import_we_group_download_failed.longValue());
        res.put(getJmxKey() + ":import_we_group_download_accomplished", import_we_group_download_accomplished.longValue());
        res.put(getJmxKey() + ":import_we_group_footer_validation", import_we_group_footer_validation.longValue());
        res.put(getJmxKey() + ":import_elements_started", import_elements_started.longValue());
        res.put(getJmxKey() + ":import_elements_failed", import_elements_failed.longValue());
        res.put(getJmxKey() + ":import_elements_accomplished", import_elements_accomplished.longValue());
        res.put(getJmxKey() + ":import_element_1st_footer_validation", import_element_1st_footer_validation.longValue());
        res.put(getJmxKey() + ":import_element_2nd_footer_validation", import_element_2nd_footer_validation.longValue());
        res.put(getJmxKey() + ":import_row_group_total_time", import_row_group_total_time.longValue());
        res.put(getJmxKey() + ":import_row_group_total_time_Count", import_row_group_total_time_Count.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("import_row_group_count_started", getimport_row_group_count_started());
        res.put("import_row_group_count_accomplished", getimport_row_group_count_accomplished());
        res.put("import_row_group_count_failed", getimport_row_group_count_failed());
        res.put("import_we_group_download_started", getimport_we_group_download_started());
        res.put("import_we_group_download_failed", getimport_we_group_download_failed());
        res.put("import_we_group_download_accomplished", getimport_we_group_download_accomplished());
        res.put("import_we_group_footer_validation", getimport_we_group_footer_validation());
        res.put("import_elements_started", getimport_elements_started());
        res.put("import_elements_failed", getimport_elements_failed());
        res.put("import_elements_accomplished", getimport_elements_accomplished());
        res.put("import_element_1st_footer_validation", getimport_element_1st_footer_validation());
        res.put("import_element_2nd_footer_validation", getimport_element_2nd_footer_validation());
        return res;
    }

    @Override
    protected Map<String, Object> statePrintFields()
    {
        return new HashMap<>();
    }

    public int getNumberOfMetrics()
    {
        return 13;
    }
}
