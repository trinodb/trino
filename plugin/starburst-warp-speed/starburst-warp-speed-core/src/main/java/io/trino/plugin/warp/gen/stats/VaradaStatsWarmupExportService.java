
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
public final class VaradaStatsWarmupExportService
        extends VaradaStatsBase
{
    /* This class file is auto-generated from warmupExportService xml file for statistics and counters */
    private final String group;

    private final LongAdder empty_column_list = new LongAdder();
    private final LongAdder export_row_group_scheduled = new LongAdder();
    private final LongAdder export_row_group_started = new LongAdder();
    private final LongAdder export_row_group_finished = new LongAdder();
    private final LongAdder export_row_group_accomplished = new LongAdder();
    private final LongAdder export_row_group_failed = new LongAdder();
    private final LongAdder export_row_group_1st_footer_validation = new LongAdder();
    private final LongAdder export_row_group_2nd_footer_validation = new LongAdder();
    private final LongAdder export_row_group_failed_due_local_sparse = new LongAdder();
    private final LongAdder export_skipped_due_demoter = new LongAdder();
    private final LongAdder export_skipped_due_row_group = new LongAdder();
    private final LongAdder export_skipped_due_key_conflict = new LongAdder();
    private final LongAdder export_skipped_due_key_demoted_row_group = new LongAdder();
    private final LongAdder export_time_Count = new LongAdder();
    private final LongAdder export_time = new LongAdder();

    @JsonCreator
    public VaradaStatsWarmupExportService(@JsonProperty("group") String group)
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
    public long getempty_column_list()
    {
        return empty_column_list.longValue();
    }

    public void incempty_column_list()
    {
        empty_column_list.increment();
    }

    public void addempty_column_list(long val)
    {
        empty_column_list.add(val);
    }

    public void setempty_column_list(long val)
    {
        empty_column_list.reset();
        addempty_column_list(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_row_group_scheduled()
    {
        return export_row_group_scheduled.longValue();
    }

    public void incexport_row_group_scheduled()
    {
        export_row_group_scheduled.increment();
    }

    public void addexport_row_group_scheduled(long val)
    {
        export_row_group_scheduled.add(val);
    }

    public void setexport_row_group_scheduled(long val)
    {
        export_row_group_scheduled.reset();
        addexport_row_group_scheduled(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_row_group_started()
    {
        return export_row_group_started.longValue();
    }

    public void incexport_row_group_started()
    {
        export_row_group_started.increment();
    }

    public void addexport_row_group_started(long val)
    {
        export_row_group_started.add(val);
    }

    public void setexport_row_group_started(long val)
    {
        export_row_group_started.reset();
        addexport_row_group_started(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_row_group_finished()
    {
        return export_row_group_finished.longValue();
    }

    public void incexport_row_group_finished()
    {
        export_row_group_finished.increment();
    }

    public void addexport_row_group_finished(long val)
    {
        export_row_group_finished.add(val);
    }

    public void setexport_row_group_finished(long val)
    {
        export_row_group_finished.reset();
        addexport_row_group_finished(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_row_group_accomplished()
    {
        return export_row_group_accomplished.longValue();
    }

    public void incexport_row_group_accomplished()
    {
        export_row_group_accomplished.increment();
    }

    public void addexport_row_group_accomplished(long val)
    {
        export_row_group_accomplished.add(val);
    }

    public void setexport_row_group_accomplished(long val)
    {
        export_row_group_accomplished.reset();
        addexport_row_group_accomplished(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_row_group_failed()
    {
        return export_row_group_failed.longValue();
    }

    public void incexport_row_group_failed()
    {
        export_row_group_failed.increment();
    }

    public void addexport_row_group_failed(long val)
    {
        export_row_group_failed.add(val);
    }

    public void setexport_row_group_failed(long val)
    {
        export_row_group_failed.reset();
        addexport_row_group_failed(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_row_group_1st_footer_validation()
    {
        return export_row_group_1st_footer_validation.longValue();
    }

    public void incexport_row_group_1st_footer_validation()
    {
        export_row_group_1st_footer_validation.increment();
    }

    public void addexport_row_group_1st_footer_validation(long val)
    {
        export_row_group_1st_footer_validation.add(val);
    }

    public void setexport_row_group_1st_footer_validation(long val)
    {
        export_row_group_1st_footer_validation.reset();
        addexport_row_group_1st_footer_validation(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_row_group_2nd_footer_validation()
    {
        return export_row_group_2nd_footer_validation.longValue();
    }

    public void incexport_row_group_2nd_footer_validation()
    {
        export_row_group_2nd_footer_validation.increment();
    }

    public void addexport_row_group_2nd_footer_validation(long val)
    {
        export_row_group_2nd_footer_validation.add(val);
    }

    public void setexport_row_group_2nd_footer_validation(long val)
    {
        export_row_group_2nd_footer_validation.reset();
        addexport_row_group_2nd_footer_validation(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_row_group_failed_due_local_sparse()
    {
        return export_row_group_failed_due_local_sparse.longValue();
    }

    public void incexport_row_group_failed_due_local_sparse()
    {
        export_row_group_failed_due_local_sparse.increment();
    }

    public void addexport_row_group_failed_due_local_sparse(long val)
    {
        export_row_group_failed_due_local_sparse.add(val);
    }

    public void setexport_row_group_failed_due_local_sparse(long val)
    {
        export_row_group_failed_due_local_sparse.reset();
        addexport_row_group_failed_due_local_sparse(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_skipped_due_demoter()
    {
        return export_skipped_due_demoter.longValue();
    }

    public void incexport_skipped_due_demoter()
    {
        export_skipped_due_demoter.increment();
    }

    public void addexport_skipped_due_demoter(long val)
    {
        export_skipped_due_demoter.add(val);
    }

    public void setexport_skipped_due_demoter(long val)
    {
        export_skipped_due_demoter.reset();
        addexport_skipped_due_demoter(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_skipped_due_row_group()
    {
        return export_skipped_due_row_group.longValue();
    }

    public void incexport_skipped_due_row_group()
    {
        export_skipped_due_row_group.increment();
    }

    public void addexport_skipped_due_row_group(long val)
    {
        export_skipped_due_row_group.add(val);
    }

    public void setexport_skipped_due_row_group(long val)
    {
        export_skipped_due_row_group.reset();
        addexport_skipped_due_row_group(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_skipped_due_key_conflict()
    {
        return export_skipped_due_key_conflict.longValue();
    }

    public void incexport_skipped_due_key_conflict()
    {
        export_skipped_due_key_conflict.increment();
    }

    public void addexport_skipped_due_key_conflict(long val)
    {
        export_skipped_due_key_conflict.add(val);
    }

    public void setexport_skipped_due_key_conflict(long val)
    {
        export_skipped_due_key_conflict.reset();
        addexport_skipped_due_key_conflict(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_skipped_due_key_demoted_row_group()
    {
        return export_skipped_due_key_demoted_row_group.longValue();
    }

    public void incexport_skipped_due_key_demoted_row_group()
    {
        export_skipped_due_key_demoted_row_group.increment();
    }

    public void addexport_skipped_due_key_demoted_row_group(long val)
    {
        export_skipped_due_key_demoted_row_group.add(val);
    }

    public void setexport_skipped_due_key_demoted_row_group(long val)
    {
        export_skipped_due_key_demoted_row_group.reset();
        addexport_skipped_due_key_demoted_row_group(val);
    }

    @JsonIgnore
    @Managed
    public long getexport_time_Count()
    {
        return export_time_Count.longValue();
    }

    @Managed
    public long getexport_time_Average()
    {
        if (export_time_Count.longValue() == 0) {
            return 0;
        }
        return export_time.longValue() / export_time_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getexport_time()
    {
        return export_time.longValue();
    }

    public void addexport_time(long val)
    {
        export_time.add(val);
        export_time_Count.add(1);
    }

    public static VaradaStatsWarmupExportService create(String group)
    {
        return new VaradaStatsWarmupExportService(group);
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
        ret.put("empty_column_list", empty_column_list);
        ret.put("export_row_group_scheduled", export_row_group_scheduled);
        ret.put("export_row_group_started", export_row_group_started);
        ret.put("export_row_group_finished", export_row_group_finished);
        ret.put("export_row_group_accomplished", export_row_group_accomplished);
        ret.put("export_row_group_failed", export_row_group_failed);
        ret.put("export_row_group_1st_footer_validation", export_row_group_1st_footer_validation);
        ret.put("export_row_group_2nd_footer_validation", export_row_group_2nd_footer_validation);
        ret.put("export_row_group_failed_due_local_sparse", export_row_group_failed_due_local_sparse);
        ret.put("export_skipped_due_demoter", export_skipped_due_demoter);
        ret.put("export_skipped_due_row_group", export_skipped_due_row_group);
        ret.put("export_skipped_due_key_conflict", export_skipped_due_key_conflict);
        ret.put("export_skipped_due_key_demoted_row_group", export_skipped_due_key_demoted_row_group);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsWarmupExportService other = (VaradaStatsWarmupExportService) varadaStatsBase;
        this.empty_column_list.add(other.empty_column_list.longValue());
        this.export_row_group_scheduled.add(other.export_row_group_scheduled.longValue());
        this.export_row_group_started.add(other.export_row_group_started.longValue());
        this.export_row_group_finished.add(other.export_row_group_finished.longValue());
        this.export_row_group_accomplished.add(other.export_row_group_accomplished.longValue());
        this.export_row_group_failed.add(other.export_row_group_failed.longValue());
        this.export_row_group_1st_footer_validation.add(other.export_row_group_1st_footer_validation.longValue());
        this.export_row_group_2nd_footer_validation.add(other.export_row_group_2nd_footer_validation.longValue());
        this.export_row_group_failed_due_local_sparse.add(other.export_row_group_failed_due_local_sparse.longValue());
        this.export_skipped_due_demoter.add(other.export_skipped_due_demoter.longValue());
        this.export_skipped_due_row_group.add(other.export_skipped_due_row_group.longValue());
        this.export_skipped_due_key_conflict.add(other.export_skipped_due_key_conflict.longValue());
        this.export_skipped_due_key_demoted_row_group.add(other.export_skipped_due_key_demoted_row_group.longValue());
        this.export_time.add(other.export_time.longValue());
        this.export_time_Count.add(other.export_time_Count.longValue());
    }

    @Override
    public void reset()
    {
        empty_column_list.reset();
        export_row_group_scheduled.reset();
        export_row_group_started.reset();
        export_row_group_finished.reset();
        export_row_group_accomplished.reset();
        export_row_group_failed.reset();
        export_row_group_1st_footer_validation.reset();
        export_row_group_2nd_footer_validation.reset();
        export_row_group_failed_due_local_sparse.reset();
        export_skipped_due_demoter.reset();
        export_skipped_due_row_group.reset();
        export_skipped_due_key_conflict.reset();
        export_skipped_due_key_demoted_row_group.reset();
        export_time.reset();
        export_time_Count.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":empty_column_list", empty_column_list.longValue());
        res.put(getJmxKey() + ":export_row_group_scheduled", export_row_group_scheduled.longValue());
        res.put(getJmxKey() + ":export_row_group_started", export_row_group_started.longValue());
        res.put(getJmxKey() + ":export_row_group_finished", export_row_group_finished.longValue());
        res.put(getJmxKey() + ":export_row_group_accomplished", export_row_group_accomplished.longValue());
        res.put(getJmxKey() + ":export_row_group_failed", export_row_group_failed.longValue());
        res.put(getJmxKey() + ":export_row_group_1st_footer_validation", export_row_group_1st_footer_validation.longValue());
        res.put(getJmxKey() + ":export_row_group_2nd_footer_validation", export_row_group_2nd_footer_validation.longValue());
        res.put(getJmxKey() + ":export_row_group_failed_due_local_sparse", export_row_group_failed_due_local_sparse.longValue());
        res.put(getJmxKey() + ":export_skipped_due_demoter", export_skipped_due_demoter.longValue());
        res.put(getJmxKey() + ":export_skipped_due_row_group", export_skipped_due_row_group.longValue());
        res.put(getJmxKey() + ":export_skipped_due_key_conflict", export_skipped_due_key_conflict.longValue());
        res.put(getJmxKey() + ":export_skipped_due_key_demoted_row_group", export_skipped_due_key_demoted_row_group.longValue());
        res.put(getJmxKey() + ":export_time", export_time.longValue());
        res.put(getJmxKey() + ":export_time_Count", export_time_Count.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("empty_column_list", getempty_column_list());
        res.put("export_row_group_scheduled", getexport_row_group_scheduled());
        res.put("export_row_group_started", getexport_row_group_started());
        res.put("export_row_group_finished", getexport_row_group_finished());
        res.put("export_row_group_accomplished", getexport_row_group_accomplished());
        res.put("export_row_group_failed", getexport_row_group_failed());
        res.put("export_row_group_1st_footer_validation", getexport_row_group_1st_footer_validation());
        res.put("export_row_group_2nd_footer_validation", getexport_row_group_2nd_footer_validation());
        res.put("export_row_group_failed_due_local_sparse", getexport_row_group_failed_due_local_sparse());
        res.put("export_skipped_due_demoter", getexport_skipped_due_demoter());
        res.put("export_skipped_due_row_group", getexport_skipped_due_row_group());
        res.put("export_skipped_due_key_conflict", getexport_skipped_due_key_conflict());
        res.put("export_skipped_due_key_demoted_row_group", getexport_skipped_due_key_demoted_row_group());
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
