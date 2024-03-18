
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
public final class VaradaStatsWarmingService
        extends VaradaStatsBase
{
    /* This class file is auto-generated from warmingService xml file for statistics and counters */
    private final String group;

    private final LongAdder warming_disabled = new LongAdder();
    private final LongAdder empty_column_list = new LongAdder();
    private final LongAdder warm_scheduled = new LongAdder();
    private final LongAdder warm_started = new LongAdder();
    private final LongAdder warm_finished = new LongAdder();
    private final LongAdder warm_accomplished = new LongAdder();
    private final LongAdder warm_failed = new LongAdder();
    private final LongAdder warm_skipped_due_demoter = new LongAdder();
    private final LongAdder warm_skipped_due_loaders_exceeded = new LongAdder();
    private final LongAdder warm_skipped_due_reaching_threshold = new LongAdder();
    private final LongAdder warm_skipped_due_queue_size = new LongAdder();
    private final LongAdder warm_skipped_due_key_conflict = new LongAdder();
    private final LongAdder warm_skip_permanent_failed_warmup_element = new LongAdder();
    private final LongAdder warm_skip_temporary_failed_warmup_element = new LongAdder();
    private final LongAdder warm_begin_retry_warmup_element = new LongAdder();
    private final LongAdder warm_success_retry_warmup_element = new LongAdder();
    private final LongAdder all_elements_warmed_or_skipped = new LongAdder();
    private final LongAdder row_group_count = new LongAdder();
    private final LongAdder warm_warp_cache_started = new LongAdder();
    private final LongAdder warm_warp_cache_accomplished = new LongAdder();
    private final LongAdder warm_warp_cache_failed = new LongAdder();
    private final LongAdder warm_warp_cache_invalid_type = new LongAdder();
    private final LongAdder warmup_elements_count = new LongAdder();
    private final LongAdder empty_row_group = new LongAdder();
    private final LongAdder deleted_warmup_elements_count = new LongAdder();
    private final LongAdder deleted_row_group_count = new LongAdder();
    private final LongAdder failed_fetching_rules = new LongAdder();
    private final LongAdder waiting_for_lock_nano_Count = new LongAdder();
    private final LongAdder waiting_for_lock_nano = new LongAdder();
    private final LongAdder execution_time_nano_Count = new LongAdder();
    private final LongAdder execution_time_nano = new LongAdder();

    @JsonCreator
    public VaradaStatsWarmingService(@JsonProperty("group") String group)
    {
        super(createKey(group), VaradaStatType.Coordinator);

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
    public long getwarming_disabled()
    {
        return warming_disabled.longValue();
    }

    public void incwarming_disabled()
    {
        warming_disabled.increment();
    }

    public void addwarming_disabled(long val)
    {
        warming_disabled.add(val);
    }

    public void setwarming_disabled(long val)
    {
        warming_disabled.reset();
        addwarming_disabled(val);
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
    public long getwarm_scheduled()
    {
        return warm_scheduled.longValue();
    }

    public void incwarm_scheduled()
    {
        warm_scheduled.increment();
    }

    public void addwarm_scheduled(long val)
    {
        warm_scheduled.add(val);
    }

    public void setwarm_scheduled(long val)
    {
        warm_scheduled.reset();
        addwarm_scheduled(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_started()
    {
        return warm_started.longValue();
    }

    public void incwarm_started()
    {
        warm_started.increment();
    }

    public void addwarm_started(long val)
    {
        warm_started.add(val);
    }

    public void setwarm_started(long val)
    {
        warm_started.reset();
        addwarm_started(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_finished()
    {
        return warm_finished.longValue();
    }

    public void incwarm_finished()
    {
        warm_finished.increment();
    }

    public void addwarm_finished(long val)
    {
        warm_finished.add(val);
    }

    public void setwarm_finished(long val)
    {
        warm_finished.reset();
        addwarm_finished(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_accomplished()
    {
        return warm_accomplished.longValue();
    }

    public void incwarm_accomplished()
    {
        warm_accomplished.increment();
    }

    public void addwarm_accomplished(long val)
    {
        warm_accomplished.add(val);
    }

    public void setwarm_accomplished(long val)
    {
        warm_accomplished.reset();
        addwarm_accomplished(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_failed()
    {
        return warm_failed.longValue();
    }

    public void incwarm_failed()
    {
        warm_failed.increment();
    }

    public void addwarm_failed(long val)
    {
        warm_failed.add(val);
    }

    public void setwarm_failed(long val)
    {
        warm_failed.reset();
        addwarm_failed(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_skipped_due_demoter()
    {
        return warm_skipped_due_demoter.longValue();
    }

    public void incwarm_skipped_due_demoter()
    {
        warm_skipped_due_demoter.increment();
    }

    public void addwarm_skipped_due_demoter(long val)
    {
        warm_skipped_due_demoter.add(val);
    }

    public void setwarm_skipped_due_demoter(long val)
    {
        warm_skipped_due_demoter.reset();
        addwarm_skipped_due_demoter(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_skipped_due_loaders_exceeded()
    {
        return warm_skipped_due_loaders_exceeded.longValue();
    }

    public void incwarm_skipped_due_loaders_exceeded()
    {
        warm_skipped_due_loaders_exceeded.increment();
    }

    public void addwarm_skipped_due_loaders_exceeded(long val)
    {
        warm_skipped_due_loaders_exceeded.add(val);
    }

    public void setwarm_skipped_due_loaders_exceeded(long val)
    {
        warm_skipped_due_loaders_exceeded.reset();
        addwarm_skipped_due_loaders_exceeded(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_skipped_due_reaching_threshold()
    {
        return warm_skipped_due_reaching_threshold.longValue();
    }

    public void incwarm_skipped_due_reaching_threshold()
    {
        warm_skipped_due_reaching_threshold.increment();
    }

    public void addwarm_skipped_due_reaching_threshold(long val)
    {
        warm_skipped_due_reaching_threshold.add(val);
    }

    public void setwarm_skipped_due_reaching_threshold(long val)
    {
        warm_skipped_due_reaching_threshold.reset();
        addwarm_skipped_due_reaching_threshold(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_skipped_due_queue_size()
    {
        return warm_skipped_due_queue_size.longValue();
    }

    public void incwarm_skipped_due_queue_size()
    {
        warm_skipped_due_queue_size.increment();
    }

    public void addwarm_skipped_due_queue_size(long val)
    {
        warm_skipped_due_queue_size.add(val);
    }

    public void setwarm_skipped_due_queue_size(long val)
    {
        warm_skipped_due_queue_size.reset();
        addwarm_skipped_due_queue_size(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_skipped_due_key_conflict()
    {
        return warm_skipped_due_key_conflict.longValue();
    }

    public void incwarm_skipped_due_key_conflict()
    {
        warm_skipped_due_key_conflict.increment();
    }

    public void addwarm_skipped_due_key_conflict(long val)
    {
        warm_skipped_due_key_conflict.add(val);
    }

    public void setwarm_skipped_due_key_conflict(long val)
    {
        warm_skipped_due_key_conflict.reset();
        addwarm_skipped_due_key_conflict(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_skip_permanent_failed_warmup_element()
    {
        return warm_skip_permanent_failed_warmup_element.longValue();
    }

    public void incwarm_skip_permanent_failed_warmup_element()
    {
        warm_skip_permanent_failed_warmup_element.increment();
    }

    public void addwarm_skip_permanent_failed_warmup_element(long val)
    {
        warm_skip_permanent_failed_warmup_element.add(val);
    }

    public void setwarm_skip_permanent_failed_warmup_element(long val)
    {
        warm_skip_permanent_failed_warmup_element.reset();
        addwarm_skip_permanent_failed_warmup_element(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_skip_temporary_failed_warmup_element()
    {
        return warm_skip_temporary_failed_warmup_element.longValue();
    }

    public void incwarm_skip_temporary_failed_warmup_element()
    {
        warm_skip_temporary_failed_warmup_element.increment();
    }

    public void addwarm_skip_temporary_failed_warmup_element(long val)
    {
        warm_skip_temporary_failed_warmup_element.add(val);
    }

    public void setwarm_skip_temporary_failed_warmup_element(long val)
    {
        warm_skip_temporary_failed_warmup_element.reset();
        addwarm_skip_temporary_failed_warmup_element(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_begin_retry_warmup_element()
    {
        return warm_begin_retry_warmup_element.longValue();
    }

    public void incwarm_begin_retry_warmup_element()
    {
        warm_begin_retry_warmup_element.increment();
    }

    public void addwarm_begin_retry_warmup_element(long val)
    {
        warm_begin_retry_warmup_element.add(val);
    }

    public void setwarm_begin_retry_warmup_element(long val)
    {
        warm_begin_retry_warmup_element.reset();
        addwarm_begin_retry_warmup_element(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_success_retry_warmup_element()
    {
        return warm_success_retry_warmup_element.longValue();
    }

    public void incwarm_success_retry_warmup_element()
    {
        warm_success_retry_warmup_element.increment();
    }

    public void addwarm_success_retry_warmup_element(long val)
    {
        warm_success_retry_warmup_element.add(val);
    }

    public void setwarm_success_retry_warmup_element(long val)
    {
        warm_success_retry_warmup_element.reset();
        addwarm_success_retry_warmup_element(val);
    }

    @JsonIgnore
    @Managed
    public long getall_elements_warmed_or_skipped()
    {
        return all_elements_warmed_or_skipped.longValue();
    }

    public void incall_elements_warmed_or_skipped()
    {
        all_elements_warmed_or_skipped.increment();
    }

    public void addall_elements_warmed_or_skipped(long val)
    {
        all_elements_warmed_or_skipped.add(val);
    }

    public void setall_elements_warmed_or_skipped(long val)
    {
        all_elements_warmed_or_skipped.reset();
        addall_elements_warmed_or_skipped(val);
    }

    @JsonIgnore
    @Managed
    public long getrow_group_count()
    {
        return row_group_count.longValue();
    }

    public void incrow_group_count()
    {
        row_group_count.increment();
    }

    public void addrow_group_count(long val)
    {
        row_group_count.add(val);
    }

    public void setrow_group_count(long val)
    {
        row_group_count.reset();
        addrow_group_count(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_warp_cache_started()
    {
        return warm_warp_cache_started.longValue();
    }

    public void incwarm_warp_cache_started()
    {
        warm_warp_cache_started.increment();
    }

    public void addwarm_warp_cache_started(long val)
    {
        warm_warp_cache_started.add(val);
    }

    public void setwarm_warp_cache_started(long val)
    {
        warm_warp_cache_started.reset();
        addwarm_warp_cache_started(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_warp_cache_accomplished()
    {
        return warm_warp_cache_accomplished.longValue();
    }

    public void incwarm_warp_cache_accomplished()
    {
        warm_warp_cache_accomplished.increment();
    }

    public void addwarm_warp_cache_accomplished(long val)
    {
        warm_warp_cache_accomplished.add(val);
    }

    public void setwarm_warp_cache_accomplished(long val)
    {
        warm_warp_cache_accomplished.reset();
        addwarm_warp_cache_accomplished(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_warp_cache_failed()
    {
        return warm_warp_cache_failed.longValue();
    }

    public void incwarm_warp_cache_failed()
    {
        warm_warp_cache_failed.increment();
    }

    public void addwarm_warp_cache_failed(long val)
    {
        warm_warp_cache_failed.add(val);
    }

    public void setwarm_warp_cache_failed(long val)
    {
        warm_warp_cache_failed.reset();
        addwarm_warp_cache_failed(val);
    }

    @JsonIgnore
    @Managed
    public long getwarm_warp_cache_invalid_type()
    {
        return warm_warp_cache_invalid_type.longValue();
    }

    public void incwarm_warp_cache_invalid_type()
    {
        warm_warp_cache_invalid_type.increment();
    }

    public void addwarm_warp_cache_invalid_type(long val)
    {
        warm_warp_cache_invalid_type.add(val);
    }

    public void setwarm_warp_cache_invalid_type(long val)
    {
        warm_warp_cache_invalid_type.reset();
        addwarm_warp_cache_invalid_type(val);
    }

    @JsonIgnore
    @Managed
    public long getwarmup_elements_count()
    {
        return warmup_elements_count.longValue();
    }

    public void incwarmup_elements_count()
    {
        warmup_elements_count.increment();
    }

    public void addwarmup_elements_count(long val)
    {
        warmup_elements_count.add(val);
    }

    public void setwarmup_elements_count(long val)
    {
        warmup_elements_count.reset();
        addwarmup_elements_count(val);
    }

    @JsonIgnore
    @Managed
    public long getempty_row_group()
    {
        return empty_row_group.longValue();
    }

    public void incempty_row_group()
    {
        empty_row_group.increment();
    }

    public void addempty_row_group(long val)
    {
        empty_row_group.add(val);
    }

    public void setempty_row_group(long val)
    {
        empty_row_group.reset();
        addempty_row_group(val);
    }

    @JsonIgnore
    @Managed
    public long getdeleted_warmup_elements_count()
    {
        return deleted_warmup_elements_count.longValue();
    }

    public void incdeleted_warmup_elements_count()
    {
        deleted_warmup_elements_count.increment();
    }

    public void adddeleted_warmup_elements_count(long val)
    {
        deleted_warmup_elements_count.add(val);
    }

    public void setdeleted_warmup_elements_count(long val)
    {
        deleted_warmup_elements_count.reset();
        adddeleted_warmup_elements_count(val);
    }

    @JsonIgnore
    @Managed
    public long getdeleted_row_group_count()
    {
        return deleted_row_group_count.longValue();
    }

    public void incdeleted_row_group_count()
    {
        deleted_row_group_count.increment();
    }

    public void adddeleted_row_group_count(long val)
    {
        deleted_row_group_count.add(val);
    }

    public void setdeleted_row_group_count(long val)
    {
        deleted_row_group_count.reset();
        adddeleted_row_group_count(val);
    }

    @JsonIgnore
    @Managed
    public long getfailed_fetching_rules()
    {
        return failed_fetching_rules.longValue();
    }

    public void incfailed_fetching_rules()
    {
        failed_fetching_rules.increment();
    }

    public void addfailed_fetching_rules(long val)
    {
        failed_fetching_rules.add(val);
    }

    public void setfailed_fetching_rules(long val)
    {
        failed_fetching_rules.reset();
        addfailed_fetching_rules(val);
    }

    @JsonIgnore
    @Managed
    public long getwaiting_for_lock_nano_Count()
    {
        return waiting_for_lock_nano_Count.longValue();
    }

    @Managed
    public long getwaiting_for_lock_nano_Average()
    {
        if (waiting_for_lock_nano_Count.longValue() == 0) {
            return 0;
        }
        return waiting_for_lock_nano.longValue() / waiting_for_lock_nano_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getwaiting_for_lock_nano()
    {
        return waiting_for_lock_nano.longValue();
    }

    public void addwaiting_for_lock_nano(long val)
    {
        waiting_for_lock_nano.add(val);
        waiting_for_lock_nano_Count.add(1);
    }

    @JsonIgnore
    @Managed
    public long getexecution_time_nano_Count()
    {
        return execution_time_nano_Count.longValue();
    }

    @Managed
    public long getexecution_time_nano_Average()
    {
        if (execution_time_nano_Count.longValue() == 0) {
            return 0;
        }
        return execution_time_nano.longValue() / execution_time_nano_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getexecution_time_nano()
    {
        return execution_time_nano.longValue();
    }

    public void addexecution_time_nano(long val)
    {
        execution_time_nano.add(val);
        execution_time_nano_Count.add(1);
    }

    public static VaradaStatsWarmingService create(String group)
    {
        return new VaradaStatsWarmingService(group);
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
        ret.put("warming_disabled", warming_disabled);
        ret.put("empty_column_list", empty_column_list);
        ret.put("warm_scheduled", warm_scheduled);
        ret.put("warm_started", warm_started);
        ret.put("warm_finished", warm_finished);
        ret.put("warm_accomplished", warm_accomplished);
        ret.put("warm_failed", warm_failed);
        ret.put("warm_skipped_due_demoter", warm_skipped_due_demoter);
        ret.put("warm_skipped_due_loaders_exceeded", warm_skipped_due_loaders_exceeded);
        ret.put("warm_skipped_due_reaching_threshold", warm_skipped_due_reaching_threshold);
        ret.put("warm_skipped_due_queue_size", warm_skipped_due_queue_size);
        ret.put("warm_skipped_due_key_conflict", warm_skipped_due_key_conflict);
        ret.put("warm_skip_permanent_failed_warmup_element", warm_skip_permanent_failed_warmup_element);
        ret.put("warm_skip_temporary_failed_warmup_element", warm_skip_temporary_failed_warmup_element);
        ret.put("warm_begin_retry_warmup_element", warm_begin_retry_warmup_element);
        ret.put("warm_success_retry_warmup_element", warm_success_retry_warmup_element);
        ret.put("all_elements_warmed_or_skipped", all_elements_warmed_or_skipped);
        ret.put("row_group_count", row_group_count);
        ret.put("warm_warp_cache_started", warm_warp_cache_started);
        ret.put("warm_warp_cache_accomplished", warm_warp_cache_accomplished);
        ret.put("warm_warp_cache_failed", warm_warp_cache_failed);
        ret.put("warm_warp_cache_invalid_type", warm_warp_cache_invalid_type);
        ret.put("warmup_elements_count", warmup_elements_count);
        ret.put("empty_row_group", empty_row_group);
        ret.put("deleted_warmup_elements_count", deleted_warmup_elements_count);
        ret.put("deleted_row_group_count", deleted_row_group_count);
        ret.put("failed_fetching_rules", failed_fetching_rules);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsWarmingService other = (VaradaStatsWarmingService) varadaStatsBase;
        this.warming_disabled.add(other.warming_disabled.longValue());
        this.empty_column_list.add(other.empty_column_list.longValue());
        this.warm_scheduled.add(other.warm_scheduled.longValue());
        this.warm_started.add(other.warm_started.longValue());
        this.warm_finished.add(other.warm_finished.longValue());
        this.warm_accomplished.add(other.warm_accomplished.longValue());
        this.warm_failed.add(other.warm_failed.longValue());
        this.warm_skipped_due_demoter.add(other.warm_skipped_due_demoter.longValue());
        this.warm_skipped_due_loaders_exceeded.add(other.warm_skipped_due_loaders_exceeded.longValue());
        this.warm_skipped_due_reaching_threshold.add(other.warm_skipped_due_reaching_threshold.longValue());
        this.warm_skipped_due_queue_size.add(other.warm_skipped_due_queue_size.longValue());
        this.warm_skipped_due_key_conflict.add(other.warm_skipped_due_key_conflict.longValue());
        this.warm_skip_permanent_failed_warmup_element.add(other.warm_skip_permanent_failed_warmup_element.longValue());
        this.warm_skip_temporary_failed_warmup_element.add(other.warm_skip_temporary_failed_warmup_element.longValue());
        this.warm_begin_retry_warmup_element.add(other.warm_begin_retry_warmup_element.longValue());
        this.warm_success_retry_warmup_element.add(other.warm_success_retry_warmup_element.longValue());
        this.all_elements_warmed_or_skipped.add(other.all_elements_warmed_or_skipped.longValue());
        this.row_group_count.add(other.row_group_count.longValue());
        this.warm_warp_cache_started.add(other.warm_warp_cache_started.longValue());
        this.warm_warp_cache_accomplished.add(other.warm_warp_cache_accomplished.longValue());
        this.warm_warp_cache_failed.add(other.warm_warp_cache_failed.longValue());
        this.warm_warp_cache_invalid_type.add(other.warm_warp_cache_invalid_type.longValue());
        this.warmup_elements_count.add(other.warmup_elements_count.longValue());
        this.empty_row_group.add(other.empty_row_group.longValue());
        this.deleted_warmup_elements_count.add(other.deleted_warmup_elements_count.longValue());
        this.deleted_row_group_count.add(other.deleted_row_group_count.longValue());
        this.failed_fetching_rules.add(other.failed_fetching_rules.longValue());
        this.waiting_for_lock_nano.add(other.waiting_for_lock_nano.longValue());
        this.waiting_for_lock_nano_Count.add(other.waiting_for_lock_nano_Count.longValue());
        this.execution_time_nano.add(other.execution_time_nano.longValue());
        this.execution_time_nano_Count.add(other.execution_time_nano_Count.longValue());
    }

    @Override
    public void reset()
    {
        warming_disabled.reset();
        empty_column_list.reset();
        warm_scheduled.reset();
        warm_started.reset();
        warm_finished.reset();
        warm_accomplished.reset();
        warm_failed.reset();
        warm_skipped_due_demoter.reset();
        warm_skipped_due_loaders_exceeded.reset();
        warm_skipped_due_reaching_threshold.reset();
        warm_skipped_due_queue_size.reset();
        warm_skipped_due_key_conflict.reset();
        warm_skip_permanent_failed_warmup_element.reset();
        warm_skip_temporary_failed_warmup_element.reset();
        warm_begin_retry_warmup_element.reset();
        warm_success_retry_warmup_element.reset();
        all_elements_warmed_or_skipped.reset();
        row_group_count.reset();
        warm_warp_cache_started.reset();
        warm_warp_cache_accomplished.reset();
        warm_warp_cache_failed.reset();
        warm_warp_cache_invalid_type.reset();
        warmup_elements_count.reset();
        empty_row_group.reset();
        deleted_warmup_elements_count.reset();
        deleted_row_group_count.reset();
        failed_fetching_rules.reset();
        waiting_for_lock_nano.reset();
        waiting_for_lock_nano_Count.reset();
        execution_time_nano.reset();
        execution_time_nano_Count.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":warming_disabled", warming_disabled.longValue());
        res.put(getJmxKey() + ":empty_column_list", empty_column_list.longValue());
        res.put(getJmxKey() + ":warm_scheduled", warm_scheduled.longValue());
        res.put(getJmxKey() + ":warm_started", warm_started.longValue());
        res.put(getJmxKey() + ":warm_finished", warm_finished.longValue());
        res.put(getJmxKey() + ":warm_accomplished", warm_accomplished.longValue());
        res.put(getJmxKey() + ":warm_failed", warm_failed.longValue());
        res.put(getJmxKey() + ":warm_skipped_due_demoter", warm_skipped_due_demoter.longValue());
        res.put(getJmxKey() + ":warm_skipped_due_loaders_exceeded", warm_skipped_due_loaders_exceeded.longValue());
        res.put(getJmxKey() + ":warm_skipped_due_reaching_threshold", warm_skipped_due_reaching_threshold.longValue());
        res.put(getJmxKey() + ":warm_skipped_due_queue_size", warm_skipped_due_queue_size.longValue());
        res.put(getJmxKey() + ":warm_skipped_due_key_conflict", warm_skipped_due_key_conflict.longValue());
        res.put(getJmxKey() + ":warm_skip_permanent_failed_warmup_element", warm_skip_permanent_failed_warmup_element.longValue());
        res.put(getJmxKey() + ":warm_skip_temporary_failed_warmup_element", warm_skip_temporary_failed_warmup_element.longValue());
        res.put(getJmxKey() + ":warm_begin_retry_warmup_element", warm_begin_retry_warmup_element.longValue());
        res.put(getJmxKey() + ":warm_success_retry_warmup_element", warm_success_retry_warmup_element.longValue());
        res.put(getJmxKey() + ":all_elements_warmed_or_skipped", all_elements_warmed_or_skipped.longValue());
        res.put(getJmxKey() + ":row_group_count", row_group_count.longValue());
        res.put(getJmxKey() + ":warm_warp_cache_started", warm_warp_cache_started.longValue());
        res.put(getJmxKey() + ":warm_warp_cache_accomplished", warm_warp_cache_accomplished.longValue());
        res.put(getJmxKey() + ":warm_warp_cache_failed", warm_warp_cache_failed.longValue());
        res.put(getJmxKey() + ":warm_warp_cache_invalid_type", warm_warp_cache_invalid_type.longValue());
        res.put(getJmxKey() + ":warmup_elements_count", warmup_elements_count.longValue());
        res.put(getJmxKey() + ":empty_row_group", empty_row_group.longValue());
        res.put(getJmxKey() + ":deleted_warmup_elements_count", deleted_warmup_elements_count.longValue());
        res.put(getJmxKey() + ":deleted_row_group_count", deleted_row_group_count.longValue());
        res.put(getJmxKey() + ":failed_fetching_rules", failed_fetching_rules.longValue());
        res.put(getJmxKey() + ":waiting_for_lock_nano", waiting_for_lock_nano.longValue());
        res.put(getJmxKey() + ":waiting_for_lock_nano_Count", waiting_for_lock_nano_Count.longValue());
        res.put(getJmxKey() + ":execution_time_nano", execution_time_nano.longValue());
        res.put(getJmxKey() + ":execution_time_nano_Count", execution_time_nano_Count.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("warming_disabled", getwarming_disabled());
        res.put("warm_started", getwarm_started());
        res.put("warm_accomplished", getwarm_accomplished());
        res.put("warm_failed", getwarm_failed());
        res.put("warm_skipped_due_demoter", getwarm_skipped_due_demoter());
        res.put("warm_skipped_due_loaders_exceeded", getwarm_skipped_due_loaders_exceeded());
        res.put("warm_skipped_due_reaching_threshold", getwarm_skipped_due_reaching_threshold());
        res.put("warm_skipped_due_queue_size", getwarm_skipped_due_queue_size());
        res.put("warm_skipped_due_key_conflict", getwarm_skipped_due_key_conflict());
        res.put("warm_skip_permanent_failed_warmup_element", getwarm_skip_permanent_failed_warmup_element());
        res.put("warm_skip_temporary_failed_warmup_element", getwarm_skip_temporary_failed_warmup_element());
        res.put("warm_begin_retry_warmup_element", getwarm_begin_retry_warmup_element());
        res.put("warm_success_retry_warmup_element", getwarm_success_retry_warmup_element());
        res.put("row_group_count", getrow_group_count());
        res.put("warm_warp_cache_started", getwarm_warp_cache_started());
        res.put("warm_warp_cache_accomplished", getwarm_warp_cache_accomplished());
        res.put("warm_warp_cache_failed", getwarm_warp_cache_failed());
        res.put("warm_warp_cache_invalid_type", getwarm_warp_cache_invalid_type());
        res.put("warmup_elements_count", getwarmup_elements_count());
        res.put("empty_row_group", getempty_row_group());
        res.put("deleted_warmup_elements_count", getdeleted_warmup_elements_count());
        res.put("deleted_row_group_count", getdeleted_row_group_count());
        return res;
    }

    @Override
    protected Map<String, Object> statePrintFields()
    {
        return new HashMap<>();
    }

    public int getNumberOfMetrics()
    {
        return 27;
    }
}
