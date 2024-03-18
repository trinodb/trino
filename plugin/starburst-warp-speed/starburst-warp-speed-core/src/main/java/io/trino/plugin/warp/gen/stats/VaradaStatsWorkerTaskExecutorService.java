
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
public final class VaradaStatsWorkerTaskExecutorService
        extends VaradaStatsBase
{
    /* This class file is auto-generated from workerTaskExecutorService xml file for statistics and counters */
    private final String group;

    private final LongAdder task_scheduled = new LongAdder();
    private final LongAdder task_finished = new LongAdder();
    private final LongAdder task_skipped_due_queue_size = new LongAdder();
    private final LongAdder task_pended = new LongAdder();
    private final LongAdder task_resubmitted = new LongAdder();
    private final LongAdder task_delayed = new LongAdder();

    @JsonCreator
    public VaradaStatsWorkerTaskExecutorService(@JsonProperty("group") String group)
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
    public long gettask_scheduled()
    {
        return task_scheduled.longValue();
    }

    public void inctask_scheduled()
    {
        task_scheduled.increment();
    }

    public void addtask_scheduled(long val)
    {
        task_scheduled.add(val);
    }

    public void settask_scheduled(long val)
    {
        task_scheduled.reset();
        addtask_scheduled(val);
    }

    @JsonIgnore
    @Managed
    public long gettask_finished()
    {
        return task_finished.longValue();
    }

    public void inctask_finished()
    {
        task_finished.increment();
    }

    public void addtask_finished(long val)
    {
        task_finished.add(val);
    }

    public void settask_finished(long val)
    {
        task_finished.reset();
        addtask_finished(val);
    }

    @JsonIgnore
    @Managed
    public long gettask_skipped_due_queue_size()
    {
        return task_skipped_due_queue_size.longValue();
    }

    public void inctask_skipped_due_queue_size()
    {
        task_skipped_due_queue_size.increment();
    }

    public void addtask_skipped_due_queue_size(long val)
    {
        task_skipped_due_queue_size.add(val);
    }

    public void settask_skipped_due_queue_size(long val)
    {
        task_skipped_due_queue_size.reset();
        addtask_skipped_due_queue_size(val);
    }

    @JsonIgnore
    @Managed
    public long gettask_pended()
    {
        return task_pended.longValue();
    }

    public void inctask_pended()
    {
        task_pended.increment();
    }

    public void addtask_pended(long val)
    {
        task_pended.add(val);
    }

    public void settask_pended(long val)
    {
        task_pended.reset();
        addtask_pended(val);
    }

    @JsonIgnore
    @Managed
    public long gettask_resubmitted()
    {
        return task_resubmitted.longValue();
    }

    public void inctask_resubmitted()
    {
        task_resubmitted.increment();
    }

    public void addtask_resubmitted(long val)
    {
        task_resubmitted.add(val);
    }

    public void settask_resubmitted(long val)
    {
        task_resubmitted.reset();
        addtask_resubmitted(val);
    }

    @JsonIgnore
    @Managed
    public long gettask_delayed()
    {
        return task_delayed.longValue();
    }

    public void inctask_delayed()
    {
        task_delayed.increment();
    }

    public void addtask_delayed(long val)
    {
        task_delayed.add(val);
    }

    public void settask_delayed(long val)
    {
        task_delayed.reset();
        addtask_delayed(val);
    }

    public static VaradaStatsWorkerTaskExecutorService create(String group)
    {
        return new VaradaStatsWorkerTaskExecutorService(group);
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
        ret.put("task_scheduled", task_scheduled);
        ret.put("task_finished", task_finished);
        ret.put("task_skipped_due_queue_size", task_skipped_due_queue_size);
        ret.put("task_pended", task_pended);
        ret.put("task_resubmitted", task_resubmitted);
        ret.put("task_delayed", task_delayed);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsWorkerTaskExecutorService other = (VaradaStatsWorkerTaskExecutorService) varadaStatsBase;
        this.task_scheduled.add(other.task_scheduled.longValue());
        this.task_finished.add(other.task_finished.longValue());
        this.task_skipped_due_queue_size.add(other.task_skipped_due_queue_size.longValue());
        this.task_pended.add(other.task_pended.longValue());
        this.task_resubmitted.add(other.task_resubmitted.longValue());
        this.task_delayed.add(other.task_delayed.longValue());
    }

    @Override
    public void reset()
    {
        task_scheduled.reset();
        task_finished.reset();
        task_skipped_due_queue_size.reset();
        task_pended.reset();
        task_resubmitted.reset();
        task_delayed.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":task_scheduled", task_scheduled.longValue());
        res.put(getJmxKey() + ":task_finished", task_finished.longValue());
        res.put(getJmxKey() + ":task_skipped_due_queue_size", task_skipped_due_queue_size.longValue());
        res.put(getJmxKey() + ":task_pended", task_pended.longValue());
        res.put(getJmxKey() + ":task_resubmitted", task_resubmitted.longValue());
        res.put(getJmxKey() + ":task_delayed", task_delayed.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("task_scheduled", gettask_scheduled());
        res.put("task_finished", gettask_finished());
        res.put("task_skipped_due_queue_size", gettask_skipped_due_queue_size());
        res.put("task_pended", gettask_pended());
        res.put("task_resubmitted", gettask_resubmitted());
        res.put("task_delayed", gettask_delayed());
        return res;
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
}
