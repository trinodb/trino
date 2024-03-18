
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
public final class VaradaStatsFlowSequencer
        extends VaradaStatsBase
{
    /* This class file is auto-generated from flowSequencer xml file for statistics and counters */
    private final String group;
    private final String flowType;

    private final LongAdder flow_started = new LongAdder();
    private final LongAdder flow_finished = new LongAdder();

    @JsonCreator
    public VaradaStatsFlowSequencer(@JsonProperty("group") String group, @JsonProperty("flowType") String flowType)
    {
        super(createKey(group, flowType), VaradaStatType.Coordinator);

        this.group = group;
        this.flowType = flowType;
    }

    @JsonProperty
    @Managed
    public String getGroup()
    {
        return group;
    }

    @JsonProperty
    @Managed
    public String getFlowType()
    {
        return flowType;
    }

    @JsonIgnore
    @Managed
    public long getflow_started()
    {
        return flow_started.longValue();
    }

    public void incflow_started()
    {
        flow_started.increment();
    }

    public void addflow_started(long val)
    {
        flow_started.add(val);
    }

    public void setflow_started(long val)
    {
        flow_started.reset();
        addflow_started(val);
    }

    @JsonIgnore
    @Managed
    public long getflow_finished()
    {
        return flow_finished.longValue();
    }

    public void incflow_finished()
    {
        flow_finished.increment();
    }

    public void addflow_finished(long val)
    {
        flow_finished.add(val);
    }

    public void setflow_finished(long val)
    {
        flow_finished.reset();
        addflow_finished(val);
    }

    public static VaradaStatsFlowSequencer create(String group, String flowType)
    {
        return new VaradaStatsFlowSequencer(group, flowType);
    }

    public static String createKey(String group, String flowType)
    {
        return new StringJoiner(".").add(group).add(flowType).toString();
    }

    @Override
    public void merge(VaradaStatsBase varadaStatsBase)
    {
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("flow_started", flow_started);
        ret.put("flow_finished", flow_finished);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsFlowSequencer other = (VaradaStatsFlowSequencer) varadaStatsBase;
        this.flow_started.add(other.flow_started.longValue());
        this.flow_finished.add(other.flow_finished.longValue());
    }

    @Override
    public void reset()
    {
        flow_started.reset();
        flow_finished.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":flow_started", flow_started.longValue());
        res.put(getJmxKey() + ":flow_finished", flow_finished.longValue());
        return res;
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
}
