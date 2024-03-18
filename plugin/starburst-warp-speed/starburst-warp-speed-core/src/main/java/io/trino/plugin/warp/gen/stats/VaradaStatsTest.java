
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
public final class VaradaStatsTest
        extends VaradaStatsBase
{
    /* This class file is auto-generated from test xml file for statistics and counters */
    private final String group;

    private final LongAdder param1 = new LongAdder();
    private final LongAdder param2 = new LongAdder();
    private final LongAdder param3 = new LongAdder();

    @JsonCreator
    public VaradaStatsTest(@JsonProperty("group") String group)
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

    @Managed
    public long getparam1()
    {
        return param1.longValue();
    }

    public void incparam1()
    {
        param1.increment();
    }

    public void addparam1(long val)
    {
        param1.add(val);
    }

    public void setparam1(long val)
    {
        param1.reset();
        addparam1(val);
    }

    @Managed
    public long getparam2()
    {
        return param2.longValue();
    }

    public void incparam2()
    {
        param2.increment();
    }

    public void addparam2(long val)
    {
        param2.add(val);
    }

    public void setparam2(long val)
    {
        param2.reset();
        addparam2(val);
    }

    @JsonIgnore
    @Managed
    public long getparam3()
    {
        return param3.longValue();
    }

    public void incparam3()
    {
        param3.increment();
    }

    public void addparam3(long val)
    {
        param3.add(val);
    }

    public void setparam3(long val)
    {
        param3.reset();
        addparam3(val);
    }

    public static VaradaStatsTest create(String group)
    {
        return new VaradaStatsTest(group);
    }

    public static String createKey(String group)
    {
        return new StringJoiner(".").add(group).toString();
    }

    @Override
    public void merge(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase != null) {
            VaradaStatsTest other = (VaradaStatsTest) varadaStatsBase;
            addparam1(other.getparam1());
            addparam2(other.getparam2());
        }
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("param1", param1);
        ret.put("param2", param2);
        ret.put("param3", param3);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsTest other = (VaradaStatsTest) varadaStatsBase;
        this.param1.add(other.param1.longValue());
        this.param2.add(other.param2.longValue());
        this.param3.add(other.param3.longValue());
    }

    @Override
    public void reset()
    {
        param1.reset();
        param2.reset();
        param3.reset();
    }

    @Override
    public boolean hasPersistentMetric()
    {
        return true;
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":param1", param1.longValue());
        res.put(getJmxKey() + ":param2", param2.longValue());
        res.put(getJmxKey() + ":param3", param3.longValue());
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
        return 3;
    }
}
