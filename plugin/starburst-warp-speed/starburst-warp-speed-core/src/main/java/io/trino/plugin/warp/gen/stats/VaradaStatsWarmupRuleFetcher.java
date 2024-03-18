
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
public final class VaradaStatsWarmupRuleFetcher
        extends VaradaStatsBase
{
    /* This class file is auto-generated from warmupRuleFetcher xml file for statistics and counters */
    private final String group;

    private final LongAdder success = new LongAdder();
    private final LongAdder fail = new LongAdder();

    @JsonCreator
    public VaradaStatsWarmupRuleFetcher(@JsonProperty("group") String group)
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
    public long getsuccess()
    {
        return success.longValue();
    }

    public void incsuccess()
    {
        success.increment();
    }

    public void addsuccess(long val)
    {
        success.add(val);
    }

    public void setsuccess(long val)
    {
        success.reset();
        addsuccess(val);
    }

    @JsonIgnore
    @Managed
    public long getfail()
    {
        return fail.longValue();
    }

    public void incfail()
    {
        fail.increment();
    }

    public void addfail(long val)
    {
        fail.add(val);
    }

    public void setfail(long val)
    {
        fail.reset();
        addfail(val);
    }

    public static VaradaStatsWarmupRuleFetcher create(String group)
    {
        return new VaradaStatsWarmupRuleFetcher(group);
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
        ret.put("success", success);
        ret.put("fail", fail);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsWarmupRuleFetcher other = (VaradaStatsWarmupRuleFetcher) varadaStatsBase;
        this.success.add(other.success.longValue());
        this.fail.add(other.fail.longValue());
    }

    @Override
    public void reset()
    {
        success.reset();
        fail.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":success", success.longValue());
        res.put(getJmxKey() + ":fail", fail.longValue());
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
