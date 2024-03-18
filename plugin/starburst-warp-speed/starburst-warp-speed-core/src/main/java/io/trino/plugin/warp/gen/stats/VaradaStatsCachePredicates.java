
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
public final class VaradaStatsCachePredicates
        extends VaradaStatsBase
{
    /* This class file is auto-generated from cachePredicates xml file for statistics and counters */
    private final String group;

    private final LongAdder in_use_small = new LongAdder();
    private final LongAdder in_use_medium = new LongAdder();
    private final LongAdder in_use_large = new LongAdder();

    @JsonCreator
    public VaradaStatsCachePredicates(@JsonProperty("group") String group)
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
    public long getin_use_small()
    {
        return in_use_small.longValue();
    }

    public void incin_use_small()
    {
        in_use_small.increment();
    }

    public void addin_use_small(long val)
    {
        in_use_small.add(val);
    }

    public void setin_use_small(long val)
    {
        in_use_small.reset();
        addin_use_small(val);
    }

    @JsonIgnore
    @Managed
    public long getin_use_medium()
    {
        return in_use_medium.longValue();
    }

    public void incin_use_medium()
    {
        in_use_medium.increment();
    }

    public void addin_use_medium(long val)
    {
        in_use_medium.add(val);
    }

    public void setin_use_medium(long val)
    {
        in_use_medium.reset();
        addin_use_medium(val);
    }

    @JsonIgnore
    @Managed
    public long getin_use_large()
    {
        return in_use_large.longValue();
    }

    public void incin_use_large()
    {
        in_use_large.increment();
    }

    public void addin_use_large(long val)
    {
        in_use_large.add(val);
    }

    public void setin_use_large(long val)
    {
        in_use_large.reset();
        addin_use_large(val);
    }

    public static VaradaStatsCachePredicates create(String group)
    {
        return new VaradaStatsCachePredicates(group);
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
        ret.put("in_use_small", in_use_small);
        ret.put("in_use_medium", in_use_medium);
        ret.put("in_use_large", in_use_large);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsCachePredicates other = (VaradaStatsCachePredicates) varadaStatsBase;
        this.in_use_small.add(other.in_use_small.longValue());
        this.in_use_medium.add(other.in_use_medium.longValue());
        this.in_use_large.add(other.in_use_large.longValue());
    }

    @Override
    public void reset()
    {
        in_use_small.reset();
        in_use_medium.reset();
        in_use_large.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":in_use_small", in_use_small.longValue());
        res.put(getJmxKey() + ":in_use_medium", in_use_medium.longValue());
        res.put(getJmxKey() + ":in_use_large", in_use_large.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("in_use_small", getin_use_small());
        res.put("in_use_medium", getin_use_medium());
        res.put("in_use_large", getin_use_large());
        return res;
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
