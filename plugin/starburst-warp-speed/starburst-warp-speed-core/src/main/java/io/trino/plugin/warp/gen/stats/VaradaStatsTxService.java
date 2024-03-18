
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
public final class VaradaStatsTxService
        extends VaradaStatsBase
{
    /* This class file is auto-generated from txService xml file for statistics and counters */
    private final String group;

    private final LongAdder currently_used = new LongAdder();
    private final LongAdder allocated = new LongAdder();
    private final LongAdder failed_allocated = new LongAdder();
    private final LongAdder released = new LongAdder();
    private final LongAdder failed_released = new LongAdder();
    private final LongAdder running_page_source = new LongAdder();
    private final LongAdder blocking_warmings = new LongAdder();

    @JsonCreator
    public VaradaStatsTxService(@JsonProperty("group") String group)
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
    public long getcurrently_used()
    {
        return currently_used.longValue();
    }

    public void inccurrently_used()
    {
        currently_used.increment();
    }

    public void addcurrently_used(long val)
    {
        currently_used.add(val);
    }

    public void setcurrently_used(long val)
    {
        currently_used.reset();
        addcurrently_used(val);
    }

    @JsonIgnore
    @Managed
    public long getallocated()
    {
        return allocated.longValue();
    }

    public void incallocated()
    {
        allocated.increment();
    }

    public void addallocated(long val)
    {
        allocated.add(val);
    }

    public void setallocated(long val)
    {
        allocated.reset();
        addallocated(val);
    }

    @JsonIgnore
    @Managed
    public long getfailed_allocated()
    {
        return failed_allocated.longValue();
    }

    public void incfailed_allocated()
    {
        failed_allocated.increment();
    }

    public void addfailed_allocated(long val)
    {
        failed_allocated.add(val);
    }

    public void setfailed_allocated(long val)
    {
        failed_allocated.reset();
        addfailed_allocated(val);
    }

    @JsonIgnore
    @Managed
    public long getreleased()
    {
        return released.longValue();
    }

    public void increleased()
    {
        released.increment();
    }

    public void addreleased(long val)
    {
        released.add(val);
    }

    public void setreleased(long val)
    {
        released.reset();
        addreleased(val);
    }

    @JsonIgnore
    @Managed
    public long getfailed_released()
    {
        return failed_released.longValue();
    }

    public void incfailed_released()
    {
        failed_released.increment();
    }

    public void addfailed_released(long val)
    {
        failed_released.add(val);
    }

    public void setfailed_released(long val)
    {
        failed_released.reset();
        addfailed_released(val);
    }

    @JsonIgnore
    @Managed
    public long getrunning_page_source()
    {
        return running_page_source.longValue();
    }

    public void incrunning_page_source()
    {
        running_page_source.increment();
    }

    public void addrunning_page_source(long val)
    {
        running_page_source.add(val);
    }

    public void setrunning_page_source(long val)
    {
        running_page_source.reset();
        addrunning_page_source(val);
    }

    @JsonIgnore
    @Managed
    public long getblocking_warmings()
    {
        return blocking_warmings.longValue();
    }

    public void incblocking_warmings()
    {
        blocking_warmings.increment();
    }

    public void addblocking_warmings(long val)
    {
        blocking_warmings.add(val);
    }

    public void setblocking_warmings(long val)
    {
        blocking_warmings.reset();
        addblocking_warmings(val);
    }

    public static VaradaStatsTxService create(String group)
    {
        return new VaradaStatsTxService(group);
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
        ret.put("currently_used", currently_used);
        ret.put("allocated", allocated);
        ret.put("failed_allocated", failed_allocated);
        ret.put("released", released);
        ret.put("failed_released", failed_released);
        ret.put("running_page_source", running_page_source);
        ret.put("blocking_warmings", blocking_warmings);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsTxService other = (VaradaStatsTxService) varadaStatsBase;
        this.currently_used.add(other.currently_used.longValue());
        this.allocated.add(other.allocated.longValue());
        this.failed_allocated.add(other.failed_allocated.longValue());
        this.released.add(other.released.longValue());
        this.failed_released.add(other.failed_released.longValue());
        this.running_page_source.add(other.running_page_source.longValue());
        this.blocking_warmings.add(other.blocking_warmings.longValue());
    }

    @Override
    public void reset()
    {
        currently_used.reset();
        allocated.reset();
        failed_allocated.reset();
        released.reset();
        failed_released.reset();
        running_page_source.reset();
        blocking_warmings.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":currently_used", currently_used.longValue());
        res.put(getJmxKey() + ":allocated", allocated.longValue());
        res.put(getJmxKey() + ":failed_allocated", failed_allocated.longValue());
        res.put(getJmxKey() + ":released", released.longValue());
        res.put(getJmxKey() + ":failed_released", failed_released.longValue());
        res.put(getJmxKey() + ":running_page_source", running_page_source.longValue());
        res.put(getJmxKey() + ":blocking_warmings", blocking_warmings.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("running_page_source", getrunning_page_source());
        res.put("blocking_warmings", getblocking_warmings());
        return res;
    }

    @Override
    protected Map<String, Object> statePrintFields()
    {
        return new HashMap<>();
    }

    public int getNumberOfMetrics()
    {
        return 7;
    }
}
