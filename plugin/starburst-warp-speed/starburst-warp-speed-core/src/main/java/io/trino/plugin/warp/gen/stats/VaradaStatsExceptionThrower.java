
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
public final class VaradaStatsExceptionThrower
        extends VaradaStatsBase
{
    /* This class file is auto-generated from exceptionThrower xml file for statistics and counters */
    private final String group;

    private final LongAdder recoverable = new LongAdder();
    private final LongAdder nonRecoverable = new LongAdder();

    @JsonCreator
    public VaradaStatsExceptionThrower(@JsonProperty("group") String group)
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
    public long getrecoverable()
    {
        return recoverable.longValue();
    }

    public void increcoverable()
    {
        recoverable.increment();
    }

    public void addrecoverable(long val)
    {
        recoverable.add(val);
    }

    public void setrecoverable(long val)
    {
        recoverable.reset();
        addrecoverable(val);
    }

    @JsonIgnore
    @Managed
    public long getnonRecoverable()
    {
        return nonRecoverable.longValue();
    }

    public void incnonRecoverable()
    {
        nonRecoverable.increment();
    }

    public void addnonRecoverable(long val)
    {
        nonRecoverable.add(val);
    }

    public void setnonRecoverable(long val)
    {
        nonRecoverable.reset();
        addnonRecoverable(val);
    }

    public static VaradaStatsExceptionThrower create(String group)
    {
        return new VaradaStatsExceptionThrower(group);
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
        ret.put("recoverable", recoverable);
        ret.put("nonRecoverable", nonRecoverable);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsExceptionThrower other = (VaradaStatsExceptionThrower) varadaStatsBase;
        this.recoverable.add(other.recoverable.longValue());
        this.nonRecoverable.add(other.nonRecoverable.longValue());
    }

    @Override
    public void reset()
    {
        recoverable.reset();
        nonRecoverable.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":recoverable", recoverable.longValue());
        res.put(getJmxKey() + ":nonRecoverable", nonRecoverable.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("recoverable", getrecoverable());
        res.put("nonRecoverable", getnonRecoverable());
        return res;
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
