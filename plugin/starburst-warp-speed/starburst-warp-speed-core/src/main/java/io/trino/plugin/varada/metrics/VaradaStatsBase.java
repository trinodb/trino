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
package io.trino.plugin.varada.metrics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class VaradaStatsBase
{
    @JsonIgnore
    private final String jmxKey;
    private final VaradaStatType varadaStatType;
    private Map auditMap = Collections.emptyMap();

    protected VaradaStatsBase(String jmxKey, VaradaStatType varadaStatType)
    {
        this.jmxKey = jmxKey;
        this.varadaStatType = varadaStatType;
    }

    @JsonIgnore
    public String getJmxKey()
    {
        return jmxKey;
    }

    @JsonIgnore
    public Map getAuditMap()
    {
        return auditMap;
    }

    public Map<String, LongAdder> getCounters()
    {
        return Collections.emptyMap();
    }

    public void reset()
    {
    }

    public void merge(VaradaStatsBase varadaStatsBase)
    {
        throw new UnsupportedOperationException();
    }

    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        throw new UnsupportedOperationException();
    }

    public <K, V> Map<K, V> statsCounterMapper()
    {
        return Collections.emptyMap();
    }

    protected <K, V> Map<K, V> deltaPrintFields()
    {
        return Collections.emptyMap();
    }

    protected <K, V> Map<K, V> statePrintFields()
    {
        return Collections.emptyMap();
    }

    public VaradaStatType getVaradaStatType()
    {
        return varadaStatType;
    }

    public boolean hasPersistentMetric()
    {
        return false;
    }

    public <K, V> Map<K, V> printStatsMap()
    {
        Map newAuditMap = deltaPrintFields();
        Map res = new HashMap();
        newAuditMap.forEach((k, v) -> {
            Long prevVal = (Long) auditMap.get(k);
            if (prevVal == null || !prevVal.equals(v)) {
                res.put(k, formatDeltaValue((Long) v, prevVal));
            }
        });
        res.putAll(statePrintFields());
        auditMap = newAuditMap;
        return res;
    }

    private String formatDeltaValue(Long newVal, Long prevVal)
    {
        long diff = prevVal == null ? newVal : newVal - prevVal;
        String sign = diff >= 0 ? "+" : "-";
        return String.format("%s%d (%d)", sign, Math.abs(diff), newVal);
    }
}
