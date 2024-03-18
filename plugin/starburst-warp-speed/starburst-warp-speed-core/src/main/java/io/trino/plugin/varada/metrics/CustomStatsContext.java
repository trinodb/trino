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

import io.trino.plugin.varada.dispatcher.CustomStat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomStatsContext
{
    private final Map<String, VaradaStatsBase> registeredStats = new HashMap<>();
    private final Map<String, Long> fixedStats = new HashMap<>();
    private final MetricsManager metricsManager;

    public CustomStatsContext(MetricsManager metricsManager, List<CustomStat> customStats)
    {
        this.metricsManager = metricsManager;
        customStats.forEach(customStat -> fixedStats.put(customStat.statName(), customStat.statValue()));
    }

    public VaradaStatsBase getStat(String statKey)
    {
        return registeredStats.get(statKey);
    }

    public VaradaStatsBase getOrRegister(VaradaStatsBase varadaStatsBase)
    {
        if (registeredStats.containsKey(varadaStatsBase.getJmxKey())) {
            return registeredStats.get(varadaStatsBase.getJmxKey());
        }
        registeredStats.put(varadaStatsBase.getJmxKey(), varadaStatsBase);
        return varadaStatsBase;
    }

    public Map<String, VaradaStatsBase> getRegisteredStats()
    {
        return new HashMap<>(registeredStats);
    }

    public void addFixedStat(String key, long value)
    {
        fixedStats.put(key, value);
    }

    public Map<String, Long> getFixedStats()
    {
        return fixedStats;
    }

    public void copyStatsToGlobalMetricsManager()
    {
        registeredStats.forEach((key, value) -> {
            VaradaStatsBase varadaStatsBase = metricsManager.get(key);
            if (varadaStatsBase != null) {
                varadaStatsBase.mergeStats(value);
            }
        });
    }
}
