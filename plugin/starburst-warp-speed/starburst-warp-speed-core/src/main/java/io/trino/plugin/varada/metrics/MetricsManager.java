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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@Singleton
public class MetricsManager
{
    private static final Logger logger = Logger.get(MetricsManager.class);

    private final MetricsRegistry metricsRegistry;

    @Inject
    public MetricsManager(
            MetricsRegistry metricsRegistry)
    {
        this.metricsRegistry = requireNonNull(metricsRegistry);
    }

    /**
     * Register a metric
     *
     * @param varadaStatsBase -
     * @return the registered instance of the key provided by {varadaStatsBase}
     */
    public <T extends VaradaStatsBase> T registerMetric(T varadaStatsBase)
    {
        T ret = varadaStatsBase;
        String jmxKey = varadaStatsBase.getJmxKey();
        logger.debug("exporting metric key %s", metricsRegistry.getKey(jmxKey));
        if (!metricsRegistry.registerMetric(varadaStatsBase)) {
            ret = (T) metricsRegistry.get(jmxKey);
        }
        return ret;
    }

    public void unregisterMetric(String key)
    {
        logger.debug("remove exporting metric key %s", key);
        metricsRegistry.unregisterMetric(key);
    }

    public VaradaStatsBase get(String key)
    {
        return metricsRegistry.get(key);
    }

    public Map<String, VaradaStatsBase> getAll()
    {
        return metricsRegistry.getAll();
    }
}
