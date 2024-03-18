
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

import io.trino.plugin.varada.metrics.MetricsManager;

@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName"})
public final class VaradaStatsMgr
{
    /* This class file is auto-generated from xml file for statistics and counters */

    public VaradaStatsMgr(MetricsManager metricsManager)
    {
        metricsManager.registerMetric(new VaradaStatsBloom());
        metricsManager.registerMetric(new VaradaStatsBtree());
        metricsManager.registerMetric(new VaradaStatsCollecttime());
        metricsManager.registerMetric(new VaradaStatsConnector());
        metricsManager.registerMetric(new VaradaStatsDatacompression());
        metricsManager.registerMetric(new VaradaStatsFastwarming());
        metricsManager.registerMetric(new VaradaStatsIndexcompression());
        metricsManager.registerMetric(new VaradaStatsLucene());
        metricsManager.registerMetric(new VaradaStatsMatchtime());
        metricsManager.registerMetric(new VaradaStatsMemory());
        metricsManager.registerMetric(new VaradaStatsQueryresult());
        metricsManager.registerMetric(new VaradaStatsRowgroup());
        metricsManager.registerMetric(new VaradaStatsStoragecache());
        metricsManager.registerMetric(new VaradaStatsStorageio());
        metricsManager.registerMetric(new VaradaStatsStorageusage());
    }
}
