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

import io.trino.plugin.varada.configuration.MetricsConfiguration;
import io.varada.tools.CatalogNameProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

public class CustomStatsContextTest
{
    private MetricsManager metricsManager;
    private CustomStatsContext customStatsContext;

    @BeforeEach
    public void before()
    {
        metricsManager = new MetricsManager(new MetricsRegistry(new CatalogNameProvider("catalog-name"), new MetricsConfiguration()));
        customStatsContext = new CustomStatsContext(metricsManager, Collections.emptyList());
    }

    @Test
    public void getOrRegisterTest()
    {
        String jmxKey = "test";
        VaradaStatsBase stats = new VaradaTestStats(jmxKey);
        assertThat(customStatsContext.getStat(jmxKey)).isNull();
        customStatsContext.getOrRegister(stats);
        assertThat(customStatsContext.getStat(jmxKey)).isNotNull();

        VaradaStatsBase statsNew = new VaradaTestStats(jmxKey);
        //does not create new instance
        assertThat(customStatsContext.getOrRegister(statsNew)).isEqualTo(stats);
    }

    @Test
    public void copyStatsToGlobalMetricsManagerTest()
    {
        final String jmxKey = "test";
        VaradaTestStats testStatsContext = new VaradaTestStats(jmxKey);
        customStatsContext.getOrRegister(testStatsContext);
        VaradaTestStats testStatsGlobal = new VaradaTestStats(jmxKey);
        metricsManager.registerMetric(testStatsGlobal);
        testStatsContext.incCounter();
        customStatsContext.copyStatsToGlobalMetricsManager();
        assertThat(((VaradaTestStats) metricsManager.get(jmxKey)).getCounter()).isEqualTo(((VaradaTestStats) customStatsContext.getStat(jmxKey)).getCounter());

        CustomStatsContext customStatsContext2 = new CustomStatsContext(metricsManager, Collections.emptyList());
        VaradaTestStats testStatsContext2 = new VaradaTestStats(jmxKey);
        customStatsContext2.getOrRegister(testStatsContext2);
        testStatsContext2.incCounter();
        customStatsContext2.copyStatsToGlobalMetricsManager();
        assertThat(((VaradaTestStats) metricsManager.get(jmxKey)).getCounter()).isGreaterThan(((VaradaTestStats) customStatsContext2.getStat(jmxKey)).getCounter());
    }

    static class VaradaTestStats
            extends VaradaStatsBase
    {
        final LongAdder counter = new LongAdder();

        VaradaTestStats(String jmxKey)
        {
            super(jmxKey, VaradaStatType.Worker);
        }

        public void incCounter()
        {
            this.counter.increment();
        }

        public long getCounter()
        {
            return this.counter.longValue();
        }

        @Override
        public void mergeStats(VaradaStatsBase varadaStatsBase)
        {
            if (varadaStatsBase == null) {
                return;
            }
            VaradaTestStats other = (VaradaTestStats) varadaStatsBase;
            this.counter.add(other.counter.longValue());
        }
    }
}
