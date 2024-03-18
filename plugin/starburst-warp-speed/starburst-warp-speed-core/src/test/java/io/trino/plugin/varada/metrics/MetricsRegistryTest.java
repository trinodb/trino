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
import org.junit.jupiter.api.Test;
import org.weakref.jmx.MBeanExporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MetricsRegistryTest
{
    @Test
    public void testMBeanExporterEnabled()
    {
        MBeanExporter mBeanExporter = mock(MBeanExporter.class);
        MetricsRegistry metricsRegistry = new MetricsRegistry(
                new CatalogNameProvider("catalog-name"),
                mBeanExporter,
                new MetricsConfiguration());

        VaradaTestStats stats = new VaradaTestStats("11111");
        String key = stats.getJmxKey() + ".catalog-name";

        metricsRegistry.registerMetric(stats);
        verify(mBeanExporter, times(1))
                .exportWithGeneratedName(stats, stats.getClass(), key);

        VaradaTestStats statResult = (VaradaTestStats) metricsRegistry.get(stats.getJmxKey());
        assertThat(statResult).isEqualTo(stats);

        assertThat(metricsRegistry.getRegisteredInstances()).containsExactly(stats);

        metricsRegistry.unregisterMetric(stats.getJmxKey());
        verify(mBeanExporter, times(1))
                .unexportWithGeneratedName(stats.getClass(), key);

        statResult = (VaradaTestStats) metricsRegistry.get(stats.getJmxKey());
        assertThat(statResult).isNull();
    }

    @Test
    public void testMBeanExporterDisabled()
    {
        MBeanExporter mBeanExporter = mock(MBeanExporter.class);
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration();
        metricsConfiguration.setEnabled(false);

        MetricsRegistry metricsRegistry = new MetricsRegistry(
                new CatalogNameProvider("catalog-name"),
                mBeanExporter,
                metricsConfiguration);

        VaradaTestStats stats = new VaradaTestStats("11111");
        String key = stats.getJmxKey() + ".catalog-name";

        metricsRegistry.registerMetric(stats);
        verify(mBeanExporter, never())
                .exportWithGeneratedName(stats, stats.getClass(), key);

        VaradaTestStats statsResult = (VaradaTestStats) metricsRegistry.get(stats.getJmxKey());
        assertThat(statsResult).isEqualTo(stats);

        assertThat(metricsRegistry.getRegisteredInstances()).containsExactly(stats);

        metricsRegistry.unregisterMetric(stats.getJmxKey());
        verify(mBeanExporter, never())
                .unexportWithGeneratedName(stats.getClass(), key);

        statsResult = (VaradaTestStats) metricsRegistry.get(stats.getJmxKey());
        assertThat(statsResult).isNull();
    }

    static class VaradaTestStats
            extends VaradaStatsBase
    {
        VaradaTestStats(String jmxKey)
        {
            super(jmxKey, VaradaStatType.Worker);
        }
    }
}
