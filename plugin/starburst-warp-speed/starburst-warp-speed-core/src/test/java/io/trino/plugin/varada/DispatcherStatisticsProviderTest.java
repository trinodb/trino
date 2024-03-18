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
package io.trino.plugin.varada;

import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherStatisticsProvider;
import io.trino.spi.statistics.Estimate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DispatcherStatisticsProviderTest
{
    @Test
    public void cardinalityIllegalConfigurationShouldThrow()
    {
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setCardinalityBuckets("");
        assertThatThrownBy(() -> new DispatcherStatisticsProvider(globalConfiguration));
        globalConfiguration.setCardinalityBuckets("1.5");
        assertThatThrownBy(() -> new DispatcherStatisticsProvider(globalConfiguration));
        globalConfiguration.setCardinalityBuckets("abc");
        assertThatThrownBy(() -> new DispatcherStatisticsProvider(globalConfiguration));
        globalConfiguration.setCardinalityBuckets("1,10,5");
        assertThatThrownBy(() -> new DispatcherStatisticsProvider(globalConfiguration));
    }

    @Test
    public void cardinalitySimpleGet()
    {
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setCardinalityBuckets("1,100,300");
        DispatcherStatisticsProvider dispatcherStatisticsProvider = new DispatcherStatisticsProvider(globalConfiguration);
        assertThat(dispatcherStatisticsProvider.getColumnCardinalityBucket(Estimate.of(20))).isEqualTo(1);
        assertThat(dispatcherStatisticsProvider.getColumnCardinalityBucket(Estimate.of(110))).isEqualTo(2);
        assertThat(dispatcherStatisticsProvider.getColumnCardinalityBucket(Estimate.of(400))).isEqualTo(3);
    }

    @Test
    public void cardinalityUnknownShouldReturnZero()
    {
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setCardinalityBuckets("1,100,300");
        DispatcherStatisticsProvider dispatcherStatisticsProvider = new DispatcherStatisticsProvider(globalConfiguration);
        assertThat(dispatcherStatisticsProvider.getColumnCardinalityBucket(Estimate.unknown())).isEqualTo(0);
    }
}
