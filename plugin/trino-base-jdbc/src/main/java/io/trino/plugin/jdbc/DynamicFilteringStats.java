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
package io.trino.plugin.jdbc;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.spi.connector.DynamicFilter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DynamicFilteringStats
{
    private final CounterStat totalDynamicFilters = new CounterStat();
    private final CounterStat completedDynamicFilters = new CounterStat();
    private final CounterStat awaitableDynamicFilters = new CounterStat();
    private final TimeStat dynamicFilterWaitingTime = new TimeStat(MILLISECONDS);

    @Managed
    @Nested
    public CounterStat getTotalDynamicFilters()
    {
        return totalDynamicFilters;
    }

    @Managed
    @Nested
    public CounterStat getCompletedDynamicFilters()
    {
        return completedDynamicFilters;
    }

    @Managed
    @Nested
    public CounterStat getAwaitableDynamicFilters()
    {
        return awaitableDynamicFilters;
    }

    @Managed
    @Nested
    public TimeStat getDynamicFilterWaitingTime()
    {
        return dynamicFilterWaitingTime;
    }

    public void processDynamicFilter(DynamicFilter dynamicFilter, Duration waitingTime)
    {
        totalDynamicFilters.update(1);
        if (dynamicFilter.isComplete()) {
            completedDynamicFilters.update(1);
        }
        if (dynamicFilter.isAwaitable()) {
            awaitableDynamicFilters.update(1);
        }
        dynamicFilterWaitingTime.add(waitingTime);
    }
}
