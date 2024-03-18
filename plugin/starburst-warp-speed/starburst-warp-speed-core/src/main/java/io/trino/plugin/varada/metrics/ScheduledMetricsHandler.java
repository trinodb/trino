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
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.util.VaradaInitializedServiceMarker;

import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

@Singleton
public class ScheduledMetricsHandler
        implements VaradaInitializedServiceMarker
{
    private static final Logger logger = Logger.get(ScheduledMetricsHandler.class);

    private final Set<MetricsTimerTask> metricsTimerTasks;
    private final Timer timer;

    @Inject
    public ScheduledMetricsHandler(
            Set<MetricsTimerTask> metricsTimerTasks,
            VaradaInitializedServiceRegistry varadaInitializedServiceRegistry)
    {
        this.metricsTimerTasks = requireNonNull(metricsTimerTasks);
        varadaInitializedServiceRegistry.addService(this);
        this.timer = new Timer();
    }

    @Override
    public void init()
    {
        logger.debug("init metricsTimerTasks");
        metricsTimerTasks.forEach(metricsTimerTask -> {
            long delayMillis = metricsTimerTask.getDelay().getSeconds() > 0
                    ? TimeUnit.SECONDS.toMillis(metricsTimerTask.getDelay().getSeconds())
                    : TimeUnit.NANOSECONDS.toMillis(metricsTimerTask.getDelay().toNanosPart());

            long intervalInSeconds = metricsTimerTask.getInterval().getSeconds();
            int intervalInNanos = metricsTimerTask.getInterval().toNanosPart();
            long intervalMillis = intervalInSeconds > 0 ? TimeUnit.SECONDS.toMillis(intervalInSeconds) : TimeUnit.NANOSECONDS.toMillis(intervalInNanos);

            timer.scheduleAtFixedRate(metricsTimerTask, delayMillis, intervalMillis);
        });
    }
}
