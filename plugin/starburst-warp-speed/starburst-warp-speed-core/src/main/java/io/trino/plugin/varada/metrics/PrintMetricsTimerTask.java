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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.MetricsConfiguration;
import io.varada.log.ShapingLogger;
import io.varada.tools.CatalogNameProvider;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrintMetricsTimerTask
        extends MetricsTimerTask
{
    static final String STATS = "stats";
    private final ShapingLogger shapingLogger;
    private static final Logger dumpLogger = Logger.get("METRICS-DUMP");
    private static final Logger logger = Logger.get(PrintMetricsTimerTask.class);
    private static final String TIMESTAMP = "timestamp";
    private static final String CATALOG = "catalog";
    private final MetricsManager metricsManager;
    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();
    private final CatalogNameProvider catalogNameProvider;

    @Inject
    public PrintMetricsTimerTask(MetricsConfiguration metricsConfiguration,
            MetricsManager metricsManager,
            CatalogNameProvider catalogNameProvider)
    {
        super(metricsConfiguration);
        this.metricsManager = requireNonNull(metricsManager);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        this.shapingLogger = ShapingLogger.getInstance(
                dumpLogger,
                Integer.MAX_VALUE,
                Duration.ofMinutes(1), // at maximum, we want to print once a minute
                1);

        logger.debug("PrintMetricsTimerTask constructor");
    }

    @Override
    public Duration getInterval()
    {
        return metricsConfiguration.getPrintMetricsDuration();
    }

    @Override
    public void run()
    {
        print(true);
    }

    public void print(boolean isScheduledPrint)
    {
        if (logger.isInfoEnabled()) {
            Map<String, Object> fullJson = buildJsonDump();
            if (!fullJson.isEmpty()) {
                try {
                    if (isScheduledPrint) {
                        dumpLogger.info(objectMapper.writeValueAsString(fullJson));
                    }
                    else {
                        shapingLogger.info(objectMapper.writeValueAsString(fullJson));
                    }
                }
                catch (JsonProcessingException e) {
                    logger.debug("failed writing metrics map %s", fullJson);
                }
            }
        }
    }

    Map<String, Object> buildJsonDump()
    {
        Map<String, Object> fullJson = new HashMap<>();
        Map<String, Object> metricsDump = getMetricsDump();
        if (!metricsDump.isEmpty()) {
            fullJson.put(TIMESTAMP, System.currentTimeMillis());
            fullJson.put(STATS, metricsDump);
            fullJson.put(CATALOG, catalogNameProvider.get());
        }
        return fullJson;
    }

    Map<String, Object> getMetricsDump()
    {
        Map<String, Object> res = new HashMap<>();
        metricsManager.getAll().forEach((statsGroup, stats) -> {
            Map<Object, Object> statsMap = stats.printStatsMap();
            if (!statsMap.isEmpty()) {
                res.put(statsGroup, statsMap);
            }
        });
        return res;
    }
}
