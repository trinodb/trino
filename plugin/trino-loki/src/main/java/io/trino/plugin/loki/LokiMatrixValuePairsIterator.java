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
package io.trino.plugin.loki;

import io.github.jeschkies.loki.client.model.Matrix;
import io.github.jeschkies.loki.client.model.MetricPoint;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;

public class LokiMatrixValuePairsIterator
        implements LokiQueryResultIterator
{
    private final Iterator<LokiMatrixValuePairsIterator.Point> metrics;

    record Point(MetricPoint timeAndValue, Map<String, String> labels) {}

    private Point current;

    public LokiMatrixValuePairsIterator(Matrix matrix)
    {
        this.metrics = matrix.getMetrics().stream()
                .flatMap(metric -> metric.values().stream()
                        .map(value -> new LokiMatrixValuePairsIterator.Point(value, metric.labels()))).iterator();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!metrics.hasNext()) {
            return false;
        }
        current = metrics.next();
        return true;
    }

    @Override
    public Map<String, String> getLabels()
    {
        return current.labels;
    }

    @Override
    public long getTimestamp()
    {
        return toTimeWithTimeZone(current.timeAndValue.getTs());
    }

    @Override
    public Object getValue()
    {
        return current.timeAndValue.getValue();
    }

    /**
     * Metric value pairs have a timestamp in seconds.
     *
     * @param seconds since epoch.
     * @return time in Trino's packed format.
     */
    private static long toTimeWithTimeZone(Long seconds)
    {
        return packDateTimeWithZone(Instant.ofEpochSecond(seconds).toEpochMilli(), UTC_KEY);
    }
}
