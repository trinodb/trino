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
package io.trino.plugin.hive.parquet;

import io.trino.filesystem.InputFileMetrics;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.util.Map;

public final class MetricsUtils
{
    private MetricsUtils() {}

    public static Metrics convert(InputFileMetrics inputMetrics)
    {
        Map<String, Metric<?>> metrics = Map.of(
                "bytes_read_from_cache", new LongCount(inputMetrics.getBytesReadFromCache()),
                "bytes_read_externally", new LongCount(inputMetrics.getBytesReadExternally()));
        return new Metrics(metrics);
    }
}
