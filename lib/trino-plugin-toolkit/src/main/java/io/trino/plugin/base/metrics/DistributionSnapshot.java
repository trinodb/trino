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
package io.trino.plugin.base.metrics;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects.ToStringHelper;
import io.trino.spi.metrics.Distribution;
import io.trino.spi.metrics.Metric;

import java.util.Locale;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;

@JsonTypeInfo(use = JsonTypeInfo.Id.NONE) // Do not add @class property
@JsonSerialize
public record DistributionSnapshot(long total, double min, double max, double p01, double p05, double p10, double p25, double p50, double p75, double p90, double p95, double p99)
        implements Metric<DistributionSnapshot>
{
    public static DistributionSnapshot fromDistribution(Distribution<?> distribution)
    {
        double[] percentiles = distribution.getPercentiles(1, 5, 10, 25, 50, 75, 90, 95, 99);
        return new DistributionSnapshot(
                distribution.getTotal(),
                distribution.getMin(),
                distribution.getMax(),
                percentiles[0],
                percentiles[1],
                percentiles[2],
                percentiles[3],
                percentiles[4],
                percentiles[5],
                percentiles[6],
                percentiles[7],
                percentiles[8]);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper("")
                .add("count", total)
                .add("p01", formatDouble(p01))
                .add("p05", formatDouble(p05))
                .add("p10", formatDouble(p10))
                .add("p25", formatDouble(p25))
                .add("p50", formatDouble(p50))
                .add("p75", formatDouble(p75))
                .add("p90", formatDouble(p90))
                .add("p95", formatDouble(p95))
                .add("p99", formatDouble(p99))
                .add("min", formatDouble(min))
                .add("max", formatDouble(max));
        return helper.toString();
    }

    @Override
    public DistributionSnapshot mergeWith(DistributionSnapshot other)
    {
        throw new UnsupportedOperationException("Merging of DistributionSnapshot is not supported");
    }

    private static String formatDouble(double value)
    {
        return format(Locale.US, "%.2f", value);
    }
}
