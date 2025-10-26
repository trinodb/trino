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

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.stats.TDigest;
import io.airlift.units.Duration;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static io.trino.spi.metrics.Metrics.accumulator;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMetrics
{
    @Test
    public void testMergeCount()
    {
        Metrics m1 = new Metrics(ImmutableMap.of(
                "a", new LongCount(1),
                "b", new LongCount(2)));
        Metrics m2 = new Metrics(ImmutableMap.of(
                "b", new LongCount(3),
                "c", new LongCount(4)));
        Metrics merged = merge(m1, m2);
        Map<String, Metric<?>> expectedMap = ImmutableMap.of(
                "a", new LongCount(1),
                "b", new LongCount(5),
                "c", new LongCount(4));
        assertThat(merged.getMetrics()).isEqualTo(expectedMap);
    }

    @Test
    public void testMergeHistogram()
    {
        Metrics m1 = new Metrics(ImmutableMap.of("a", TDigestHistogram.fromValue(10.0, 1)));
        Metrics m2 = new Metrics(ImmutableMap.of("a", TDigestHistogram.fromValue(5.0, 2)));
        TDigestHistogram merged = (TDigestHistogram) merge(m1, m2).getMetrics().get("a");

        assertThat(merged.getTotal()).isEqualTo(3L);
        double[] mergedPercentiles = merged.getPercentiles(0, 100);
        assertThat(mergedPercentiles[0]).isEqualTo(5.0);
        assertThat(mergedPercentiles[1]).isEqualTo(10.0);
        assertThat(merged.toString())
                .matches("\\{count=3, p01=5\\.00, p05=5\\.00, p10=5\\.00, p25=5\\.00, p50=7\\.50, p75=10\\.00, p90=10\\.00, p95=10\\.00, p99=10\\.00, min=5\\.00, max=10\\.00\\}");
    }

    @Test
    public void testDurationTiming()
    {
        DurationTiming d1 = new DurationTiming(new Duration(1234, NANOSECONDS));
        DurationTiming d2 = new DurationTiming(new Duration(1, NANOSECONDS));

        assertThat(d1.toString()).matches("\\{duration=1.23us\\}");

        Metrics m1 = new Metrics(ImmutableMap.of("a", d1));
        Metrics m2 = new Metrics(ImmutableMap.of("a", d2));

        DurationTiming merged = (DurationTiming) merge(m1, m2).getMetrics().get("a");

        assertThat(merged.getAirliftDuration().roundTo(NANOSECONDS)).isEqualTo(1235);
        assertThat(merged.getDuration().toNanos()).isEqualTo(1235);
        assertThat(merged.toString()).matches("\\{duration=1.24us\\}");
    }

    @Test
    public void testHistogramJson()
    {
        JsonCodec<TDigestHistogram> codec = JsonCodec.jsonCodec(TDigestHistogram.class);

        TDigest digest = new TDigest();
        digest.add(123);

        String json = codec.toJson(new TDigestHistogram(digest));
        TDigestHistogram result = codec.fromJson(json);
        assertThat(result.getDigest().getCount()).isEqualTo(digest.getCount());
    }

    @Test
    public void testDurationJson()
    {
        JsonCodec<DurationTiming> codec = JsonCodec.jsonCodec(DurationTiming.class);
        DurationTiming duration = new DurationTiming(new Duration(123, NANOSECONDS));
        String json = codec.toJson(duration);
        DurationTiming result = codec.fromJson(json);
        assertThat(result.getAirliftDuration()).isEqualTo(duration.getAirliftDuration());
    }

    @Test
    public void testFailIncompatibleTypes()
    {
        assertThatThrownBy(() -> {
            Metrics m1 = new Metrics(ImmutableMap.of("a", new TDigestHistogram(new TDigest())));
            Metrics m2 = new Metrics(ImmutableMap.of("a", new LongCount(0)));
            merge(m1, m2);
        })
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    public void testReduceSingleMetrics()
    {
        Metrics metrics = new Metrics(ImmutableMap.of("a", new LongCount(0)));
        assertThat(accumulator().add(metrics).get()).isEqualTo(metrics);

        Metrics metrics1 = new Metrics(ImmutableMap.of("a", new LongCount(1)));
        Metrics metrics2 = new Metrics(ImmutableMap.of("a", new LongCount(2)));
        assertThat(accumulator().add(metrics1).add(metrics2).get()).isEqualTo(new Metrics(ImmutableMap.of("a", new LongCount(3))));
    }

    private static Metrics merge(Metrics... metrics)
    {
        return Arrays.stream(metrics).reduce(Metrics.EMPTY, Metrics::mergeWith);
    }
}
