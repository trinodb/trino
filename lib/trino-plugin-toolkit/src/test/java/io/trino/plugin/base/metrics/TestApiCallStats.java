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

import com.google.common.base.Ticker;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestApiCallStats
{
    @Test
    void testNoCallsRecorded()
    {
        ApiCallStats stats = new ApiCallStats(new ManualTicker());
        assertThat(stats.toMetrics("metastore")).isSameAs(Metrics.EMPTY);
    }

    @Test
    void testSuccessfulCall()
    {
        ManualTicker ticker = new ManualTicker();
        ApiCallStats stats = new ApiCallStats(ticker);

        String result = stats.measure("getTable", () -> {
            ticker.advance(7);
            return "table";
        });
        assertThat(result).isEqualTo("table");

        Map<String, Metric<?>> metrics = stats.toMetrics("metastore").getMetrics();
        assertThat(metrics.keySet()).containsExactlyInAnyOrder(
                "metastore.all.time.total",
                "metastore.all.time.distribution",
                "metastore.getTable.time.total",
                "metastore.getTable.time.distribution");
        assertThat(((LongCount) metrics.get("metastore.all.time.total")).getTotal()).isEqualTo(7);
        assertThat(((DistributionSnapshot) metrics.get("metastore.all.time.distribution")).total()).isEqualTo(1);
        assertThat(((LongCount) metrics.get("metastore.getTable.time.total")).getTotal()).isEqualTo(7);
        assertThat(((DistributionSnapshot) metrics.get("metastore.getTable.time.distribution")).total()).isEqualTo(1);
    }

    @Test
    void testFailedCall()
    {
        ManualTicker ticker = new ManualTicker();
        ApiCallStats stats = new ApiCallStats(ticker);

        assertThatThrownBy(() -> stats.measure("getTable", () -> {
            ticker.advance(3);
            throw new IllegalStateException("boom");
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");

        Map<String, Metric<?>> metrics = stats.toMetrics("metastore").getMetrics();
        assertThat(((LongCount) metrics.get("metastore.all.time.total")).getTotal()).isEqualTo(3);
        assertThat(((LongCount) metrics.get("metastore.all.failures")).getTotal()).isEqualTo(1);
        assertThat(((LongCount) metrics.get("metastore.getTable.time.total")).getTotal()).isEqualTo(3);
        assertThat(((LongCount) metrics.get("metastore.getTable.failures")).getTotal()).isEqualTo(1);
    }

    @Test
    void testExpectedExceptionIsNotFailure()
    {
        ManualTicker ticker = new ManualTicker();
        ApiCallStats stats = new ApiCallStats(ticker);

        assertThatThrownBy(() -> stats.measure(
                "loadView",
                () -> {
                    ticker.advance(5);
                    throw new NotFoundException();
                },
                NotFoundException.class))
                .isInstanceOf(NotFoundException.class);

        Map<String, Metric<?>> metrics = stats.toMetrics("metastore").getMetrics();
        assertThat(((LongCount) metrics.get("metastore.loadView.time.total")).getTotal()).isEqualTo(5);
        assertThat(((DistributionSnapshot) metrics.get("metastore.loadView.time.distribution")).total()).isEqualTo(1);
        assertThat(metrics).doesNotContainKeys("metastore.all.failures", "metastore.loadView.failures");
    }

    private static class NotFoundException
            extends RuntimeException {}

    private static class ManualTicker
            extends Ticker
    {
        private long nanos;

        void advance(long nanos)
        {
            this.nanos += nanos;
        }

        @Override
        public long read()
        {
            return nanos;
        }
    }
}
