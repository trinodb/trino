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
package io.trino.execution.buffer;

import com.google.common.base.Ticker;
import io.airlift.stats.TDigest;
import io.trino.plugin.base.util.Lazy;
import org.junit.jupiter.api.Test;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static org.assertj.core.api.Assertions.assertThat;

final class TestOutputBufferMemoryManager
{
    private static final long MAX_BUFFERED_BYTES = 1024;

    @Test
    void testUtilizationHistogramWithStalledTicker()
    {
        // a ticker that never advances yields zero-duration samples, which must not reach the TDigest
        OutputBufferMemoryManager memoryManager = createMemoryManager(new StalledTicker());

        memoryManager.updateMemoryUsage(512);
        memoryManager.updateMemoryUsage(-512);
        memoryManager.close();

        assertThat(memoryManager.getUtilizationHistogram().getCount()).isEqualTo(0);
    }

    @Test
    void testUtilizationHistogramWithAdvancingTicker()
    {
        OutputBufferMemoryManager memoryManager = createMemoryManager(new AdvancingTicker());

        memoryManager.updateMemoryUsage(MAX_BUFFERED_BYTES);
        memoryManager.updateMemoryUsage(-MAX_BUFFERED_BYTES);

        // one sample per recording: the initial empty buffer, the full buffer, and the empty buffer again
        TDigest histogram = memoryManager.getUtilizationHistogram();
        assertThat(histogram.getCount()).isEqualTo(3);
        assertThat(histogram.getMin()).isEqualTo(0.0);
        assertThat(histogram.getMax()).isEqualTo(1.0);
    }

    private static OutputBufferMemoryManager createMemoryManager(Ticker ticker)
    {
        return new OutputBufferMemoryManager(
                MAX_BUFFERED_BYTES,
                Lazy.from(() -> newSimpleAggregatedMemoryContext().newLocalMemoryContext(TestOutputBufferMemoryManager.class.getSimpleName())),
                directExecutor(),
                ticker);
    }

    private static final class StalledTicker
            extends Ticker
    {
        @Override
        public long read()
        {
            return 42;
        }
    }

    private static final class AdvancingTicker
            extends Ticker
    {
        private long value;

        @Override
        public long read()
        {
            return value++;
        }
    }
}
