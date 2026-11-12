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
package io.trino.execution.executor;

import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLeafSplitMemoryEstimate
{
    @Test
    public void testSamplesLargestDriver()
    {
        LeafSplitMemoryEstimate estimate = new LeafSplitMemoryEstimate(new TestingTaskMemory(100, 40, 0, 0, 0));
        assertThat(estimate.sample(0)).isEqualTo(40);
    }

    @Test
    public void testSamplesOncePerInterval()
    {
        LeafDriverMemory memory = new LeafDriverMemory(100);
        LeafSplitMemoryEstimate estimate = new LeafSplitMemoryEstimate(memory);
        assertThat(estimate.sample(0)).isEqualTo(100);
        memory.setLeafDriverMaxBytes(200);
        assertThat(estimate.sample(MILLISECONDS.toNanos(99))).isEqualTo(100);
        assertThat(estimate.sample(MILLISECONDS.toNanos(100))).isEqualTo(200);
    }

    @Test
    public void testKeepsPeakSample()
    {
        LeafDriverMemory memory = new LeafDriverMemory(200);
        LeafSplitMemoryEstimate estimate = new LeafSplitMemoryEstimate(memory);
        assertThat(estimate.sample(0)).isEqualTo(200);
        memory.setLeafDriverMaxBytes(50);
        assertThat(estimate.sample(MILLISECONDS.toNanos(100))).isEqualTo(200);
    }

    // Reports a settable largest leaf driver memory and zero for every other figure.
    private static class LeafDriverMemory
            implements TaskMemory
    {
        private volatile long leafDriverMaxBytes;

        LeafDriverMemory(long leafDriverMaxBytes)
        {
            this.leafDriverMaxBytes = leafDriverMaxBytes;
        }

        void setLeafDriverMaxBytes(long leafDriverMaxBytes)
        {
            this.leafDriverMaxBytes = leafDriverMaxBytes;
        }

        @Override
        public long getLeafPipelineBytes()
        {
            return 0;
        }

        @Override
        public long getLeafDriverMaxBytes()
        {
            return leafDriverMaxBytes;
        }

        @Override
        public long getQueryFreeBytes()
        {
            return 0;
        }

        @Override
        public long getQueryMaxBytes()
        {
            return 0;
        }

        @Override
        public long getPoolFreeBytes()
        {
            return 0;
        }
    }
}
