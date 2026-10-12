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
    public void testNoSampleWithoutTaskMemory()
    {
        assertThat(new LeafSplitMemoryEstimate().sample(0)).isEqualTo(0);
    }

    @Test
    public void testSamplesLargestDriver()
    {
        LeafSplitMemoryEstimate estimate = new LeafSplitMemoryEstimate();
        estimate.setTaskMemory(new TestingTaskMemory(100, 40, 0, 0, 0));
        assertThat(estimate.sample(0)).isEqualTo(40);
    }

    @Test
    public void testSamplesOncePerInterval()
    {
        LeafSplitMemoryEstimate estimate = new LeafSplitMemoryEstimate();
        estimate.setTaskMemory(memory(100));
        assertThat(estimate.sample(0)).isEqualTo(100);
        estimate.setTaskMemory(memory(200));
        assertThat(estimate.sample(MILLISECONDS.toNanos(99))).isEqualTo(100);
        assertThat(estimate.sample(MILLISECONDS.toNanos(100))).isEqualTo(200);
    }

    @Test
    public void testKeepsPeakSample()
    {
        LeafSplitMemoryEstimate estimate = new LeafSplitMemoryEstimate();
        estimate.setTaskMemory(memory(200));
        assertThat(estimate.sample(0)).isEqualTo(200);
        estimate.setTaskMemory(memory(50));
        assertThat(estimate.sample(MILLISECONDS.toNanos(100))).isEqualTo(200);
    }

    private static TaskMemory memory(long leafDriverMaxBytes)
    {
        return new TestingTaskMemory(0, leafDriverMaxBytes, 0, 0, 0);
    }
}
