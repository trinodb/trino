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
package io.trino.execution.resourcegroups;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThan;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStochasticPriorityQueue
{
    @Test
    public void testContainsAndRemove()
    {
        StochasticPriorityQueue<String> queue = new StochasticPriorityQueue<>();
        for (int i = 0; i < 100; i++) {
            assertThat(queue.addOrUpdate("test" + i, i + 1)).isTrue();
        }
        for (int i = 0; i < 100; i += 2) {
            assertThat(queue.remove("test" + i)).isTrue();
        }
        for (int i = 1; i < 100; i += 2) {
            assertThat(queue.contains("test" + i)).isTrue();
        }
        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.size()).isEqualTo(50);
        for (int i = 1; i < 100; i += 2) {
            assertThat(queue.remove("test" + i)).isTrue();
        }
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);
    }

    @Test
    public void testPollDistribution()
    {
        StochasticPriorityQueue<String> queue = new StochasticPriorityQueue<>();
        for (int i = 0; i < 100; i++) {
            assertThat(queue.addOrUpdate("foo" + i, 1)).isTrue();
        }
        for (int i = 0; i < 100; i++) {
            assertThat(queue.addOrUpdate("bar" + i, 1)).isTrue();
        }
        int foo = 0;
        for (int i = 0; i < 1000; i++) {
            String value = queue.poll();
            if (value.startsWith("foo")) {
                foo++;
            }
            assertThat(queue.addOrUpdate(value, 1)).isTrue();
        }
        BinomialDistribution binomial = new BinomialDistribution(1000, 0.5);
        int lowerBound = binomial.inverseCumulativeProbability(0.000001);
        int upperBound = binomial.inverseCumulativeProbability(0.999999);
        assertLessThan(foo, upperBound);
        assertGreaterThan(foo, lowerBound);

        // Update foo weights to 2:1 distribution
        for (int i = 0; i < 100; i++) {
            assertThat(queue.addOrUpdate("foo" + i, 2)).isFalse();
        }
        foo = 0;
        for (int i = 0; i < 1000; i++) {
            String value = queue.poll();
            if (value.startsWith("foo")) {
                foo++;
                assertThat(queue.addOrUpdate(value, 2)).isTrue();
            }
            else {
                assertThat(queue.addOrUpdate(value, 1)).isTrue();
            }
        }
        binomial = new BinomialDistribution(1000, 2.0 / 3.0);
        lowerBound = binomial.inverseCumulativeProbability(0.000001);
        upperBound = binomial.inverseCumulativeProbability(0.999999);
        assertLessThan(foo, upperBound);
        assertGreaterThan(foo, lowerBound);
    }
}
