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

import io.trino.execution.resourcegroups.WeightedFairQueue.Usage;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestWeightedFairQueue
{
    @Test
    public void testBasic()
    {
        WeightedFairQueue<String> queue = new WeightedFairQueue<>();
        String item1 = "1";
        String item2 = "2";
        queue.addOrUpdate(item1, new Usage(1, 1));
        queue.addOrUpdate(item2, new Usage(2, 1));

        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.poll()).isEqualTo(item2);
        assertThat(queue.contains(item1)).isTrue();
        assertThat(queue.poll()).isEqualTo(item1);
        assertThat(queue.size()).isEqualTo(0);
        assertThat(queue.poll()).isEqualTo(null);
        assertThat(queue.poll()).isEqualTo(null);
        assertThat(queue.size()).isEqualTo(0);
    }

    @Test
    public void testUpdate()
    {
        WeightedFairQueue<String> queue = new WeightedFairQueue<>();
        String item1 = "1";
        String item2 = "2";
        String item3 = "3";
        queue.addOrUpdate(item1, new Usage(1, 1));
        queue.addOrUpdate(item2, new Usage(2, 1));
        queue.addOrUpdate(item3, new Usage(3, 1));

        assertThat(queue.poll()).isEqualTo(item3);
        queue.addOrUpdate(item1, new Usage(4, 1));
        assertThat(queue.poll()).isEqualTo(item1);
        assertThat(queue.poll()).isEqualTo(item2);
        assertThat(queue.size()).isEqualTo(0);
    }

    @Test
    public void testMultipleWinners()
    {
        WeightedFairQueue<String> queue = new WeightedFairQueue<>();
        String item1 = "1";
        String item2 = "2";
        queue.addOrUpdate(item1, new Usage(2, 0));
        queue.addOrUpdate(item2, new Usage(1, 0));

        int count1 = 0;
        int count2 = 0;
        for (int i = 0; i < 1000; i++) {
            if (queue.poll().equals(item1)) {
                queue.addOrUpdate(item1, new Usage(2, 0));
                count1++;
            }
            else {
                queue.addOrUpdate(item2, new Usage(1, 0));
                count2++;
            }
        }

        assertThat(count1).isEqualTo(500);
        assertThat(count2).isEqualTo(500);
    }
}
