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
package io.trino.execution.executor.scheduler;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestShardedSchedulingQueue
{
    @Test
    public void testRejectsInvalidShardCount()
    {
        assertThatThrownBy(() -> new ShardedSchedulingQueue<String, String>(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("shardCount must be at least 1");
    }

    @Test
    public void testGroupIsPinnedToSingleShard()
    {
        ShardedSchedulingQueue<String, String> queue = new ShardedSchedulingQueue<>(4);

        queue.startGroup("G1");
        assertThat(queue.enqueue("G1", "T1", 0)).isTrue();

        int owning = -1;
        for (int i = 0; i < queue.shardCount(); i++) {
            if (queue.shard(i).getRunnableCount() > 0) {
                assertThat(owning)
                        .describedAs("More than one shard holds the group")
                        .isEqualTo(-1);
                owning = i;
            }
        }
        assertThat(owning)
                .describedAs("Owning shard")
                .isNotEqualTo(-1);

        // every later operation for the group must route to the same shard
        assertThat(queue.enqueue("G1", "T2", 0)).isTrue();
        assertThat(queue.shard(owning).getRunnableCount()).isEqualTo(2);
        assertThat(queue.getRunnableCount()).isEqualTo(2);

        assertThat(queue.finish("G1", "T1")).isTrue();
        assertThat(queue.shard(owning).getRunnableCount()).isEqualTo(1);
        assertThat(queue.getRunnableCount()).isEqualTo(1);

        assertThat(queue.finishGroup("G1")).containsExactly("T2");
        assertThat(queue.getRunnableCount()).isEqualTo(0);
    }

    @Test
    public void testUnknownGroupIsRejectedOnEveryShard()
    {
        ShardedSchedulingQueue<String, String> queue = new ShardedSchedulingQueue<>(4);

        assertThat(queue.enqueue("missing", "T1", 0)).isFalse();
        assertThat(queue.block("missing", "T1", 0)).isFalse();
        assertThat(queue.finish("missing", "T1")).isFalse();
        assertThat(queue.unblockToRunning("missing", "T1", 0)).isFalse();
    }

    @Test
    public void testRunnableCountAggregatesAcrossShards()
    {
        ShardedSchedulingQueue<String, String> queue = new ShardedSchedulingQueue<>(4);

        for (int i = 0; i < 32; i++) {
            queue.startGroup("G" + i);
            queue.enqueue("G" + i, "T" + i, 0);
        }

        assertThat(queue.getRunnableCount()).isEqualTo(32);

        // spread across more than one shard, otherwise this test proves nothing
        int nonEmptyShards = 0;
        for (int i = 0; i < queue.shardCount(); i++) {
            if (queue.shard(i).getRunnableCount() > 0) {
                nonEmptyShards++;
            }
        }
        assertThat(nonEmptyShards).isGreaterThan(1);
    }

    @Test
    public void testFinishAllCollectsFromEveryShard()
    {
        ShardedSchedulingQueue<String, String> queue = new ShardedSchedulingQueue<>(4);

        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 32; i++) {
            queue.startGroup("G" + i);
            queue.enqueue("G" + i, "T" + i, 0);
            expected.add("T" + i);
        }

        assertThat(queue.finishAll()).isEqualTo(expected);
        assertThat(queue.getRunnableCount()).isEqualTo(0);
    }
}
