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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.floorMod;

/**
 * <p>Partitions a {@link BlockingSchedulingQueue} into independent shards so that scheduling
 * events do not all serialize on a single lock. A group is pinned to a shard for its lifetime
 * by its hash, so all operations for a given group hit the same shard and see a consistent
 * view.</p>
 *
 * <p><b>Fairness:</b> fair-share ordering is exact <em>within</em> a shard only. Across shards,
 * groups compete for capacity through the shared concurrency semaphore, which hands out slots
 * per shard rather than per group. A shard that happens to hold more groups therefore gives
 * each of its groups a proportionally smaller share. With G groups spread over K shards the
 * worst-case skew is bounded by the most-loaded shard's group count relative to the mean, which
 * is only close to 1 when G is much larger than K. Sharding should be enabled only when queue
 * lock contention has been shown to cost more than that skew; a single shard reproduces the
 * previous behavior exactly.</p>
 */
@ThreadSafe
final class ShardedSchedulingQueue<G, T>
{
    private final List<BlockingSchedulingQueue<G, T>> shards;

    public ShardedSchedulingQueue(int shardCount)
    {
        checkArgument(shardCount > 0, "shardCount must be at least 1");

        ImmutableList.Builder<BlockingSchedulingQueue<G, T>> builder = ImmutableList.builderWithExpectedSize(shardCount);
        for (int i = 0; i < shardCount; i++) {
            builder.add(new BlockingSchedulingQueue<>());
        }
        this.shards = builder.build();
    }

    public int shardCount()
    {
        return shards.size();
    }

    /**
     * The shard a scheduler thread with the given index is responsible for draining.
     */
    public BlockingSchedulingQueue<G, T> shard(int index)
    {
        return shards.get(index);
    }

    private BlockingSchedulingQueue<G, T> shardFor(G group)
    {
        return shards.get(floorMod(group.hashCode(), shards.size()));
    }

    public void startGroup(G group)
    {
        shardFor(group).startGroup(group);
    }

    public Set<T> finishGroup(G group)
    {
        return shardFor(group).finishGroup(group);
    }

    public Set<T> getTasks(G group)
    {
        return shardFor(group).getTasks(group);
    }

    public Set<T> finishAll()
    {
        ImmutableSet.Builder<T> tasks = ImmutableSet.builder();
        for (BlockingSchedulingQueue<G, T> shard : shards) {
            tasks.addAll(shard.finishAll());
        }
        return tasks.build();
    }

    public boolean enqueue(G group, T task, long deltaWeight)
    {
        return shardFor(group).enqueue(group, task, deltaWeight);
    }

    public boolean block(G group, T task, long deltaWeight)
    {
        return shardFor(group).block(group, task, deltaWeight);
    }

    public boolean unblockToRunning(G group, T task, long expectedWeight)
    {
        return shardFor(group).unblockToRunning(group, task, expectedWeight);
    }

    public boolean finish(G group, T task)
    {
        return shardFor(group).finish(group, task);
    }

    /**
     * Approximate total number of runnable tasks across all shards. Safe to read without
     * holding any lock, but the value may be stale by the time the caller acts on it.
     */
    public int getRunnableCount()
    {
        int count = 0;
        for (BlockingSchedulingQueue<G, T> shard : shards) {
            count += shard.getRunnableCount();
        }
        return count;
    }

    @Override
    public String toString()
    {
        if (shards.size() == 1) {
            return shards.getFirst().toString();
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < shards.size(); i++) {
            builder.append("Shard %s:\n".formatted(i));
            builder.append(shards.get(i).toString().indent(4));
        }
        return builder.toString();
    }
}
