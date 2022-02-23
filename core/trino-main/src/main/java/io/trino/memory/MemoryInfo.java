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
package io.trino.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.TaskMemoryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MemoryInfo
{
    private final int availableProcessors;
    private final MemoryPoolInfo pool;
    private final ListMultimap<QueryId, TaskMemoryInfo> tasksMemoryInfo;

    public MemoryInfo(
            int availableProcessors,
            MemoryPoolInfo pool)
    {
        this(availableProcessors, pool, ImmutableListMultimap.of());
    }

    @JsonCreator
    public MemoryInfo(
            @JsonProperty("availableProcessors") int availableProcessors,
            @JsonProperty("pool") MemoryPoolInfo pool,
            @JsonProperty("tasksMemoryInfo") ListMultimap<QueryId, TaskMemoryInfo> tasksMemoryInfo)
    {
        this.availableProcessors = availableProcessors;
        this.pool = requireNonNull(pool, "pool is null");
        this.tasksMemoryInfo = ImmutableListMultimap.copyOf(requireNonNull(tasksMemoryInfo, "tasksMemoryInfo is null"));
    }

    @JsonProperty
    public int getAvailableProcessors()
    {
        return availableProcessors;
    }

    @JsonProperty
    public MemoryPoolInfo getPool()
    {
        return pool;
    }

    @JsonProperty
    public ListMultimap<QueryId, TaskMemoryInfo> getTasksMemoryInfo()
    {
        return tasksMemoryInfo;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("availableProcessors", availableProcessors)
                .add("pool", pool)
                .add("tasksMemoryInfo", tasksMemoryInfo)
                .toString();
    }

    public MemoryInfo withTasksMemoryInfo(ListMultimap<QueryId, TaskMemoryInfo> tasksMemoryInfo)
    {
        return new MemoryInfo(availableProcessors, pool, tasksMemoryInfo);
    }
}
