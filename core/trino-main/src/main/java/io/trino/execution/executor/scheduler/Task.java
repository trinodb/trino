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

import io.trino.annotation.NotThreadSafe;
import io.trino.execution.executor.ExecutionPriority;

import static java.util.Objects.requireNonNull;

@NotThreadSafe
final class Task
{
    private final ExecutionPriority priority;
    private State state;
    private long weight;
    private long uncommittedWeight;

    public Task(ExecutionPriority priority, long initialWeight)
    {
        this.priority = requireNonNull(priority, "priority is null");
        weight = initialWeight;
    }

    public void setState(State state)
    {
        this.state = state;
    }

    public void commitWeight(long delta)
    {
        weight += delta;
        uncommittedWeight = 0;
    }

    public void addWeight(long delta)
    {
        weight += delta;
    }

    public long weight()
    {
        return priority.toTaskWeight(weight + uncommittedWeight);
    }

    public void setUncommittedWeight(long weight)
    {
        this.uncommittedWeight = weight;
    }

    public long uncommittedWeight()
    {
        return uncommittedWeight;
    }

    public State state()
    {
        return state;
    }
}
