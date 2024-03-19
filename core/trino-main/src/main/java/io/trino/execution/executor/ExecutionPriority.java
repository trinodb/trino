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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Execution priority based on a normal to a low-priority tasks resource usage ratio.
 * For example, 1 means both use the same amount of resources.
 * 100 means normal tasks should use 100 times more resources than low-priority tasks.
 */
public record ExecutionPriority(double normalPriorityResourceUsageMultiplier)
{
    public static final ExecutionPriority NORMAL = new ExecutionPriority(1);

    public ExecutionPriority
    {
        checkArgument(normalPriorityResourceUsageMultiplier >= 1, "only priority lower than normal is supported but got " + normalPriorityResourceUsageMultiplier);
    }

    /**
     * Create {@link ExecutionPriority} from a given target resource (CPU) usage fraction.
     * Tasks should use at most {@code resourcePercentage} of CPU,
     * if the cluster is close to 100% CPU utilization and there are higher priority tasks running.
     */
    public static ExecutionPriority fromResourcePercentage(double resourcePercentage)
    {
        checkArgument(resourcePercentage > 0 && resourcePercentage < 1, "resourcePercentage must be > 0 and < 1");
        double normalToLowPriorityResourceRatio = (1 / resourcePercentage) - 1;
        return new ExecutionPriority(normalToLowPriorityResourceRatio);
    }

    public long toTaskWeight(long elapsed)
    {
        // elapsed is nanoseconds and in extreme case the multiplication can produce
        // a `double` value bigger than Long.MAX_VALUE but the cast to long
        // will truncate it to the Long.MAX_VALUE
        return (long) (elapsed * normalPriorityResourceUsageMultiplier);
    }
}
