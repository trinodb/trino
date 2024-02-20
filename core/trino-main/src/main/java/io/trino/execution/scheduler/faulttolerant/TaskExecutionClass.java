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
package io.trino.execution.scheduler.faulttolerant;

public enum TaskExecutionClass
{
    // Tasks from stages with all upstream stages finished
    STANDARD,

    // Tasks from stages with some upstream stages still running.
    // To be scheduled only if no STANDARD tasks can fit.
    // Picked to kill if worker runs out of memory to prevent deadlock.
    SPECULATIVE,

    // Tasks from stages with some upstream stages still running but with high priority.
    // Will be scheduled even if there are resources to schedule STANDARD tasks on cluster.
    // Tasks of EAGER_SPECULATIVE are used to implement early termination of queries, when it
    // is probable that we do not need to run whole downstream stages to produce final query result.
    // EAGER_SPECULATIVE will not prevent STANDARD tasks from being scheduled and will still be picked
    // to kill if needed when worker runs out of memory; this is needed to prevent deadlocks.
    EAGER_SPECULATIVE,
    /**/;

    boolean canTransitionTo(TaskExecutionClass targetExecutionClass)
    {
        return switch (this) {
            case STANDARD -> targetExecutionClass == STANDARD;
            case SPECULATIVE -> targetExecutionClass == SPECULATIVE || targetExecutionClass == STANDARD;
            case EAGER_SPECULATIVE -> targetExecutionClass == EAGER_SPECULATIVE || targetExecutionClass == STANDARD;
        };
    }

    boolean isSpeculative()
    {
        return switch (this) {
            case STANDARD -> false;
            case SPECULATIVE, EAGER_SPECULATIVE -> true;
        };
    }
}
