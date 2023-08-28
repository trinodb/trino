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
package io.trino.execution.scheduler;

public enum TaskExecutionClass
{
    // Tasks from stages with all upstream stages finished
    STANDARD,

    // Tasks from stages with some upstream stages still running.
    // To be scheduled only if no STANDARD tasks can fit.
    // Picked to kill if worker runs out of memory to prevent deadlock.
    SPECULATIVE,
    /**/;

    boolean canTransitionTo(TaskExecutionClass targetExecutionClass)
    {
        return switch (this) {
            case STANDARD -> targetExecutionClass == STANDARD;
            case SPECULATIVE -> targetExecutionClass == SPECULATIVE || targetExecutionClass == STANDARD;
        };
    }
}
