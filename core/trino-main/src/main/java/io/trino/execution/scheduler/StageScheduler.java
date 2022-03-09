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

import java.io.Closeable;

public interface StageScheduler
        extends Closeable
{
    /**
     * Called by the query scheduler when the scheduling process begins.
     * This method is called before the ExecutionSchedule takes a decision
     * to schedule a stage but after the query scheduling has been fully initialized.
     * Within this method the scheduler may decide to schedule tasks that
     * are necessary for query execution to make progress.
     * For example the scheduler may decide to schedule a task without
     * assigning any splits to unblock dynamic filter collection.
     */
    default void start() {}

    /**
     * Schedules as much work as possible without blocking.
     * The schedule results is a hint to the query scheduler if and
     * when the stage scheduler should be invoked again.  It is
     * important to note that this is only a hint and the query
     * scheduler may call the schedule method at any time.
     */
    ScheduleResult schedule();

    @Override
    default void close() {}
}
