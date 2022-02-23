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
package io.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.execution.TaskId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TaskMemoryInfo
{
    private final TaskId taskId;
    private final long memoryReservation;

    @JsonCreator
    public TaskMemoryInfo(
            @JsonProperty("taskId") TaskId taskId,
            @JsonProperty("memoryReservation") long memoryReservation)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.memoryReservation = memoryReservation;
    }

    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public long getMemoryReservation()
    {
        return memoryReservation;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("memoryReservation", memoryReservation)
                .toString();
    }
}
