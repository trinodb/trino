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
package io.trino.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.execution.TaskId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class DirectExchangeInput
        implements ExchangeInput
{
    private static final int INSTANCE_SIZE = instanceSize(DirectExchangeInput.class);

    private final TaskId taskId;
    private final String location;

    @JsonCreator
    public DirectExchangeInput(
            @JsonProperty("taskId") TaskId taskId,
            @JsonProperty("location") String location)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.location = requireNonNull(location, "location is null");
    }

    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("location", location)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + taskId.getRetainedSizeInBytes()
                + estimatedSizeOf(location);
    }
}
