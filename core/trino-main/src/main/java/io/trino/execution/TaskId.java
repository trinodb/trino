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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.spi.QueryId;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.QueryId.parseDottedId;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public record TaskId(String fullId, StageId stageId, int partitionId, int attemptId)
{
    private static final int INSTANCE_SIZE = instanceSize(TaskId.class);

    @JsonCreator
    public static TaskId valueOf(String fullId)
    {
        List<String> parts = parseDottedId(fullId, 4, "taskId");
        return new TaskId(fullId, new StageId(new QueryId(parts.get(0)), parseInt(parts.get(1))), parseInt(parts.get(2)), parseInt(parts.get(3)));
    }

    public TaskId
    {
        requireNonNull(fullId, "fullId is null");
        requireNonNull(stageId, "stageId is null");
        checkArgument(partitionId >= 0, "partitionId is negative: %s", partitionId);
        checkArgument(attemptId >= 0, "attemptId is negative: %s", attemptId);
        checkArgument(fullId.equals(constructFullId(stageId, partitionId, attemptId)), "fullId does not match provided components");
    }

    public TaskId(StageId stageId, int partitionId, int attemptId)
    {
        this(constructFullId(stageId, partitionId, attemptId), stageId, partitionId, attemptId);
    }

    public QueryId queryId()
    {
        return stageId.queryId();
    }

    @Override
    @JsonValue
    public String toString()
    {
        return fullId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fullId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskId other = (TaskId) obj;
        return Objects.equals(this.fullId, other.fullId);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(fullId);
    }

    private static String constructFullId(StageId stageId, int partitionId, int attemptId)
    {
        return "%s.%s.%s".formatted(stageId.toString(), String.valueOf(partitionId), String.valueOf(attemptId));
    }
}
