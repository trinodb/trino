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

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.QueryId.parseDottedId;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public record TaskId(StageId stageId, int partitionId, int attemptId)
{
    private static final int INSTANCE_SIZE = instanceSize(TaskId.class);

    @JsonCreator
    public static TaskId valueOf(String fullId)
    {
        List<String> parts = parseDottedId(fullId, 4, "taskId");
        return new TaskId(new StageId(new QueryId(parts.get(0)), parseInt(parts.get(1))), parseInt(parts.get(2)), parseInt(parts.get(3)));
    }

    public TaskId
    {
        requireNonNull(stageId, "stageId is null");
        checkArgument(partitionId >= 0, "partitionId is negative: %s", partitionId);
        checkArgument(attemptId >= 0, "attemptId is negative: %s", attemptId);
    }

    public QueryId queryId()
    {
        return stageId.queryId();
    }

    @Override
    @JsonValue
    public String toString()
    {
        return stageId.queryId().id() + '.' + stageId.id() + '.' + partitionId + '.' + attemptId;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + stageId.getRetainedSizeInBytes();
    }
}
