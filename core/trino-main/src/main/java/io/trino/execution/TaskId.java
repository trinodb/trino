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
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

public class TaskId
{
    private static final int INSTANCE_SIZE = instanceSize(TaskId.class);

    @JsonCreator
    public static TaskId valueOf(String taskId)
    {
        return new TaskId(taskId);
    }

    private final String fullId;
    private final StageId stageId;
    private final int partitionId;
    private final int attemptId;

    public TaskId(StageId stageId, int partitionId, int attemptId)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        checkArgument(partitionId >= 0, "partitionId is negative: %s", partitionId);
        checkArgument(attemptId >= 0, "attemptId is negative: %s", attemptId);
        this.partitionId = partitionId;
        this.attemptId = attemptId;

        // There is a strange JDK bug related to the CompactStrings implementation in JDK20+ which causes some fullId values
        // to get corrupted when this particular line is JIT-optimized. Changing implicit concatenation to a String.join call
        // seems to mitigate this issue. See: https://github.com/trinodb/trino/issues/18272 for more details.
        this.fullId = join(".", stageId.toString(), String.valueOf(partitionId), String.valueOf(attemptId));
    }

    private TaskId(String fullId)
    {
        this.fullId = requireNonNull(fullId, "fullId is null");
        List<String> parts = parseDottedId(fullId, 4, "taskId");
        this.stageId = new StageId(new QueryId(parts.get(0)), parseInt(parts.get(1)));
        this.partitionId = parseInt(parts.get(2));
        this.attemptId = parseInt(parts.get(3));
        checkArgument(partitionId >= 0, "partitionId is negative: %s", partitionId);
        checkArgument(attemptId >= 0, "attemptId is negative: %s", attemptId);
    }

    public QueryId getQueryId()
    {
        return stageId.queryId();
    }

    public StageId getStageId()
    {
        return stageId;
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    public int getAttemptId()
    {
        return attemptId;
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
}
