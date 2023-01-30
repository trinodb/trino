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
package io.trino.split;

import io.trino.execution.TaskId;
import io.trino.spi.connector.ConnectorPageSinkId;

import static com.google.common.base.Preconditions.checkArgument;

public class PageSinkId
        implements ConnectorPageSinkId
{
    private final long id;

    public static PageSinkId fromTaskId(TaskId taskId)
    {
        long stageId = taskId.getStageId().getId();
        long partitionId = taskId.getPartitionId();
        checkArgument(partitionId == (partitionId & 0x00FFFFFF), "partitionId is out of allowable range");
        long attemptId = taskId.getAttemptId();
        checkArgument(attemptId == (attemptId & 0xFF), "attemptId is out of allowable range");
        long id = (stageId << 32) + (partitionId << 8) + attemptId;
        return new PageSinkId(id);
    }

    private PageSinkId(long id)
    {
        this.id = id;
    }

    @Override
    public long getId()
    {
        return this.id;
    }
}
