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
package io.prestosql.spi.eventlistener;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TracerEvent
{
    private final String nodeId;
    private final String uri;
    private final String queryId;
    private final Optional<String> stageId;
    private final Optional<String> taskId;
    private final Optional<String> pipelineId;
    private final Optional<String> splitId;
    private final Optional<String> operatorId;

    private final String threadName;

    //time of the source node
    private final Instant time;

    private final String eventType;

    private final Optional<String> payload;

    public TracerEvent(
            String nodeId,
            String uri,
            String queryId,
            Optional<String> stageId,
            Optional<String> taskId,
            Optional<String> pipelineId,
            Optional<String> splitId,
            Optional<String> operatorId,
            String threadName,
            Instant time,
            String eventType,
            Optional<String> payload)
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.stageId = stageId;
        this.taskId = taskId;
        this.pipelineId = pipelineId;
        this.splitId = splitId;
        this.operatorId = operatorId;
        this.threadName = requireNonNull(threadName, "threadName is null");
        this.time = requireNonNull(time, "time is null");
        this.eventType = requireNonNull(eventType, "actionType is null");
        this.payload = payload;
    }

    public String getNodeId()
    {
        return nodeId;
    }

    public String getUri()
    {
        return uri;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public Optional<String> getStageId()
    {
        return stageId;
    }

    public Optional<String> getTaskId()
    {
        return taskId;
    }

    public Optional<String> getPipelineId()
    {
        return pipelineId;
    }

    public Optional<String> getSplitId()
    {
        return splitId;
    }

    public Optional<String> getOperatorId()
    {
        return operatorId;
    }

    public String getThreadName()
    {
        return threadName;
    }

    public Instant getTime()
    {
        return time;
    }

    public String getEventType()
    {
        return eventType;
    }

    public Optional<String> getPayload()
    {
        return payload;
    }

    @Override
    public String toString()
    {
        return
                "nodeId: " + nodeId +
                ", uri: " + uri +
                ", queryId: " + queryId +
                ", stageId: " + stageId.toString() +
                ", taskId: " + taskId +
                ", pipelineId: " + pipelineId +
                ", splitId: " + splitId +
                ", operatorId: " + operatorId +
                ", threadName: " + threadName +
                ", timeStamp: " + time +
                ", eventType: " + eventType +
                ", payload: " + payload;
    }
}
