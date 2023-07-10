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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import java.time.Instant;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class SplitCompletedEvent
{
    private final String queryId;
    private final String stageId;
    private final String taskId;
    private final Optional<String> catalogName;

    private final Instant createTime;
    private final Optional<Instant> startTime;
    private final Optional<Instant> endTime;

    private final SplitStatistics statistics;
    private final Optional<SplitFailureInfo> failureInfo;

    private final String payload;

    @JsonCreator
    @Unstable
    public SplitCompletedEvent(
            String queryId,
            String stageId,
            String taskId,
            Optional<String> catalogName,
            Instant createTime,
            Optional<Instant> startTime,
            Optional<Instant> endTime,
            SplitStatistics statistics,
            Optional<SplitFailureInfo> failureInfo,
            String payload)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.startTime = requireNonNull(startTime, "startTime is null");
        this.endTime = requireNonNull(endTime, "endTime is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.payload = requireNonNull(payload, "payload is null");
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public String getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public Instant getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public Optional<Instant> getStartTime()
    {
        return startTime;
    }

    @JsonProperty
    public Optional<Instant> getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public SplitStatistics getStatistics()
    {
        return statistics;
    }

    @JsonProperty
    public Optional<SplitFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    @JsonProperty
    public String getPayload()
    {
        return payload;
    }

    @JsonProperty
    public Optional<String> getCatalogName()
    {
        return catalogName;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", SplitCompletedEvent.class.getSimpleName() + "[", "]")
                .add("queryId='" + queryId + "'")
                .add("stageId='" + stageId + "'")
                .add("taskId='" + taskId + "'")
                .toString();
    }
}
