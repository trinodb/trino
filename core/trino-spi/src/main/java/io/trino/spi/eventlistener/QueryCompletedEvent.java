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
import io.trino.spi.TrinoWarning;
import io.trino.spi.Unstable;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class QueryCompletedEvent
{
    private final QueryMetadata metadata;
    private final QueryStatistics statistics;
    private final QueryContext context;
    private final QueryIOMetadata ioMetadata;
    private final Optional<QueryFailureInfo> failureInfo;
    private final List<TrinoWarning> warnings;

    private final Instant createTime;
    private final Instant executionStartTime;
    private final Instant endTime;

    @JsonCreator
    @Unstable
    public QueryCompletedEvent(
            QueryMetadata metadata,
            QueryStatistics statistics,
            QueryContext context,
            QueryIOMetadata ioMetadata,
            Optional<QueryFailureInfo> failureInfo,
            List<TrinoWarning> warnings,
            Instant createTime,
            Instant executionStartTime,
            Instant endTime)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.context = requireNonNull(context, "context is null");
        this.ioMetadata = requireNonNull(ioMetadata, "ioMetadata is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.warnings = requireNonNull(warnings, "warnings is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.executionStartTime = requireNonNull(executionStartTime, "executionStartTime is null");
        this.endTime = requireNonNull(endTime, "endTime is null");
    }

    @JsonProperty
    public QueryMetadata getMetadata()
    {
        return metadata;
    }

    @JsonProperty
    public QueryStatistics getStatistics()
    {
        return statistics;
    }

    @JsonProperty
    public QueryContext getContext()
    {
        return context;
    }

    @JsonProperty
    public QueryIOMetadata getIoMetadata()
    {
        return ioMetadata;
    }

    @JsonProperty
    public Optional<QueryFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    @JsonProperty
    public List<TrinoWarning> getWarnings()
    {
        return warnings;
    }

    @JsonProperty
    public Instant getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public Instant getExecutionStartTime()
    {
        return executionStartTime;
    }

    @JsonProperty
    public Instant getEndTime()
    {
        return endTime;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", QueryCompletedEvent.class.getSimpleName() + "[", "]")
                .add("queryId='" + getMetadata().getQueryId() + "'")
                .add("queryState='" + getMetadata().getQueryState() + "'")
                .toString();
    }
}
