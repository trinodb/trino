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
package io.prestosql.dispatcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.execution.ExecutionFailureInfo;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class QueryAnalysisResponse
{
    public enum Status
    {
        SUCCESS, FAILED, UNKNOWN
    }

    private final Status status;
    private final Optional<ExecutionFailureInfo> failureInfo;

    public static QueryAnalysisResponse analysisPassed()
    {
        return new QueryAnalysisResponse(Status.SUCCESS, Optional.empty());
    }

    public static QueryAnalysisResponse analysisFailed(ExecutionFailureInfo failureInfo)
    {
        return new QueryAnalysisResponse(Status.FAILED, Optional.of(failureInfo));
    }

    public static QueryAnalysisResponse analysisUnknown()
    {
        return new QueryAnalysisResponse(Status.UNKNOWN, Optional.empty());
    }

    @JsonCreator
    public QueryAnalysisResponse(
            @JsonProperty("status") Status status,
            @JsonProperty("failureInfo") Optional<ExecutionFailureInfo> failureInfo)
    {
        this.status = requireNonNull(status, "status is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
    }

    @JsonProperty
    public Status getStatus()
    {
        return status;
    }

    @JsonProperty
    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("status", status)
                .add("failureInfo", failureInfo.orElse(null))
                .toString();
    }
}
