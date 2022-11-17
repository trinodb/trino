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
import io.trino.spi.ErrorCode;
import io.trino.spi.Unstable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class QueryFailureInfo
{
    private final ErrorCode errorCode;
    private final Optional<String> failureType;
    private final Optional<String> failureMessage;
    private final Optional<String> failureTask;
    private final Optional<String> failureHost;
    private final String failuresJson;

    @JsonCreator
    @Unstable
    public QueryFailureInfo(
            ErrorCode errorCode,
            Optional<String> failureType,
            Optional<String> failureMessage,
            Optional<String> failureTask,
            Optional<String> failureHost,
            String failuresJson)
    {
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.failureType = requireNonNull(failureType, "failureType is null");
        this.failureMessage = requireNonNull(failureMessage, "failureMessage is null");
        this.failureTask = requireNonNull(failureTask, "failureTask is null");
        this.failureHost = requireNonNull(failureHost, "failureHost is null");
        this.failuresJson = requireNonNull(failuresJson, "failuresJson is null");
    }

    @JsonProperty
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @JsonProperty
    public Optional<String> getFailureType()
    {
        return failureType;
    }

    @JsonProperty
    public Optional<String> getFailureMessage()
    {
        return failureMessage;
    }

    @JsonProperty
    public Optional<String> getFailureTask()
    {
        return failureTask;
    }

    @JsonProperty
    public Optional<String> getFailureHost()
    {
        return failureHost;
    }

    @JsonProperty
    public String getFailuresJson()
    {
        return failuresJson;
    }
}
