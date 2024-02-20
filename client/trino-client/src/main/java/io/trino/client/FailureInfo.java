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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import jakarta.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class FailureInfo
{
    private final String type;
    private final String message;
    private final FailureInfo cause;
    private final List<FailureInfo> suppressed;
    private final List<String> stack;
    private final ErrorInfo errorInfo;
    private final ErrorLocation errorLocation;

    @JsonCreator
    public FailureInfo(
            @JsonProperty("type") String type,
            @JsonProperty("message") String message,
            @JsonProperty("cause") FailureInfo cause,
            @JsonProperty("suppressed") List<FailureInfo> suppressed,
            @JsonProperty("stack") List<String> stack,
            @JsonProperty("errorInfo") @Nullable ErrorInfo errorInfo,
            @JsonProperty("errorLocation") @Nullable ErrorLocation errorLocation)
    {
        requireNonNull(type, "type is null");
        requireNonNull(suppressed, "suppressed is null");
        requireNonNull(stack, "stack is null");

        this.type = type;
        this.message = message;
        this.cause = cause;
        this.suppressed = ImmutableList.copyOf(suppressed);
        this.stack = ImmutableList.copyOf(stack);
        this.errorInfo = errorInfo;
        this.errorLocation = errorLocation;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @Nullable
    @JsonProperty
    public String getMessage()
    {
        return message;
    }

    @Nullable
    @JsonProperty
    public FailureInfo getCause()
    {
        return cause;
    }

    @JsonProperty
    public List<FailureInfo> getSuppressed()
    {
        return suppressed;
    }

    @JsonProperty
    public List<String> getStack()
    {
        return stack;
    }

    @Nullable
    @JsonProperty
    public ErrorInfo getErrorInfo()
    {
        return errorInfo;
    }

    @Nullable
    @JsonProperty
    public ErrorLocation getErrorLocation()
    {
        return errorLocation;
    }

    public RuntimeException toException()
    {
        return new FailureException(this);
    }
}
