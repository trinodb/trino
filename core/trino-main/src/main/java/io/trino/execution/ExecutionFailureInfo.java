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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.client.ErrorInfo;
import io.trino.client.ErrorLocation;
import io.trino.client.FailureInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.HostAddress;
import jakarta.annotation.Nullable;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class ExecutionFailureInfo
{
    private final String type;
    private final String message;
    private final ExecutionFailureInfo cause;
    private final List<ExecutionFailureInfo> suppressed;
    private final List<String> stack;
    private final ErrorLocation errorLocation;
    private final ErrorCode errorCode;
    // use for transport errors
    private final HostAddress remoteHost;

    @JsonCreator
    public ExecutionFailureInfo(
            @JsonProperty("type") String type,
            @JsonProperty("message") String message,
            @JsonProperty("cause") ExecutionFailureInfo cause,
            @JsonProperty("suppressed") List<ExecutionFailureInfo> suppressed,
            @JsonProperty("stack") List<String> stack,
            @JsonProperty("errorLocation") @Nullable ErrorLocation errorLocation,
            @JsonProperty("errorCode") @Nullable ErrorCode errorCode,
            @JsonProperty("remoteHost") @Nullable HostAddress remoteHost)
    {
        requireNonNull(type, "type is null");
        requireNonNull(suppressed, "suppressed is null");
        requireNonNull(stack, "stack is null");

        this.type = type;
        this.message = message;
        this.cause = cause;
        this.suppressed = ImmutableList.copyOf(suppressed);
        this.stack = ImmutableList.copyOf(stack);
        this.errorLocation = errorLocation;
        this.errorCode = errorCode;
        this.remoteHost = remoteHost;
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
    public ExecutionFailureInfo getCause()
    {
        return cause;
    }

    @JsonProperty
    public List<ExecutionFailureInfo> getSuppressed()
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
    public ErrorLocation getErrorLocation()
    {
        return errorLocation;
    }

    @Nullable
    @JsonProperty
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @Nullable
    @JsonProperty
    public HostAddress getRemoteHost()
    {
        return remoteHost;
    }

    public FailureInfo toFailureInfo()
    {
        List<FailureInfo> suppressed = this.suppressed.stream()
                .map(ExecutionFailureInfo::toFailureInfo)
                .collect(toImmutableList());

        ErrorInfo errorInfo = null;
        if (errorCode != null) {
            errorInfo = new ErrorInfo(errorCode.getCode(), errorCode.getName(), errorCode.getType().toString());
        }
        return new FailureInfo(type, message, cause == null ? null : cause.toFailureInfo(), suppressed, stack, errorInfo, errorLocation);
    }

    public RuntimeException toException()
    {
        return new Failure(this);
    }
}
