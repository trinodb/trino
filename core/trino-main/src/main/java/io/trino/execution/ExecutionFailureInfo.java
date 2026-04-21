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

import com.google.common.collect.ImmutableList;
import io.trino.client.ErrorInfo;
import io.trino.client.ErrorLocation;
import io.trino.client.FailureInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.HostAddress;
import jakarta.annotation.Nullable;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * @param remoteHost use for transport errors
 */
public record ExecutionFailureInfo(
        String type,
        String message,
        ExecutionFailureInfo cause,
        List<ExecutionFailureInfo> suppressed,
        List<String> stack,
        @Nullable ErrorLocation errorLocation,
        @Nullable ErrorCode errorCode,
        // use for transport errors
        @Nullable HostAddress remoteHost)
{
    public ExecutionFailureInfo
    {
        requireNonNull(type, "type is null");
        requireNonNull(suppressed, "suppressed is null");
        requireNonNull(stack, "stack is null");
        suppressed = ImmutableList.copyOf(suppressed);
        stack = ImmutableList.copyOf(stack);
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
