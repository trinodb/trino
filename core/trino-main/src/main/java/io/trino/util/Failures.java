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
package io.trino.util;

import com.google.common.collect.ImmutableList;
import io.trino.client.ErrorLocation;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.Failure;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.HostAddress;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoTransportException;
import io.trino.sql.parser.ParsingException;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Failures
{
    private static final String NODE_CRASHED_ERROR = "The node may have crashed or be under too much load. " +
            "This is probably a transient issue, so please retry your query in a few minutes.";

    public static final String WORKER_NODE_ERROR = "Encountered too many errors talking to a worker node. " + NODE_CRASHED_ERROR;

    public static final String REMOTE_TASK_MISMATCH_ERROR = "Could not communicate with the remote task. " + NODE_CRASHED_ERROR;

    private Failures() {}

    public static ExecutionFailureInfo toFailure(Throwable failure)
    {
        return toFailure(failure, newIdentityHashSet());
    }

    public static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new TrinoException(errorCode, format(formatString, args));
        }
    }

    public static List<ExecutionFailureInfo> toFailures(Collection<? extends Throwable> failures)
    {
        return failures.stream()
                .map(Failures::toFailure)
                .collect(toImmutableList());
    }

    private static ExecutionFailureInfo toFailure(Throwable throwable, Set<Throwable> seenFailures)
    {
        if (throwable == null) {
            return null;
        }

        String type;
        HostAddress remoteHost = null;
        if (throwable instanceof Failure) {
            type = ((Failure) throwable).getType();
        }
        else {
            Class<?> clazz = throwable.getClass();
            type = firstNonNull(clazz.getCanonicalName(), clazz.getName());
        }
        if (throwable instanceof TrinoTransportException) {
            remoteHost = ((TrinoTransportException) throwable).getRemoteHost();
        }

        if (seenFailures.contains(throwable)) {
            return new ExecutionFailureInfo(
                    type,
                    "[cyclic] " + throwable.getMessage(),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    null,
                    GENERIC_INTERNAL_ERROR.toErrorCode(),
                    remoteHost);
        }
        seenFailures.add(throwable);

        ExecutionFailureInfo cause = toFailure(throwable.getCause(), seenFailures);
        ErrorCode errorCode = toErrorCode(throwable);
        if (errorCode == null) {
            if (cause == null) {
                errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
            }
            else {
                errorCode = cause.getErrorCode();
            }
        }

        return new ExecutionFailureInfo(
                type,
                throwable.getMessage(),
                cause,
                Arrays.stream(throwable.getSuppressed())
                        .map(failure -> toFailure(failure, seenFailures))
                        .collect(toImmutableList()),
                Arrays.stream(throwable.getStackTrace())
                        .map(Objects::toString)
                        .collect(toImmutableList()),
                getErrorLocation(throwable),
                errorCode,
                remoteHost);
    }

    @Nullable
    private static ErrorLocation getErrorLocation(Throwable throwable)
    {
        // TODO: this is a big hack
        if (throwable instanceof ParsingException parsingException) {
            return new ErrorLocation(parsingException.getLineNumber(), parsingException.getColumnNumber());
        }
        if (throwable instanceof TrinoException trinoException) {
            return trinoException.getLocation()
                    .map(location -> new ErrorLocation(location.getLineNumber(), location.getColumnNumber()))
                    .orElse(null);
        }
        return null;
    }

    @Nullable
    private static ErrorCode toErrorCode(Throwable throwable)
    {
        requireNonNull(throwable);

        if (throwable instanceof TrinoException trinoException) {
            return trinoException.getErrorCode();
        }
        if (throwable instanceof Failure failure && failure.getErrorCode() != null) {
            return failure.getErrorCode();
        }
        if (throwable instanceof ParsingException) {
            return SYNTAX_ERROR.toErrorCode();
        }
        return null;
    }

    public static TrinoException internalError(Throwable t)
    {
        throwIfInstanceOf(t, Error.class);
        throwIfInstanceOf(t, TrinoException.class);
        return new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, t);
    }
}
