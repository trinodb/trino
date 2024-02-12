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
package io.trino.testing.assertions;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.cache.SafeCaches;
import io.trino.client.ErrorInfo;
import io.trino.client.FailureException;
import io.trino.client.FailureInfo;
import io.trino.execution.Failure;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.ParsingException;
import io.trino.testing.QueryFailedException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.util.CheckReturnValue;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public final class TrinoExceptionAssert
        extends AbstractThrowableAssert<TrinoExceptionAssert, Throwable>
{
    private static final LoadingCache<String, Boolean> isTrinoExceptionCache = SafeCaches.buildNonEvictableCache(
            CacheBuilder.newBuilder().maximumSize(100),
            CacheLoader.from(type -> {
                try {
                    Class<?> exceptionClass = Class.forName(type);
                    return TrinoException.class.isAssignableFrom(exceptionClass) ||
                            ParsingException.class.isAssignableFrom(exceptionClass);
                }
                catch (ClassNotFoundException e) {
                    return false;
                }
            }));

    private final FailureInfo failureInfo;

    @CheckReturnValue
    public static TrinoExceptionAssert assertTrinoExceptionThrownBy(ThrowingCallable throwingCallable)
    {
        Throwable throwable = catchThrowable(throwingCallable);
        if (throwable == null) {
            failBecauseExceptionWasNotThrown(TrinoException.class);
        }
        return assertThatTrinoException(throwable);
    }

    @CheckReturnValue
    public static TrinoExceptionAssert assertThatTrinoException(Throwable throwable)
    {
        Optional<FailureInfo> failureInfo = getFailureInfo(throwable);
        if (failureInfo.isEmpty() || !isTrinoException(failureInfo.get().getType())) {
            throw new AssertionError("Expected TrinoException or wrapper, but got: " + throwable.getClass().getName() + " " + throwable, throwable);
        }
        return new TrinoExceptionAssert(throwable, failureInfo.get());
    }

    private static boolean isTrinoException(String type)
    {
        return isTrinoExceptionCache.getUnchecked(type);
    }

    private static Optional<FailureInfo> getFailureInfo(Throwable throwable)
    {
        return switch (throwable) {
            case TrinoException trinoException -> Optional.of(toFailure(trinoException).toFailureInfo());

            case QueryFailedException queryFailedException -> {
                if (queryFailedException.getCause() == null) {
                    yield Optional.empty();
                }
                Optional<FailureInfo> failureInfo = switch (queryFailedException.getCause()) {
                    case Failure failure -> Optional.of(failure.getFailureInfo().toFailureInfo());
                    case FailureException failure -> Optional.of(failure.getFailureInfo());
                    default -> Optional.empty();
                };
                yield failureInfo;
            }

            default -> Optional.empty();
        };
    }

    private TrinoExceptionAssert(Throwable actual, FailureInfo failureInfo)
    {
        super(actual, TrinoExceptionAssert.class);
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
    }

    @CanIgnoreReturnValue
    public TrinoExceptionAssert hasErrorCode(ErrorCodeSupplier... errorCodeSupplier)
    {
        ErrorCode errorCode = null;
        ErrorInfo errorInfo = failureInfo.getErrorInfo();
        if (errorInfo != null) {
            errorCode = new ErrorCode(errorInfo.getCode(), errorInfo.getName(), ErrorType.valueOf(errorInfo.getType()));
        }

        try {
            assertThat(errorCode).isIn(
                    Stream.of(errorCodeSupplier)
                            .map(ErrorCodeSupplier::toErrorCode)
                            .collect(toSet()));
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }

    @CanIgnoreReturnValue
    public TrinoExceptionAssert hasLocation(int lineNumber, int columnNumber)
    {
        try {
            Optional<Location> location = Optional.ofNullable(failureInfo.getErrorLocation())
                    .map(errorLocation -> new Location(errorLocation.getLineNumber(), errorLocation.getColumnNumber()));
            assertThat(location).hasValue(new Location(lineNumber, columnNumber));
        }
        catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
        return myself;
    }
}
