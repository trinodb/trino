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

import com.google.common.cache.CacheBuilder;
import io.airlift.units.Duration;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;

import javax.inject.Inject;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FailureInjector
{
    public static final String FAILURE_INJECTION_MESSAGE = "This error is injected by the failure injection service";

    private final NonEvictableCache<Key, InjectedFailure> failures;
    private final Duration requestTimeout;

    @Inject
    public FailureInjector(FailureInjectionConfig config)
    {
        this(
                config.getExpirationPeriod(),
                config.getRequestTimeout());
    }

    public FailureInjector(Duration expirationPeriod, Duration requestTimeout)
    {
        failures = buildNonEvictableCache(CacheBuilder.newBuilder()
                .expireAfterWrite(expirationPeriod.toMillis(), MILLISECONDS));
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
    }

    public void injectTaskFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId,
            InjectedFailureType injectionType,
            Optional<ErrorType> errorType)
    {
        failures.put(new Key(traceToken, stageId, partitionId, attemptId), new InjectedFailure(injectionType, errorType));
    }

    public Optional<InjectedFailure> getInjectedFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId)
    {
        if (failures.size() == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(failures.getIfPresent(new Key(traceToken, stageId, partitionId, attemptId)));
    }

    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    private static class Key
    {
        private final String traceToken;
        private final int stageId;
        private final int partitionId;
        private final int attemptId;

        private Key(String traceToken, int stageId, int partitionId, int attemptId)
        {
            this.traceToken = requireNonNull(traceToken, "traceToken is null");
            this.stageId = stageId;
            this.partitionId = partitionId;
            this.attemptId = attemptId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return stageId == key.stageId && partitionId == key.partitionId && attemptId == key.attemptId && Objects.equals(traceToken, key.traceToken);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(traceToken, stageId, partitionId, attemptId);
        }
    }

    public enum InjectedFailureType
    {
        TASK_MANAGEMENT_REQUEST_FAILURE,
        TASK_MANAGEMENT_REQUEST_TIMEOUT,
        TASK_GET_RESULTS_REQUEST_FAILURE,
        TASK_GET_RESULTS_REQUEST_TIMEOUT,
        TASK_FAILURE,
    }

    public static class InjectedFailure
    {
        private final InjectedFailureType injectedFailureType;
        private final Optional<ErrorType> taskFailureErrorType;

        public InjectedFailure(InjectedFailureType injectedFailureType, Optional<ErrorType> taskFailureErrorType)
        {
            this.injectedFailureType = requireNonNull(injectedFailureType, "injectedFailureType is null");
            this.taskFailureErrorType = requireNonNull(taskFailureErrorType, "taskFailureErrorType is null");
            if (injectedFailureType == TASK_FAILURE) {
                checkArgument(taskFailureErrorType.isPresent(), "error type must be present when injection type is task failure");
            }
            else {
                checkArgument(taskFailureErrorType.isEmpty(), "error type must not be present when injection type is not task failure");
            }
        }

        public InjectedFailureType getInjectedFailureType()
        {
            return injectedFailureType;
        }

        public ErrorType getTaskFailureErrorType()
        {
            return taskFailureErrorType.orElseThrow(() -> new IllegalStateException("this method must only be called for failure type of TASK_FAILURE"));
        }

        public Throwable getTaskFailureException()
        {
            ErrorType errorType = getTaskFailureErrorType();
            return new TrinoException(InjectedErrorCode.getErrorCode(errorType), FAILURE_INJECTION_MESSAGE);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("injectedFailureType", injectedFailureType)
                    .add("taskFailureErrorType", taskFailureErrorType)
                    .toString();
        }
    }

    public enum InjectedErrorCode
            implements ErrorCodeSupplier
    {
        INJECTED_USER_ERROR(1, USER_ERROR),
        INJECTED_INTERNAL_ERROR(2, INTERNAL_ERROR),
        INJECTED_INSUFFICIENT_RESOURCES_ERROR(3, INSUFFICIENT_RESOURCES),
        INJECTED_EXTERNAL_ERROR(4, EXTERNAL),
        /**/;

        private final ErrorCode errorCode;

        InjectedErrorCode(int code, ErrorType type)
        {
            errorCode = new ErrorCode(code + 0x30000, name(), type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }

        public static InjectedErrorCode getErrorCode(ErrorType errorType)
        {
            for (InjectedErrorCode code : values()) {
                if (code.toErrorCode().getType() == errorType) {
                    return code;
                }
            }
            throw new IllegalArgumentException("unexpected error type: " + errorType);
        }
    }
}
