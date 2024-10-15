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

import io.airlift.units.Duration;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;
import static java.util.Objects.requireNonNull;

public interface FailureInjector
{
    void injectTaskFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId,
            FailureInjector.InjectedFailureType injectionType,
            Optional<ErrorType> errorType);

    Optional<InjectedFailure> getInjectedFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId);

    Duration getRequestTimeout();

    enum InjectedFailureType
    {
        TASK_MANAGEMENT_REQUEST_FAILURE,
        TASK_MANAGEMENT_REQUEST_TIMEOUT,
        TASK_GET_RESULTS_REQUEST_FAILURE,
        TASK_GET_RESULTS_REQUEST_TIMEOUT,
        TASK_FAILURE,
    }

    String FAILURE_INJECTION_MESSAGE = "This error is injected by the failure injection service";

    class InjectedFailure
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

    enum InjectedErrorCode
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
