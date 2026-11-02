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
package io.trino.execution.scheduler.faulttolerant;

import io.trino.spi.ErrorCode;

import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.EXCHANGE_DATA_UNRECOVERABLE;

public final class ExchangeFailureClassifier
{
    public enum ExchangeFailureKind
    {
        TRANSIENT,
        EXCHANGE_DATA_UNRECOVERABLE,
        FATAL,
    }

    private ExchangeFailureClassifier() {}

    public static ExchangeFailureKind classify(ErrorCode errorCode)
    {
        if (errorCode.getType() == USER_ERROR || errorCode.isFatal()) {
            return ExchangeFailureKind.FATAL;
        }

        if (errorCode.getCode() == EXCHANGE_DATA_UNRECOVERABLE.toErrorCode().getCode()) {
            return ExchangeFailureKind.EXCHANGE_DATA_UNRECOVERABLE;
        }

        return ExchangeFailureKind.TRANSIENT;
    }
}
