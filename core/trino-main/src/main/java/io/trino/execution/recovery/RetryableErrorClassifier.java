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
package io.trino.execution.recovery;

import io.trino.spi.ErrorCode;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;

public final class RetryableErrorClassifier
{
    private RetryableErrorClassifier() {}

    public static boolean isRetryable(ErrorCode errorCode)
    {
        if (errorCode == null) {
            return true;
        }

        if (errorCode.isFatal()) {
            return false;
        }
        return errorCode.getType() == INTERNAL_ERROR
                || errorCode.getType() == EXTERNAL
                || errorCode.getCode() == CLUSTER_OUT_OF_MEMORY.toErrorCode().getCode();
    }
}
