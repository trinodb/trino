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
import io.trino.spi.StandardErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.execution.scheduler.faulttolerant.ExchangeFailureClassifier.ExchangeFailureKind.EXCHANGE_DATA_UNRECOVERABLE;
import static io.trino.execution.scheduler.faulttolerant.ExchangeFailureClassifier.ExchangeFailureKind.FATAL;
import static io.trino.execution.scheduler.faulttolerant.ExchangeFailureClassifier.ExchangeFailureKind.TRANSIENT;
import static io.trino.execution.scheduler.faulttolerant.ExchangeFailureClassifier.classify;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExchangeFailureClassifier
{
    @Test
    public void testUserErrorIsFatal()
    {
        assertThat(classify(GENERIC_USER_ERROR.toErrorCode())).isEqualTo(FATAL);
    }

    @Test
    public void testFatalInternalErrorIsFatal()
    {
        ErrorCode fatalInternal = new ErrorCode(99999, "FATAL_INTERNAL", INTERNAL_ERROR, true);
        assertThat(classify(fatalInternal)).isEqualTo(FATAL);
    }

    @Test
    public void testExchangeDataUnrecoverableCodeIsUnrecoverable()
    {
        assertThat(classify(StandardErrorCode.EXCHANGE_DATA_UNRECOVERABLE.toErrorCode())).isEqualTo(EXCHANGE_DATA_UNRECOVERABLE);
    }

    @Test
    public void testOtherNonFatalErrorsAreTransient()
    {
        assertThat(classify(REMOTE_HOST_GONE.toErrorCode())).isEqualTo(TRANSIENT);
        assertThat(classify(GENERIC_INTERNAL_ERROR.toErrorCode())).isEqualTo(TRANSIENT);
        ErrorCode externalError = new ErrorCode(200001, "SOME_PLUGIN_ERROR", EXTERNAL, false);
        assertThat(classify(externalError)).isEqualTo(TRANSIENT);
    }
}
