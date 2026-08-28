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
import org.junit.jupiter.api.Test;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static org.assertj.core.api.Assertions.assertThat;

class TestRetryableErrorClassifier
{
    @Test
    void testMissingErrorCodeIsRetryable()
    {
        assertThat(RetryableErrorClassifier.isRetryable(null)).isTrue();
    }

    @Test
    void testRetryableErrorTypes()
    {
        assertThat(RetryableErrorClassifier.isRetryable(new ErrorCode(1, "TEST_INTERNAL", INTERNAL_ERROR))).isTrue();
        assertThat(RetryableErrorClassifier.isRetryable(new ErrorCode(2, "TEST_EXTERNAL", EXTERNAL))).isTrue();
    }

    @Test
    void testClusterOutOfMemoryIsRetryable()
    {
        assertThat(RetryableErrorClassifier.isRetryable(CLUSTER_OUT_OF_MEMORY.toErrorCode())).isTrue();
    }

    @Test
    void testNonRetryableErrorTypes()
    {
        assertThat(RetryableErrorClassifier.isRetryable(new ErrorCode(3, "TEST_USER", USER_ERROR))).isFalse();
        assertThat(RetryableErrorClassifier.isRetryable(new ErrorCode(4, "TEST_RESOURCES", INSUFFICIENT_RESOURCES))).isFalse();
    }

    @Test
    void testFatalErrorIsNotRetryable()
    {
        assertThat(RetryableErrorClassifier.isRetryable(new ErrorCode(5, "TEST_FATAL_INTERNAL", INTERNAL_ERROR, true))).isFalse();
    }
}
