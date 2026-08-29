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

import io.airlift.units.Duration;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.assertj.core.api.Assertions.assertThat;

class TestRetryingRecoveryStrategy
{
    private static final ErrorCode RETRYABLE_ERROR = new ErrorCode(1, "TEST_INTERNAL", INTERNAL_ERROR);
    private static final ErrorCode NON_RETRYABLE_ERROR = new ErrorCode(2, "TEST_USER", USER_ERROR);
    private static final BooleanSupplier NOT_DONE = () -> false;

    @Test
    void testRerunsUntilRetriesExhausted()
    {
        TestingRecoveryAction recorder = new TestingRecoveryAction();
        int maxRetries = 2;
        RetryingRecoveryStrategy strategy = new RetryingRecoveryStrategy(recorder, NOT_DONE, backoff(maxRetries, "1s", "10s", 2.0));
        List<Throwable> queryFailures = new ArrayList<>();

        assertThat(strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add)).isTrue();
        assertThat(strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add)).isTrue();
        assertThat(recorder.reruns()).hasSize(maxRetries);
        assertThat(queryFailures).isEmpty();

        assertThat(strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add)).isFalse();
        assertThat(recorder.reruns()).hasSize(maxRetries);
        assertThat(queryFailures).isEmpty();
    }

    @Test
    void testZeroRetriesPassesFailureOn()
    {
        TestingRecoveryAction recorder = new TestingRecoveryAction();
        RetryingRecoveryStrategy strategy = new RetryingRecoveryStrategy(recorder, NOT_DONE, backoff(0, "1s", "10s", 2.0));
        List<Throwable> queryFailures = new ArrayList<>();

        assertThat(strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add)).isFalse();
        assertThat(recorder.reruns()).isEmpty();
        assertThat(queryFailures).isEmpty();
    }

    @Test
    void testNonRetryableErrorFailsQuery()
    {
        TestingRecoveryAction rerunner = new TestingRecoveryAction();
        RetryingRecoveryStrategy strategy = new RetryingRecoveryStrategy(rerunner, NOT_DONE, backoff(1, "1s", "10s", 2.0));
        List<Throwable> queryFailures = new ArrayList<>();

        TrinoException failure = failure();
        strategy.handleFailure(failure, NON_RETRYABLE_ERROR, queryFailures::add);
        assertThat(rerunner.reruns()).isEmpty();
        assertThat(queryFailures).containsExactly(failure);
    }

    @Test
    void testExponentialBackoffCappedAtMaxDelay()
    {
        TestingRecoveryAction recorder = new TestingRecoveryAction();
        RetryingRecoveryStrategy strategy = new RetryingRecoveryStrategy(recorder, NOT_DONE, backoff(10, "1s", "4s", 2.0));
        List<Throwable> queryFailures = new ArrayList<>();

        strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add);
        strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add);
        strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add);
        strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add);

        assertThat(recorder.reruns().stream().map(Duration::toMillis))
                .containsExactly(1000L, 2000L, 4000L, 4000L);
        assertThat(queryFailures).isEmpty();
    }

    @Test
    void testQueryDoneShortCircuitsToFail()
    {
        TestingRecoveryAction recorder = new TestingRecoveryAction();
        RetryingRecoveryStrategy strategy = new RetryingRecoveryStrategy(recorder, () -> true, backoff(3, "1s", "10s", 2.0));
        List<Throwable> queryFailures = new ArrayList<>();

        TrinoException failure = failure();
        strategy.handleFailure(failure, RETRYABLE_ERROR, queryFailures::add);

        assertThat(recorder.reruns()).isEmpty();
        assertThat(queryFailures).containsExactly(failure);
    }

    @Test
    void testSingleImmediatePolicy()
    {
        TestingRecoveryAction recorder = new TestingRecoveryAction();
        RetryingRecoveryStrategy strategy = new RetryingRecoveryStrategy(recorder, NOT_DONE, BackoffPolicy.RETRY_ONCE_IMMEDIATE);
        List<Throwable> queryFailures = new ArrayList<>();

        assertThat(strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add)).isTrue();
        assertThat(recorder.reruns()).hasSize(1);
        assertThat(recorder.reruns().get(0).toMillis()).isZero();
        assertThat(queryFailures).isEmpty();

        assertThat(strategy.handleFailure(failure(), RETRYABLE_ERROR, queryFailures::add)).isFalse();
        assertThat(recorder.reruns()).hasSize(1);
        assertThat(queryFailures).isEmpty();
    }

    private static TrinoException failure()
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, "worker crashed");
    }

    private static BackoffPolicy backoff(int maxRetries, String initialDelay, String maxDelay, double scaleFactor)
    {
        return new BackoffPolicy(maxRetries, Duration.valueOf(initialDelay), Duration.valueOf(maxDelay), scaleFactor);
    }
}
