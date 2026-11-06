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
package io.trino.testing.services.junit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
class TestFlakyTestRetryExtension
{
    private int attempts;
    private long lastAttemptNanos;
    private TestInfo originalTestInfo;
    private final List<String> attemptedValues = new ArrayList<>();

    @Test
    void testRetryWithInterruptFlagSet()
    {
        FlakyRetryFixture.attempts = 0;
        FlakyRetryFixture.interruptedRetry = false;
        FlakyRetryFixture.interruptedAfterEach = false;

        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectMethod(FlakyRetryFixture.class, "test"))
                .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                .build();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();

        try {
            LauncherFactory.create().execute(request, listener);

            assertThat(listener.getSummary().getTestsFailedCount()).isZero();
            assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(1);
            assertThat(FlakyRetryFixture.attempts).isEqualTo(2);
            assertThat(FlakyRetryFixture.interruptedRetry).isTrue();
            assertThat(FlakyRetryFixture.interruptedAfterEach).isTrue();
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    void testTimeoutDuringBackoffStopsRetries()
    {
        FlakyRetryFixture.attempts = 0;

        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectMethod(FlakyRetryFixture.class, "testTimeoutDuringBackoff"))
                .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                .build();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();

        try {
            LauncherFactory.create().execute(request, listener);

            assertThat(listener.getSummary().getTestsFailedCount()).isEqualTo(1);
            assertThat(FlakyRetryFixture.attempts).isEqualTo(1);

            Throwable failure = listener.getSummary().getFailures().get(0).getException();
            assertThat(failure).isInstanceOf(TimeoutException.class);
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    @Flaky(issue = "intentionally flaky for @Flaky test purposes", match = "I am trying hard to fail!")
    void testRetryingWithArgument(TestInfo testInfo)
    {
        assertRetryDelay(attempts);
        if (attempts == 0) {
            originalTestInfo = testInfo;
        }
        assertThat(testInfo).isSameAs(originalTestInfo);
        attempts++;
        if (attempts <= 2) {
            fail("I am trying hard to fail!");
        }
        assertThat(attempts).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    @Flaky(issue = "intentionally flaky for @Flaky test purposes", match = "I am trying hard to fail!")
    void testRetryingParameterized(String value)
    {
        assertRetryDelay(attemptedValues.size());
        attemptedValues.add(value);
        if (attemptedValues.size() <= 2) {
            fail("I am trying hard to fail! value: " + value);
        }
        assertThat(attemptedValues).containsExactly(value, value, value);
    }

    private void assertRetryDelay(int previousAttempts)
    {
        long attemptNanos = System.nanoTime();
        if (previousAttempts > 0) {
            assertThat(attemptNanos - lastAttemptNanos)
                    .isGreaterThanOrEqualTo(TimeUnit.SECONDS.toNanos(previousAttempts));
        }
        lastAttemptNanos = attemptNanos;
    }
}
