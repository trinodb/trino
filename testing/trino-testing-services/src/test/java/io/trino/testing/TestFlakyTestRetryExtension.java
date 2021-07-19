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
package io.trino.testing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import static io.trino.testing.FlakyTestRetryExtension.ALLOWED_RETRIES_COUNT;
import static io.trino.testing.FlakyTestRetryExtension.FLAKY_TEST_RETRY_ENABLED_SYSTEM_PROPERTY;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

public class TestFlakyTestRetryExtension
{
    private final Launcher launcher = LauncherFactory.create();

    @BeforeEach
    public void beforeEach()
    {
        System.setProperty(FLAKY_TEST_RETRY_ENABLED_SYSTEM_PROPERTY, TRUE.toString());
    }

    @Test
    public void testFlakyPermanentFailing()
    {
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(TestFlakyPermanentFailing.class))
                .build();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getTestsFoundCount()).isEqualTo(2 + 2 * ALLOWED_RETRIES_COUNT);
        assertThat(listener.getSummary().getTestsFailedCount()).isEqualTo(2);
        assertThat(listener.getSummary().getTestsAbortedCount()).isEqualTo(2 * ALLOWED_RETRIES_COUNT);
    }

    @Test
    public void testFlakyFurtherAttemptSucceeded()
    {
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(TestFlakyFurtherAttemptSucceeded.class))
                .build();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getTestsFoundCount()).isEqualTo(3 + ALLOWED_RETRIES_COUNT);
        assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(2);
        assertThat(listener.getSummary().getTestsAbortedCount()).isEqualTo(1 + ALLOWED_RETRIES_COUNT);
    }

    @Test
    public void testDerivedFromOtherTest()
    {
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(TestDerivedFromOtherTest.class))
                .build();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getTestsFoundCount()).isEqualTo(6 + 2 * ALLOWED_RETRIES_COUNT);
        assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(3);
        assertThat(listener.getSummary().getTestsAbortedCount()).isEqualTo(2 + 2 * ALLOWED_RETRIES_COUNT);
    }

    @Tag("FlakyInternalTesting")
    public static class TestFlakyPermanentFailing
    {
        @SuppressWarnings("deprecation")
        @Flaky(issue = "issue A", match = "never be successful")
        public void test1()
        {
            fail("This test will never be successful");
        }

        @SuppressWarnings("deprecation")
        @Flaky(issue = "issue B", match = "never be successful")
        public void test2()
        {
            fail("This test will never be successful");
        }
    }

    @Tag("FlakyInternalTesting")
    @TestInstance(PER_CLASS)
    public static class TestFlakyFurtherAttemptSucceeded
    {
        private int test1ExecutionCount;
        private int test2ExecutionCount;

        @SuppressWarnings("deprecation")
        @Flaky(issue = "issue A", match = "")
        public void test2()
        {
            if (test2ExecutionCount++ < ALLOWED_RETRIES_COUNT) {
                fail(format("This test fails on first %d attempts", ALLOWED_RETRIES_COUNT));
            }
        }

        @SuppressWarnings("deprecation")
        @Flaky(issue = "issue A", match = "This test fails")
        public void test1()
        {
            if (test1ExecutionCount++ == 0) {
                fail("This test fails on first attempt");
            }
        }
    }

    public interface TestFlakyInterface
    {
        @SuppressWarnings("deprecation")
        @Flaky(issue = "Some interface flaky test", match = "")
        default void unluckyTest()
        {
            fail("No hope");
        }
    }

    @Tag("FlakyInternalTesting")
    @TestInstance(PER_CLASS)
    public static class TestDerivedFromOtherTest
            extends TestFlakyFurtherAttemptSucceeded
            implements TestFlakyInterface
    {
        private int testExecutionCount;

        @SuppressWarnings("deprecation")
        @Flaky(issue = "Derived flaky test", match = "")
        public void test()
        {
            if (testExecutionCount++ == 0) {
                fail("This test fails on first attempt");
            }
        }
    }
}
