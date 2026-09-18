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
package io.trino.tests.framework;

import io.trino.tests.product.suite.SuiteRunner;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

class TestSuiteRunnerFailureReporting
{
    @Test
    void testPrintsCompleteFailureDetails()
    {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(FailureReportingFixture.class))
                .build();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        Launcher launcher = LauncherFactory.create();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getTestsFailedCount()).isEqualTo(1);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (PrintStream capturedOutput = new PrintStream(output, true, UTF_8)) {
            SuiteRunner.printSummary(List.of(new TestRunResult("test-run", "TestEnvironment", listener.getSummary())), capturedOutput);
        }

        assertThat(output.toString(UTF_8))
                .contains("[ERROR] failing invocation [1] value=\"test-value\" (TestEnvironment)")
                .contains("java.lang.RuntimeException: outer failure")
                .contains("Suppressed: java.lang.IllegalStateException: suppressed failure")
                .contains("Caused by: java.io.IOException: root failure")
                .containsPattern("at io\\.trino\\.tests\\.framework\\.FailureReportingFixture\\.failingTest\\(FailureReportingFixture\\.java:[0-9]+\\)")
                .doesNotContain("FailureReportingFixture.java:1)");
    }
}
