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
package io.trino.tests.product.suite;

import io.trino.testing.containers.environment.EnvironmentManager;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TagFilter;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.lang.annotation.Annotation;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;

/**
 * Utility class for running JUnit tests programmatically with environment lifecycle management.
 * <p>
 * This class provides methods to:
 * <ul>
 *   <li>Discover and run tests filtered by JUnit tags</li>
 *   <li>Ensure proper environment shutdown between test runs</li>
 *   <li>Collect and summarize test results</li>
 * </ul>
 * <p>
 * <b>Important:</b> This class uses the shared {@link EnvironmentManager#shared()} instance
 * which is also used by {@link io.trino.testing.containers.environment.ProductTestExtension}.
 * This ensures that environments started by the JUnit extension are properly tracked and
 * shut down between suite runs, avoiding duplicate container issues.
 * <p>
 * Usage in suite main() methods:
 * <pre>{@code
 * public static void main(String[] args) {
 *     System.setProperty("trino.product-test.environment-mode", "STRICT");
 *
 *     List<TestRunResult> results = new ArrayList<>();
 *     results.add(SuiteRunner.forEnvironment(MySqlEnvironment.class)
 *             .includeTag(TestGroup.Mysql.class)
 *             .run());
 *     results.add(SuiteRunner.forEnvironment(MariaDbEnvironment.class)
 *             .includeTag(TestGroup.Mariadb.class)
 *             .run());
 *
 *     SuiteRunner.printSummary(results);
 *     System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
 * }
 * }</pre>
 */
public final class SuiteRunner
{
    private static final String TEST_PACKAGE = "io.trino.tests.product";
    private static final String PRODUCT_TESTS_TIME_ZONE = "Asia/Kathmandu";

    private SuiteRunner() {}

    /**
     * Extracts the tag string from a {@link TestGroup} annotation class.
     * <p>
     * This is used to derive the JUnit tag from the annotation's {@link Tag} meta-annotation,
     * ensuring that suite runners stay in sync with the annotation definitions.
     *
     * @param annotation the annotation class (e.g., {@code TestGroup.Mysql.class})
     * @return the tag string from the {@link Tag} meta-annotation
     * @throws IllegalArgumentException if the annotation is not meta-annotated with {@link Tag}
     */
    private static String tagOf(Class<? extends Annotation> annotation)
    {
        Tag tag = annotation.getAnnotation(Tag.class);
        if (tag == null) {
            throw new IllegalArgumentException(annotation.getName() + " is not annotated with @Tag");
        }
        return tag.value();
    }

    private static FailureInfo failureInfo(TestExecutionSummary.Failure failure)
    {
        TestSource source = failure.getTestIdentifier().getSource().orElse(null);
        if (source instanceof MethodSource methodSource) {
            String className = methodSource.getClassName();
            String methodName = methodSource.getMethodName();
            String simpleClassName = className.substring(className.lastIndexOf('.') + 1);
            return new FailureInfo(className, methodName, simpleClassName, failure.getTestIdentifier().getDisplayName());
        }
        return new FailureInfo("", "", "", failure.getTestIdentifier().getDisplayName());
    }

    private static TestRunResult run(
            Class<? extends ProductTestEnvironment> environmentClass,
            String runName,
            List<String> includeTags,
            List<String> excludeTags)
    {
        // Legacy launcher forced this timezone for all product test JVM executions.
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(PRODUCT_TESTS_TIME_ZONE)));
        System.setProperty("user.timezone", PRODUCT_TESTS_TIME_ZONE);

        String includeExpr = String.join(" & ", includeTags);
        String excludeExpr = String.join(" | ", excludeTags);
        String tagExpression;
        if (excludeTags.isEmpty()) {
            tagExpression = includeExpr;
        }
        else if (includeTags.isEmpty()) {
            tagExpression = "!(" + excludeExpr + ")";
        }
        else if (includeTags.size() == 1) {
            tagExpression = includeExpr + " & !(" + excludeExpr + ")";
        }
        else {
            tagExpression = "(" + includeExpr + ") & !(" + excludeExpr + ")";
        }

        System.out.println("\n========================================");
        System.out.println("Starting test run: " + runName);
        System.out.println("Expected environment: " + environmentClass.getSimpleName());
        System.out.println("Tag filter: " + tagExpression);
        System.out.println("========================================\n");

        EnvironmentManager manager = EnvironmentManager.shared();

        try {
            manager.acquire(environmentClass);

            LauncherDiscoveryRequestBuilder requestBuilder = LauncherDiscoveryRequestBuilder.request()
                    .selectors(DiscoverySelectors.selectPackage(TEST_PACKAGE));
            requestBuilder.filters(TagFilter.includeTags(tagExpression));
            LauncherDiscoveryRequest request = requestBuilder.build();

            LauncherConfig config = LauncherConfig.builder()
                    .enableTestExecutionListenerAutoRegistration(false)
                    .build();

            Launcher launcher = LauncherFactory.create(config);
            SummaryGeneratingListener listener = new SummaryGeneratingListener();
            launcher.registerTestExecutionListeners(listener);
            launcher.execute(request);

            TestExecutionSummary summary = listener.getSummary();

            System.out.println("\n----------------------------------------");
            System.out.println("Test run completed: " + runName);
            System.out.println("Tests found: " + summary.getTestsFoundCount());
            System.out.println("Tests succeeded: " + summary.getTestsSucceededCount());
            System.out.println("Tests failed: " + summary.getTestsFailedCount());
            System.out.println("Tests skipped: " + summary.getTestsSkippedCount());
            System.out.println("Containers failed: " + summary.getContainersFailedCount());
            System.out.println("----------------------------------------\n");

            return new TestRunResult(runName, environmentClass.getSimpleName(), summary);
        }
        finally {
            System.out.println("Shutting down environment for next run...");
            manager.shutdown();
        }
    }

    /**
     * Prints a combined summary of all test runs.
     *
     * @param results the list of test run results
     */
    public static void printSummary(List<TestRunResult> results)
    {
        System.out.println("\n========================================");
        System.out.println("           COMBINED TEST SUMMARY        ");
        System.out.println("========================================\n");

        long totalFound = 0;
        long totalSucceeded = 0;
        long totalFailed = 0;
        long totalSkipped = 0;
        long totalContainerFailed = 0;

        for (TestRunResult result : results) {
            TestExecutionSummary summary = result.summary();
            totalFound += summary.getTestsFoundCount();
            totalSucceeded += summary.getTestsSucceededCount();
            totalFailed += summary.getTestsFailedCount();
            totalSkipped += summary.getTestsSkippedCount();
            totalContainerFailed += summary.getContainersFailedCount();

            System.out.printf("%-30s | Found: %3d | Passed: %3d | Failed: %3d | Skipped: %3d | CFail: %3d%n",
                    result.runName(),
                    summary.getTestsFoundCount(),
                    summary.getTestsSucceededCount(),
                    summary.getTestsFailedCount(),
                    summary.getTestsSkippedCount(),
                    summary.getContainersFailedCount());
        }

        System.out.println("----------------------------------------");
        System.out.printf("%-30s | Found: %3d | Passed: %3d | Failed: %3d | Skipped: %3d | CFail: %3d%n",
                "TOTAL",
                totalFound,
                totalSucceeded,
                totalFailed,
                totalSkipped,
                totalContainerFailed);
        System.out.println("========================================\n");

        if (hasFailures(results)) {
            System.out.println("FAILURE DETAILS:");
            System.out.println("----------------");
            for (TestRunResult result : results) {
                List<TestExecutionSummary.Failure> failures = result.summary().getFailures().stream()
                        .sorted(Comparator
                                .comparing((TestExecutionSummary.Failure failure) -> failureInfo(failure).className())
                                .thenComparing(failure -> failureInfo(failure).methodName())
                                .thenComparing(failure -> failureInfo(failure).displayName()))
                        .toList();
                if (!failures.isEmpty()) {
                    System.out.println("\n" + result.runName() + " (" + result.environmentName() + "):");
                    for (TestExecutionSummary.Failure failure : failures) {
                        Throwable exception = failure.getException();
                        String prefix = (exception instanceof AssertionError) ? "[FAILURE]" : "[ERROR]";
                        FailureInfo info = failureInfo(failure);

                        if (!info.className().isEmpty()) {
                            System.out.println(prefix + " " + info.methodName() + "(" + result.environmentName() + ")");
                            System.out.println("    at " + info.className() + "." + info.methodName() + "(" + info.simpleClassName() + ".java:1)");
                        }
                        else {
                            System.out.println(prefix + " " + info.displayName());
                        }

                        System.out.println("    " + exception.getClass().getSimpleName() + ": " + exception.getMessage());
                    }
                }
            }
            System.out.println();
        }
    }

    /**
     * Checks if any test run had failures.
     *
     * @param results the list of test run results
     * @return true if any test run had failures
     */
    public static boolean hasFailures(List<TestRunResult> results)
    {
        return results.stream()
                .anyMatch(r -> r.summary().getTestsFailedCount() > 0 || r.summary().getContainersFailedCount() > 0);
    }

    /**
     * Creates a builder for a test run with tag-based filtering.
     * <p>
     * Use this for complex tag expressions involving multiple include/exclude tags:
     * <pre>{@code
     * SuiteRunner.forEnvironment(KafkaBasicEnvironment.class)
     *         .includeTag(TestGroup.Kafka.class)
     *         .excludeTag(TestGroup.KafkaSchemaRegistry.class)
     *         .run()
     * }</pre>
     *
     * @param environmentClass the environment class for this test run
     * @return a new {@link TestRun} builder
     */
    public static TestRun forEnvironment(Class<? extends ProductTestEnvironment> environmentClass)
    {
        return new TestRun(environmentClass);
    }

    public static final class TestRun
    {
        private final Class<? extends ProductTestEnvironment> environmentClass;
        private final List<Class<? extends Annotation>> includeTags = new ArrayList<>();
        private final List<Class<? extends Annotation>> excludeTags = new ArrayList<>();

        private TestRun(Class<? extends ProductTestEnvironment> environmentClass)
        {
            this.environmentClass = requireNonNull(environmentClass, "environmentClass is null");
        }

        public TestRun includeTag(Class<? extends Annotation> tag)
        {
            includeTags.add(requireNonNull(tag, "tag is null"));
            return this;
        }

        public TestRun excludeTag(Class<? extends Annotation> tag)
        {
            excludeTags.add(requireNonNull(tag, "tag is null"));
            return this;
        }

        public TestRunResult run()
        {
            if (includeTags.isEmpty() && excludeTags.isEmpty()) {
                throw new IllegalStateException("At least one include or exclude tag must be specified");
            }

            List<String> includeTagNames = includeTags.stream()
                    .map(SuiteRunner::tagOf)
                    .toList();
            List<String> excludeTagNames = excludeTags.stream()
                    .map(SuiteRunner::tagOf)
                    .toList();

            String runName = includeTags.isEmpty()
                    ? "all"
                    : tagOf(includeTags.getFirst());

            return SuiteRunner.run(environmentClass, runName, includeTagNames, excludeTagNames);
        }
    }

    private record FailureInfo(
            String className,
            String methodName,
            String simpleClassName,
            String displayName) {}

    /**
     * Result of a single test run.
     */
    public record TestRunResult(
            String runName,
            String environmentName,
            TestExecutionSummary summary) {}
}
