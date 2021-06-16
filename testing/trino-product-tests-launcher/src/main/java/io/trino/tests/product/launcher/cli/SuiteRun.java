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
package io.trino.tests.product.launcher.cli;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.jvm.Threads;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.LauncherModule;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentConfigFactory;
import io.trino.tests.product.launcher.env.EnvironmentFactory;
import io.trino.tests.product.launcher.env.EnvironmentModule;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteFactory;
import io.trino.tests.product.launcher.suite.SuiteModule;
import io.trino.tests.product.launcher.suite.SuiteTestRun;
import io.trino.tests.product.launcher.util.ConsoleTable;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import javax.inject.Inject;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.nanosSince;
import static io.airlift.units.Duration.succinctNanos;
import static io.trino.tests.product.launcher.cli.Commands.runCommand;
import static io.trino.tests.product.launcher.cli.SuiteRun.TestRunResult.HEADER;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getThreadMXBean;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

@Command(
        name = "run",
        description = "Run suite tests",
        usageHelpAutoWidth = true)
public class SuiteRun
        implements Callable<Integer>
{
    private static final Logger log = Logger.get(SuiteRun.class);

    private static final ScheduledExecutorService diagnosticExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TestRun-diagnostic"));

    private final Module additionalEnvironments;
    private final Module additionalSuites;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Mixin
    public SuiteRunOptions suiteRunOptions = new SuiteRunOptions();

    @Mixin
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    public SuiteRun(Extensions extensions)
    {
        this.additionalEnvironments = requireNonNull(extensions, "extensions is null").getAdditionalEnvironments();
        this.additionalSuites = requireNonNull(extensions, "extensions is null").getAdditionalSuites();
    }

    @Override
    public Integer call()
    {
        return runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new SuiteModule(additionalSuites))
                        .add(new EnvironmentModule(environmentOptions, additionalEnvironments))
                        .add(suiteRunOptions.toModule())
                        .build(),
                Execution.class);
    }

    public static class SuiteRunOptions
    {
        private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

        @Option(names = "--suite", paramLabel = "<suite>", description = "Name of the suite(s) to run (comma separated)", required = true, split = ",")
        public List<String> suites;

        @Option(names = "--test-jar", paramLabel = "<jar>", description = "Path to test JAR " + DEFAULT_VALUE, defaultValue = "${product-tests.module}/target/${product-tests.name}-${project.version}-executable.jar")
        public File testJar;

        @Option(names = "--cli-executable", paramLabel = "<jar>", description = "Path to CLI executable " + DEFAULT_VALUE, defaultValue = "${cli.bin}")
        public File cliJar;

        @Option(names = "--logs-dir", paramLabel = "<dir>", description = "Location of the exported logs directory " + DEFAULT_VALUE)
        public Optional<Path> logsDirBase;

        @Option(names = "--timeout", paramLabel = "<timeout>", description = "Maximum duration of suite execution " + DEFAULT_VALUE, defaultValue = "999d")
        public Duration timeout;

        public Module toModule()
        {
            return binder -> binder.bind(SuiteRunOptions.class).toInstance(this);
        }
    }

    public static class Execution
            implements Callable<Integer>
    {
        // TODO do not store mutable state
        private final SuiteRunOptions suiteRunOptions;
        // TODO do not store mutable state
        private final EnvironmentOptions environmentOptions;
        private final SuiteFactory suiteFactory;
        private final EnvironmentFactory environmentFactory;
        private final EnvironmentConfigFactory configFactory;
        private final long suiteStartTime;

        @Inject
        public Execution(
                SuiteRunOptions suiteRunOptions,
                EnvironmentOptions environmentOptions,
                SuiteFactory suiteFactory,
                EnvironmentFactory environmentFactory,
                EnvironmentConfigFactory configFactory)
        {
            this.suiteRunOptions = requireNonNull(suiteRunOptions, "suiteRunOptions is null");
            this.environmentOptions = requireNonNull(environmentOptions, "environmentOptions is null");
            this.suiteFactory = requireNonNull(suiteFactory, "suiteFactory is null");
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            this.configFactory = requireNonNull(configFactory, "configFactory is null");
            this.suiteStartTime = System.nanoTime();
        }

        @Override
        public Integer call()
        {
            AtomicBoolean finished = new AtomicBoolean();
            Future<?> diagnosticFlow = immediateFuture(null);
            try {
                long timeoutMillis = suiteRunOptions.timeout.toMillis();
                long marginMillis = SECONDS.toMillis(10);
                if (timeoutMillis < marginMillis) {
                    log.error("Unsupported small timeout value: %s ms", timeoutMillis);
                    return ExitCode.SOFTWARE;
                }

                diagnosticFlow = diagnosticExecutor.schedule(() -> reportSuspectedTimeout(finished), timeoutMillis - marginMillis, MILLISECONDS);

                return runSuites();
            }
            finally {
                finished.set(true);
                diagnosticFlow.cancel(true);
            }
        }

        private int runSuites()
        {
            List<String> suiteNames = requireNonNull(suiteRunOptions.suites, "suiteRunOptions.suites is null");
            EnvironmentConfig environmentConfig = configFactory.getConfig(environmentOptions.config);
            ImmutableList.Builder<TestRunResult> suiteResults = ImmutableList.builder();

            suiteNames.forEach(suiteName -> {
                Suite suite = suiteFactory.getSuite(suiteName);

                List<SuiteTestRun> suiteTestRuns = suite.getTestRuns(environmentConfig).stream()
                        .map(suiteRun -> suiteRun.withConfigApplied(environmentConfig))
                        .collect(toImmutableList());

                log.info("Running suite '%s' with config '%s' and test runs:\n%s",
                        suiteName,
                        environmentConfig.getConfigName(),
                        formatSuiteTestRuns(suiteTestRuns));

                List<TestRunResult> testRunsResults = suiteTestRuns.stream()
                        .map(testRun -> executeSuiteTestRun(suiteName, testRun, environmentConfig))
                        .collect(toImmutableList());

                suiteResults.addAll(testRunsResults);
            });

            List<TestRunResult> results = suiteResults.build();
            printTestRunsSummary(results);

            if (getFailedCount(results) > 0) {
                return ExitCode.SOFTWARE;
            }

            return ExitCode.OK;
        }

        private String formatSuiteTestRuns(List<SuiteTestRun> suiteTestRuns)
        {
            Joiner joiner = Joiner.on("\n");

            ConsoleTable table = new ConsoleTable();
            table.addHeader("environment", "options", "groups", "excluded groups", "tests", "excluded tests");
            suiteTestRuns.forEach(testRun -> table.addRow(
                    testRun.getEnvironmentName(),
                    testRun.getExtraOptions(),
                    joiner.join(testRun.getGroups()),
                    joiner.join(testRun.getExcludedGroups()),
                    joiner.join(testRun.getTests()),
                    joiner.join(testRun.getExcludedTests())).addSeparator());
            return table.render();
        }

        private void printTestRunsSummary(List<TestRunResult> results)
        {
            ConsoleTable table = new ConsoleTable();
            table.addHeader(HEADER);
            results.forEach(result -> table.addRow(result.toRow()));
            table.addSeparator();
            log.info("Suite tests results:\n%s", table.render());
        }

        private static long getFailedCount(List<TestRunResult> results)
        {
            return results.stream()
                    .filter(TestRunResult::hasFailed)
                    .count();
        }

        public TestRunResult executeSuiteTestRun(String suiteName, SuiteTestRun suiteTestRun, EnvironmentConfig environmentConfig)
        {
            String runId = generateRandomRunId();

            TestRun.TestRunOptions testRunOptions = createTestRunOptions(runId, suiteName, suiteTestRun, environmentConfig, suiteRunOptions.logsDirBase);
            if (testRunOptions.timeout.toMillis() == 0) {
                return new TestRunResult(
                        suiteName,
                        runId,
                        suiteTestRun,
                        environmentConfig,
                        new Duration(0, MILLISECONDS),
                        Optional.of(new Exception("Test execution not attempted because suite total running time limit was exhausted")));
            }

            log.info("Starting test run %s with config %s and remaining timeout %s", suiteTestRun, environmentConfig, testRunOptions.timeout);
            log.info("Execute this test run using:\n%s test run %s", environmentOptions.launcherBin, OptionsPrinter.format(environmentOptions, testRunOptions));

            Stopwatch stopwatch = Stopwatch.createStarted();
            Optional<Throwable> exception = runTest(runId, environmentConfig, testRunOptions);
            return new TestRunResult(suiteName, runId, suiteTestRun, environmentConfig, succinctNanos(stopwatch.stop().elapsed(NANOSECONDS)), exception);
        }

        private static String generateRandomRunId()
        {
            return UUID.randomUUID().toString().replace("-", "");
        }

        private Optional<Throwable> runTest(String runId, EnvironmentConfig environmentConfig, TestRun.TestRunOptions testRunOptions)
        {
            try {
                TestRun.Execution execution = new TestRun.Execution(environmentFactory, environmentOptions, environmentConfig, testRunOptions);

                log.info("Test run %s started", runId);
                int exitCode = execution.call();
                log.info("Test run %s finished", runId);

                if (exitCode > 0) {
                    return Optional.of(new RuntimeException(format("Tests exited with code %d", exitCode)));
                }

                return Optional.empty();
            }
            catch (RuntimeException e) {
                return Optional.of(e);
            }
        }

        private TestRun.TestRunOptions createTestRunOptions(String runId, String suiteName, SuiteTestRun suiteTestRun, EnvironmentConfig environmentConfig, Optional<Path> logsDirBase)
        {
            TestRun.TestRunOptions testRunOptions = new TestRun.TestRunOptions();
            testRunOptions.environment = suiteTestRun.getEnvironmentName();
            testRunOptions.extraOptions = suiteTestRun.getExtraOptions();
            testRunOptions.testArguments = suiteTestRun.getTemptoRunArguments();
            testRunOptions.testJar = suiteRunOptions.testJar;
            testRunOptions.cliJar = suiteRunOptions.cliJar;
            String suiteRunId = suiteRunId(runId, suiteName, suiteTestRun, environmentConfig);
            testRunOptions.reportsDir = Paths.get("presto-product-tests/target/reports/" + suiteRunId);
            testRunOptions.logsDirBase = logsDirBase.map(dir -> dir.resolve(suiteRunId));
            // Calculate remaining time
            testRunOptions.timeout = remainingTimeout();
            return testRunOptions;
        }

        private Duration remainingTimeout()
        {
            long remainingNanos = suiteRunOptions.timeout.roundTo(NANOSECONDS) - nanosSince(suiteStartTime).roundTo(NANOSECONDS);
            return succinctNanos(max(remainingNanos, 0));
        }

        private void reportSuspectedTimeout(AtomicBoolean finished)
        {
            if (finished.get()) {
                return;
            }

            // Test may not complete on time because they take too long (legitimate from Launcher's perspective), or because Launcher hangs.
            // In the latter case it's worth reporting Launcher's threads.

            // Log something immediately before getting thread information
            log.warn("Test execution is not finished yet, a deadlock or hang is suspected. Thread dump will follow.");
            log.warn(
                    "Full Thread Dump:\n%s",
                    Arrays.stream(getThreadMXBean().dumpAllThreads(true, true))
                            .map(Threads::fullToString)
                            .collect(joining("\n")));
        }

        private static String suiteRunId(String runId, String suiteName, SuiteTestRun suiteTestRun, EnvironmentConfig environmentConfig)
        {
            return format("%s-%s-%s-%s", suiteName, suiteTestRun.getEnvironmentName(), environmentConfig.getConfigName(), runId);
        }
    }

    static class TestRunResult
    {
        public static final Object[] HEADER = {
                "id", "suite", "environment", "config", "options", "status", "elapsed", "error"
        };

        private final String runId;
        private final SuiteTestRun suiteRun;
        private final EnvironmentConfig environmentConfig;
        private final Duration duration;
        private final Optional<Throwable> throwable;
        private final String suiteName;

        public TestRunResult(String suiteName, String runId, SuiteTestRun suiteRun, EnvironmentConfig environmentConfig, Duration duration, Optional<Throwable> throwable)
        {
            this.suiteName = suiteName;
            this.runId = runId;
            this.suiteRun = requireNonNull(suiteRun, "suiteRun is null");
            this.environmentConfig = requireNonNull(environmentConfig, "environmentConfig is null");
            this.duration = requireNonNull(duration, "duration is null");
            this.throwable = requireNonNull(throwable, "throwable is null");
        }

        public boolean hasFailed()
        {
            return this.throwable.isPresent();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("suiteName", suiteName)
                    .add("runId", runId)
                    .add("suiteRun", suiteRun)
                    .add("suiteConfig", environmentConfig)
                    .add("duration", duration)
                    .add("throwable", throwable)
                    .toString();
        }

        public Object[] toRow()
        {
            return new Object[] {
                    runId,
                    suiteName,
                    suiteRun.getEnvironmentName(),
                    environmentConfig.getConfigName(),
                    suiteRun.getExtraOptions(),
                    hasFailed() ? "FAILED" : "SUCCESS",
                    duration,
                    throwable.map(Throwable::getMessage).orElse("-")};
        }
    }
}
