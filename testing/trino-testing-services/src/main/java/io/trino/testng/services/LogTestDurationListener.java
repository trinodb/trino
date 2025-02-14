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
package io.trino.testng.services;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.jvm.Threads;
import org.testng.IClassListener;
import org.testng.IExecutionListener;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestClass;
import org.testng.ITestResult;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.testing.SystemEnvironmentUtils.isEnvSet;
import static io.trino.testng.services.Listeners.formatTestName;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getThreadMXBean;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

public class LogTestDurationListener
        implements IExecutionListener, IClassListener, IInvokedMethodListener
{
    private static final Logger LOG = Logger.get(LogTestDurationListener.class);

    private static final Duration SINGLE_TEST_LOGGING_THRESHOLD = new Duration(30, SECONDS);
    private static final Duration CLASS_LOGGING_THRESHOLD = new Duration(1, MINUTES);
    // Must be below Travis "no output" timeout (10m). E.g. TestElasticsearchIntegrationSmokeTest is known to take ~5-6m.
    private static final Duration GLOBAL_IDLE_LOGGING_THRESHOLD = new Duration(8, MINUTES);

    private final boolean enabled;
    private final ScheduledExecutorService scheduledExecutorService;

    private final Map<String, Long> started = new ConcurrentHashMap<>();
    private final AtomicLong lastChange = new AtomicLong(System.nanoTime());
    private final AtomicBoolean hangLogged = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();
    @GuardedBy("this")
    private ScheduledFuture<?> monitorHangTask;

    public LogTestDurationListener()
    {
        enabled = isEnabled();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("TestHangMonitor"));
        LOG.info("LogTestDurationListener enabled: %s", enabled);
    }

    private static boolean isEnabled()
    {
        if (System.getProperty("LogTestDurationListener.enabled") != null) {
            return Boolean.getBoolean("LogTestDurationListener.enabled");
        }
        if (isEnvSet("CONTINUOUS_INTEGRATION")) {
            return true;
        }
        // LogTestDurationListener does not support concurrent invocations of same test method
        // (e.g. @Test(threadPoolSize=n)), so it should be disabled for local development purposes.
        return false;
    }

    @Override
    public synchronized void onExecutionStart()
    {
        if (!enabled) {
            return;
        }

        try {
            resetHangMonitor();
            finished.set(false);
            if (monitorHangTask == null) {
                monitorHangTask = scheduledExecutorService.scheduleWithFixedDelay(this::checkForTestHang, 5, 5, TimeUnit.SECONDS);
            }
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(LogTestDurationListener.class, "onExecutionStart: \n%s", getStackTraceAsString(e));
        }
    }

    @Override
    public synchronized void onExecutionFinish()
    {
        if (!enabled) {
            return;
        }

        try {
            resetHangMonitor();
            finished.set(true);
            // do not stop hang task so notification of hung test JVM will fire
            // Note: since the monitor uses daemon threads it will not prevent JVM shutdown
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(LogTestDurationListener.class, "onExecutionFinish: \n%s", getStackTraceAsString(e));
        }
    }

    private void checkForTestHang()
    {
        if (hangLogged.get()) {
            return;
        }

        Duration duration = nanosSince(lastChange.get());
        if (duration.compareTo(GLOBAL_IDLE_LOGGING_THRESHOLD) < 0) {
            return;
        }

        if (!hangLogged.compareAndSet(false, true)) {
            return;
        }

        Map<String, Long> runningTests = ImmutableMap.copyOf(started);
        if (!runningTests.isEmpty()) {
            String testDetails = runningTests.entrySet().stream()
                    .map(entry -> format("%s running for %s", entry.getKey(), nanosSince(entry.getValue())))
                    .collect(joining("\n\t", "\n\t", ""));
            dumpAllThreads(format("No test started or completed in %s. Running tests:%s.", GLOBAL_IDLE_LOGGING_THRESHOLD, testDetails));
        }
        else if (finished.get()) {
            dumpAllThreads(format("Tests finished, but JVM did not shutdown in %s.", GLOBAL_IDLE_LOGGING_THRESHOLD));
        }
        else {
            dumpAllThreads(format("No test started in %s", GLOBAL_IDLE_LOGGING_THRESHOLD));
        }
    }

    private static void dumpAllThreads(String message)
    {
        LOG.warn("%s\n\nFull Thread Dump:\n%s", message,
                Arrays.stream(getThreadMXBean().dumpAllThreads(true, true))
                        .map(Threads::fullToString)
                        .collect(joining("\n")));
    }

    private void resetHangMonitor()
    {
        lastChange.set(System.nanoTime());
        hangLogged.set(false);
    }

    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        if (!enabled) {
            return;
        }

        try {
            beginTest(formatTestName(testClass));
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(LogTestDurationListener.class, "onBeforeClass: \n%s", getStackTraceAsString(e));
        }
    }

    @Override
    public void onAfterClass(ITestClass testClass)
    {
        if (!enabled) {
            return;
        }

        try {
            String name = formatTestName(testClass);
            Duration duration = endTest(name);
            if (duration.compareTo(CLASS_LOGGING_THRESHOLD) > 0) {
                LOG.warn("Tests from %s took %s", name, duration);
            }
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(LogTestDurationListener.class, "onAfterClass: \n%s", getStackTraceAsString(e));
        }
    }

    @Override
    public void beforeInvocation(IInvokedMethod method, ITestResult testResult)
    {
        if (!enabled) {
            return;
        }

        try {
            beginTest(formatTestName(testResult));
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(LogTestDurationListener.class, "beforeInvocation: \n%s", getStackTraceAsString(e));
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod method, ITestResult testResult)
    {
        if (!enabled) {
            return;
        }

        try {
            String name = formatTestName(testResult);
            Duration duration = endTest(name);
            if (duration.compareTo(SINGLE_TEST_LOGGING_THRESHOLD) > 0) {
                LOG.info("Test %s took %s", name, duration);
            }
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(LogTestDurationListener.class, "afterInvocation: \n%s", getStackTraceAsString(e));
        }
    }

    private void beginTest(String name)
    {
        resetHangMonitor();
        Long existingEntry = started.putIfAbsent(name, System.nanoTime());
        // You can get concurrent tests with the same name when using @Factory.  Instead of adding complex support for
        // having multiple running tests with the same name, we simply don't use @Factory.
        checkState(existingEntry == null, "There already is a start record for test: %s", name);
    }

    private Duration endTest(String name)
    {
        resetHangMonitor();
        Long startTime = started.remove(name);
        checkState(startTime != null, "There is no start record for test: %s", name);
        return nanosSince(startTime);
    }
}
