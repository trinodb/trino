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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.jvm.Threads;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.testing.services.junit.Listeners.reportListenerFailure;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getThreadMXBean;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;

public class LogTestDurationListener
        implements TestExecutionListener
{
    private static final Logger log = Logger.get(LogTestDurationListener.class);

    private static final Duration GLOBAL_IDLE_LOGGING_THRESHOLD = new Duration(8, MINUTES);

    private final boolean enabled;
    private final ScheduledExecutorService scheduledExecutorService;

    private TestPlan currentTestPlan;
    private final Map<TestIdentifier, Long> started = new ConcurrentHashMap<>();
    private final AtomicLong lastChange = new AtomicLong(System.nanoTime());
    private final AtomicBoolean hangLogged = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();
    @GuardedBy("this")
    private ScheduledFuture<?> monitorHangTask;

    public LogTestDurationListener()
    {
        enabled = isEnabled();
        scheduledExecutorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("TestHangMonitor"));
    }

    private static boolean isEnabled()
    {
        if (System.getProperty("LogTestDurationListener.enabled") != null) {
            return Boolean.getBoolean("LogTestDurationListener.enabled");
        }
        if (System.getenv("CONTINUOUS_INTEGRATION") != null) {
            return true;
        }
        // For local development, logging durations is not typically useful.
        return false;
    }

    @Override
    public synchronized void testPlanExecutionStarted(TestPlan testPlan)
    {
        if (!enabled) {
            return;
        }

        try {
            checkState(currentTestPlan == null, "currentTestPlan already set to %s when starting %s", currentTestPlan, testPlan);
            currentTestPlan = testPlan;

            resetHangMonitor();
            finished.set(false);
            if (monitorHangTask == null) {
                monitorHangTask = scheduledExecutorService.scheduleWithFixedDelay(this::checkForTestHang, 5, 5, TimeUnit.SECONDS);
            }
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(getClass(), "testPlanExecutionStarted: \n%s", getStackTraceAsString(e));
        }
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan)
    {
        if (!enabled) {
            return;
        }

        try {
            checkState(currentTestPlan == testPlan, "currentTestPlan set to %s when finishing %s", currentTestPlan, testPlan);
            currentTestPlan = null;

            resetHangMonitor();
            finished.set(true);
            // do not stop hang task so notification of hung test JVM will fire
            // Note: since the monitor uses daemon threads it will not prevent JVM shutdown
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(getClass(), "testPlanExecutionFinished: \n%s", getStackTraceAsString(e));
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

        Map<TestIdentifier, Long> runningTests = ImmutableMap.copyOf(started);
        if (!runningTests.isEmpty()) {
            String testDetails = runningTests.entrySet().stream()
                    .map(entry -> format("%s running for %s", testName(entry.getKey()), nanosSince(entry.getValue())))
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
        log.warn("%s\n\nFull Thread Dump:\n%s", message,
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
    public void executionStarted(TestIdentifier testIdentifier)
    {
        if (!enabled) {
            return;
        }

        try {
            beginTest(testIdentifier);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(LogTestDurationListener.class, "executionStarted: \n%s", getStackTraceAsString(e));
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult)
    {
        if (!enabled) {
            return;
        }

        try {
            endTest(testIdentifier);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(LogTestDurationListener.class, "executionFinished: \n%s", getStackTraceAsString(e));
        }
    }

    private void beginTest(TestIdentifier testIdentifier)
    {
        resetHangMonitor();
        Long existingEntry = started.putIfAbsent(testIdentifier, System.nanoTime());
        // You can get concurrent tests with the same name when using @Factory.  Instead of adding complex support for
        // having multiple running tests with the same name, we simply don't use @Factory.
        checkState(existingEntry == null, "There already is a start record for test: %s", testIdentifier);
    }

    private Duration endTest(TestIdentifier testIdentifier)
    {
        resetHangMonitor();
        Long startTime = started.remove(testIdentifier);
        checkState(startTime != null, "There is no start record for test: %s", testIdentifier);
        return nanosSince(startTime);
    }

    private String testName(TestIdentifier testIdentifier)
    {
        Optional<TestIdentifier> parent = currentTestPlan.getParent(testIdentifier);
        String parentName = "";
        if (parent.isPresent()) {
            parentName = testName(parent.get());
            if (parentName.equals("JUnit Jupiter")) {
                parentName = "";
            }
            else {
                parentName += ".";
            }
        }
        return parentName + testIdentifier.getDisplayName();
    }
}
