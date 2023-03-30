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

import io.airlift.log.Logger;
import org.testng.IClassListener;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestClass;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static io.trino.testng.services.Listeners.formatTestName;
import static java.lang.String.format;

public class ProgressLoggingListener
        implements IClassListener, ITestListener, IInvokedMethodListener
{
    private static final Logger LOGGER = Logger.get(ProgressLoggingListener.class);
    private final boolean enabled;

    public ProgressLoggingListener()
    {
        enabled = isEnabled();
        if (!enabled) {
            LOGGER.info("ProgressLoggingListener is disabled!");
        }
    }

    private static boolean isEnabled()
    {
        if (System.getProperty("ProgressLoggingListener.enabled") != null) {
            return Boolean.getBoolean("ProgressLoggingListener.enabled");
        }
        if (System.getenv("CONTINUOUS_INTEGRATION") != null) {
            return true;
        }
        // most often not useful for local development
        return false;
    }

    @Override
    public void onStart(ITestContext context) {}

    @Override
    public void beforeInvocation(IInvokedMethod method, ITestResult testResult)
    {
        if (!enabled) {
            return;
        }
        boolean testMethod = method.isTestMethod();
        boolean configurationMethod = method.isConfigurationMethod();
        if (testMethod) {
            LOGGER.info("[TEST START] %s", formatTestName(testResult));
        }
        if (configurationMethod) {
            LOGGER.info("[CONFIGURATION] %s", method);
        }
        if (!testMethod && !configurationMethod) {
            LOGGER.info("[UNKNOWN THING] %s for %s", method, formatTestName(testResult));
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod method, ITestResult testResult) {}

    @Override
    public void onTestStart(ITestResult testCase) {}

    @Override
    public void onTestSuccess(ITestResult testCase)
    {
        if (!enabled) {
            return;
        }
        LOGGER.info("[TEST SUCCESS] %s; (took: %s)", formatTestName(testCase), formatDuration(testCase));
    }

    @Override
    public void onTestFailure(ITestResult testCase)
    {
        if (!enabled) {
            return;
        }
        if (testCase.getThrowable() != null) {
            LOGGER.error(testCase.getThrowable(), "[TEST FAILURE] %s; (took: %s)", formatTestName(testCase), formatDuration(testCase));
        }
        else {
            LOGGER.error("[TEST FAILURE] %s; (took: %s)", formatTestName(testCase), formatDuration(testCase));
        }
    }

    @Override
    public void onTestSkipped(ITestResult testCase)
    {
        if (!enabled) {
            return;
        }
        LOGGER.info("[TEST SKIPPED] %s", formatTestName(testCase));
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult testCase) {}

    @Override
    public void onFinish(ITestContext context) {}

    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        if (!enabled) {
            return;
        }

        LOGGER.info("[BEFORE CLASS] %s", formatTestName(testClass));
    }

    @Override
    public void onAfterClass(ITestClass testClass)
    {
        if (!enabled) {
            return;
        }

        LOGGER.info("[AFTER CLASS] %s", formatTestName(testClass));
    }

    private static String formatDuration(ITestResult testCase)
    {
        return formatDuration(testCase.getEndMillis() - testCase.getStartMillis());
    }

    private static String formatDuration(long durationInMillis)
    {
        BigDecimal durationSeconds = durationInSeconds(durationInMillis);
        if (durationSeconds.longValue() < 60L) {
            return format("%s seconds", durationSeconds);
        }
        long minutes = durationSeconds.longValue() / 60L;
        long restSeconds = durationSeconds.longValue() % 60L;
        return format("%d minutes and %d seconds", minutes, restSeconds);
    }

    private static BigDecimal durationInSeconds(long millis)
    {
        return (new BigDecimal(millis)).divide(new BigDecimal(1000), 1, RoundingMode.HALF_UP);
    }
}
