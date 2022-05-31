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

import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import org.testng.IClassListener;
import org.testng.ITestClass;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static java.lang.String.format;

public class ProgressLoggingListener
        implements IClassListener, ITestListener
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
    public void onStart(ITestContext context)
    {
    }

    @Override
    public void onTestStart(ITestResult testCase)
    {
        if (!enabled) {
            return;
        }
        LOGGER.info("[TEST START] %s", formatTestName(testCase));
    }

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
    public void onTestFailedButWithinSuccessPercentage(ITestResult testCase)
    {
    }

    @Override
    public void onFinish(ITestContext context)
    {
    }

    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        if (!enabled) {
            return;
        }

        LOGGER.info("[BEFORE CLASS] %s", getName(testClass));
    }

    @Override
    public void onAfterClass(ITestClass testClass)
    {
        if (!enabled) {
            return;
        }

        LOGGER.info("[AFTER CLASS] %s", getName(testClass));
    }

    private String formatTestName(ITestResult testCase)
    {
        // See LogTestDurationListener.getName
        return format("%s.%s%s", testCase.getTestClass().getName(), testCase.getName(), formatTestParameters(testCase));
    }

    private String formatTestParameters(ITestResult testCase)
    {
        Object[] parameters = testCase.getParameters();
        if (parameters == null || parameters.length == 0) {
            return "";
        }
        return format(" [%s]", Joiner.on(", ").join(parameters));
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

    private static String getName(ITestClass testClass)
    {
        return testClass.getName();
    }
}
