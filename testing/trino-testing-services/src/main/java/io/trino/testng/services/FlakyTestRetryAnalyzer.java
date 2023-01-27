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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import org.testng.IRetryAnalyzer;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

import javax.annotation.concurrent.GuardedBy;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.lang.String.format;

public class FlakyTestRetryAnalyzer
        implements IRetryAnalyzer
{
    private static final Logger log = Logger.get(FlakyTestRetryAnalyzer.class);

    // This property exists so that flaky tests are retried on CI only by default but tests of retrying pass locally as well.
    // TODO replace pom.xml property with explicit invocation of a testng runner (test suite with a test) and amend the retryer behavior on that level
    private static final String ENABLED_SYSTEM_PROPERTY = "io.trino.testng.services.FlakyTestRetryAnalyzer.enabled";

    @VisibleForTesting
    static final int ALLOWED_RETRIES_COUNT = 2;

    @GuardedBy("this")
    private final Map<String, Long> retryCounter = new HashMap<>();

    @Override
    public boolean retry(ITestResult result)
    {
        if (result.isSuccess()) {
            return false;
        }

        Optional<String> enabledSystemPropertyValue = Optional.ofNullable(System.getProperty(ENABLED_SYSTEM_PROPERTY));
        if (!enabledSystemPropertyValue.map(Boolean::parseBoolean)
                .orElseGet(() -> System.getenv("CONTINUOUS_INTEGRATION") != null)) {
            log.info(
                    "FlakyTestRetryAnalyzer not enabled: " +
                            "CONTINUOUS_INTEGRATION environment is not detected or " +
                            "system property '%s' is not set to 'true' (actual: %s)",
                    ENABLED_SYSTEM_PROPERTY,
                    enabledSystemPropertyValue.orElse("<not set>"));
            return false;
        }

        Method javaMethod = result.getMethod().getConstructorOrMethod().getMethod();
        if (javaMethod == null) {
            log.info("not retrying; cannot get java method");
            return false;
        }
        Flaky annotation = javaMethod.getAnnotation(Flaky.class);
        if (annotation == null) {
            log.info("not retrying; @Flaky annotation not present");
            return false;
        }
        if (result.getThrowable() == null) {
            log.info("not retrying; throwable not present in result");
            return false;
        }
        String stackTrace = getStackTraceAsString(result.getThrowable());
        if (!Pattern.compile(annotation.match()).matcher(stackTrace).find()) {
            log.warn("not retrying; stacktrace does not match pattern '%s': [%s]", annotation.match(), stackTrace);
            return false;
        }

        long retryCount;
        ITestNGMethod method = result.getMethod();
        synchronized (this) {
            String name = getName(method, result.getParameters());
            retryCount = retryCounter.getOrDefault(name, 0L);
            retryCount++;
            if (retryCount > ALLOWED_RETRIES_COUNT) {
                return false;
            }
            retryCounter.put(name, retryCount);
        }
        log.warn(
                result.getThrowable(),
                "Test %s::%s attempt %s failed, retrying...,",
                result.getTestClass().getName(),
                method.getMethodName(),
                retryCount);
        return true;
    }

    private static String getName(ITestNGMethod method, Object[] parameters)
    {
        String actualTestClass = method.getTestClass().getName();
        if (parameters.length != 0) {
            return format(
                    "%s::%s(%s)",
                    actualTestClass,
                    method.getMethodName(),
                    Joiner.on(",").useForNull("null").join(parameters));
        }
        return format("%s::%s", actualTestClass, method.getMethodName());
    }
}
