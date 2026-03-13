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
package io.trino.tests.product;

import io.airlift.log.Logger;
import io.trino.testng.services.FlakyTestRetryAnalyzer;
import org.testng.IRetryAnalyzer;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.testng.services.FlakyTestRetryAnalyzer.ALLOWED_RETRIES_COUNT;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;

/**
 * Retries convention-based product tests that fail due to known transient
 * infrastructure failures that cannot be fixed in the test itself.
 */
public class ProductTestRetryAnalyzer
        implements IRetryAnalyzer
{
    private static final Logger log = Logger.get(ProductTestRetryAnalyzer.class);

    private static final String DISABLED_SYSTEM_PROPERTY = "io.trino.tests.product.ProductTestRetryAnalyzer.disabled";

    private static final Pattern INFRASTRUCTURE_FAILURE_PATTERN = Pattern.compile(RETRYABLE_FAILURES_MATCH);

    private final Map<String, Long> retryCounter = new ConcurrentHashMap<>();

    @Override
    public boolean retry(ITestResult result)
    {
        if (result.isSuccess()) {
            return false;
        }

        if (Boolean.getBoolean(DISABLED_SYSTEM_PROPERTY)) {
            log.info("ProductTestRetryAnalyzer disabled via system property '%s'", DISABLED_SYSTEM_PROPERTY);
            return false;
        }

        if (result.getThrowable() == null) {
            return false;
        }

        String stackTrace = getStackTraceAsString(result.getThrowable());
        if (!INFRASTRUCTURE_FAILURE_PATTERN.matcher(stackTrace).find()) {
            return false;
        }

        ITestNGMethod method = result.getMethod();
        String testId = FlakyTestRetryAnalyzer.testId(method, result.getParameters());
        long retryCount = retryCounter.merge(testId, 1L, Long::sum);
        if (retryCount > ALLOWED_RETRIES_COUNT) {
            return false;
        }
        log.warn(
                result.getThrowable(),
                "Test %s::%s attempt %s failed due to infrastructure failure, retrying...,",
                result.getTestClass().getName(),
                method.getMethodName(),
                retryCount);
        return true;
    }
}
