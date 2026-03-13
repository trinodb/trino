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

import io.airlift.log.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.testing.SystemEnvironmentUtils.isEnvSet;

/**
 * JUnit 5 extension that retries tests marked with {@link Flaky} annotation when they fail
 * with a stacktrace matching the specified pattern.
 * <p>
 * Retry is only enabled on CI (when CONTINUOUS_INTEGRATION environment variable is set or
 * {@code io.trino.testing.services.junit.FlakyTestRetryExtension.enabled} system property is true).
 */
public class FlakyTestRetryExtension
        implements InvocationInterceptor
{
    private static final Logger log = Logger.get(FlakyTestRetryExtension.class);

    private static final String ENABLED_SYSTEM_PROPERTY = "io.trino.testing.services.junit.FlakyTestRetryExtension.enabled";
    private static final int MAX_RETRIES = 2;

    @Override
    public void interceptTestMethod(
            Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext)
            throws Throwable
    {
        Method method = invocationContext.getExecutable();
        Flaky flaky = method.getAnnotation(Flaky.class);

        if (flaky == null) {
            // No @Flaky annotation, just proceed normally
            invocation.proceed();
            return;
        }

        if (!isRetryEnabled()) {
            log.info("Flaky test retry not enabled (not on CI), running test once: %s", formatTestName(extensionContext));
            invocation.proceed();
            return;
        }

        Pattern matchPattern = Pattern.compile(flaky.match());
        Throwable lastFailure = null;

        for (int attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
            try {
                if (attempt == 1) {
                    invocation.proceed();
                }
                else {
                    // Re-invoke the test method directly for retry attempts
                    method.invoke(extensionContext.getRequiredTestInstance());
                }
                // Test passed
                if (attempt > 1) {
                    log.info("Test %s passed on attempt %d", formatTestName(extensionContext), attempt);
                }
                return;
            }
            catch (Throwable throwable) {
                lastFailure = throwable;
                String stackTrace = getStackTraceAsString(throwable);

                if (!matchPattern.matcher(stackTrace).find()) {
                    log.warn(throwable, "Test %s failed but stacktrace does not match pattern '%s', not retrying",
                            formatTestName(extensionContext), flaky.match());
                    throw throwable;
                }

                if (attempt <= MAX_RETRIES) {
                    log.warn(throwable, "Test %s attempt %d failed (issue: %s), retrying...",
                            formatTestName(extensionContext), attempt, flaky.issue());
                }
                else {
                    log.error(throwable, "Test %s failed after %d attempts (issue: %s)",
                            formatTestName(extensionContext), attempt, flaky.issue());
                }
            }
        }

        throw lastFailure;
    }

    private static boolean isRetryEnabled()
    {
        Optional<String> enabledSystemPropertyValue = Optional.ofNullable(System.getProperty(ENABLED_SYSTEM_PROPERTY));
        return enabledSystemPropertyValue
                .map(Boolean::parseBoolean)
                .orElseGet(() -> isEnvSet("CONTINUOUS_INTEGRATION"));
    }

    private static String formatTestName(ExtensionContext context)
    {
        return context.getTestClass()
                .map(Class::getName)
                .orElse("<unknown>") + "::" + context.getDisplayName();
    }
}
