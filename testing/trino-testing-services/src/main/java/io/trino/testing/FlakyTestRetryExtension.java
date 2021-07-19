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
package io.trino.testing;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.opentest4j.TestAbortedException;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;
import static org.junit.platform.commons.util.AnnotationUtils.isAnnotated;

public class FlakyTestRetryExtension
        implements TestTemplateInvocationContextProvider, BeforeTestExecutionCallback, AfterTestExecutionCallback,
        TestExecutionExceptionHandler
{
    private static final Logger log = Logger.get(FlakyTestRetryExtension.class);
    private static final String RETRY_STATE_STORE_KEY = "flakyTestRetryState";

    @VisibleForTesting
    static final String FLAKY_TEST_RETRY_ENABLED_SYSTEM_PROPERTY = "test.flaky-test-retry.enabled";
    @VisibleForTesting
    static final String CONTINUOUS_INTEGRATION_ENVIRONMENT = "CONTINUOUS_INTEGRATION";
    @VisibleForTesting
    static final int ALLOWED_RETRIES_COUNT = 2;

    @SuppressWarnings("deprecation")
    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext)
    {
        return isAnnotated(extensionContext.getTestMethod(), Flaky.class);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext extensionContext)
    {
        if (findAnnotation(extensionContext.getTestMethod(), org.testng.annotations.Test.class).isPresent()) {
            throw new TestAbortedException("This is a TestNG test, will be executed by TestNG");
        }

        @SuppressWarnings("deprecation")
        Flaky flakyTestAnnotation = findAnnotation(extensionContext.getTestMethod(), Flaky.class)
                .orElseThrow(() -> new RuntimeException("Provided test method is not annotated with @" + Flaky.class.getName()));

        getStore(extensionContext).put(
                RETRY_STATE_STORE_KEY,
                new RetryStateHolder(
                        new FlakyTestRetryState(
                                isFlakyTestRetryEnabled() ? ALLOWED_RETRIES_COUNT : 0,
                                flakyTestAnnotation.issue(),
                                Pattern.compile(flakyTestAnnotation.match()))));

        return Stream.iterate(
                new TestTemplateInvocationContext() {},
                t -> shouldRetryTest(extensionContext),
                t -> new TestTemplateInvocationContext() {});
    }

    @Override
    public void beforeTestExecution(ExtensionContext extensionContext)
    {
        FlakyTestRetryState state = getRetryStateHolder(extensionContext).get();
        log.info(
                "Flaky test %s.%s, run number %d (maximum retries %d)",
                extensionContext.getRequiredTestInstance().getClass().getName(),
                extensionContext.getRequiredTestMethod(),
                state.retry() + 1,
                state.maxRetries());
    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext)
    {
        if (extensionContext.getExecutionException().isEmpty()) {
            getRetryStateHolder(extensionContext).updateAndGet(FlakyTestRetryState::testSuccess);
        }
    }

    @Override
    public void handleTestExecutionException(ExtensionContext extensionContext, Throwable throwable)
            throws Throwable
    {
        FlakyTestRetryState retryState = getRetryStateHolder(extensionContext)
                .updateAndGet(previousState -> previousState.testFailure(throwable));

        if (retryState.shouldRetryTest()) {
            log.warn("Flaky test fail, we will repeat it due to known issue (%s) ", retryState.issue());
            throw new TestAbortedException(format("Flaky test fail, we will repeat it due to known issue (%s) ", retryState.issue()), throwable);
        }

        if (retryState.isTestFailed()) {
            log.error(throwable, "Flaky test ultimately fail due to an unknown error (not matched by flaky annotation)");
        }
        else {
            log.error("Flaky test ultimately fail due to a number of retries exhausted (%d)", retryState.retry() - 1);
        }

        throw throwable;
    }

    private boolean isFlakyTestRetryEnabled()
    {
        Optional<String> enabledSystemPropertyValue = Optional.ofNullable(System.getProperty(FLAKY_TEST_RETRY_ENABLED_SYSTEM_PROPERTY));
        if (!enabledSystemPropertyValue.map(Boolean::parseBoolean)
                .orElse(System.getenv(CONTINUOUS_INTEGRATION_ENVIRONMENT) != null)) {
            log.info(
                    "FlakyTestRetryExtension not enabled: " +
                            "%s environment is not detected or " +
                            "system property '%s' is not set to 'true' (actual: %s)",
                    CONTINUOUS_INTEGRATION_ENVIRONMENT,
                    FLAKY_TEST_RETRY_ENABLED_SYSTEM_PROPERTY,
                    enabledSystemPropertyValue.orElse("<not set>"));
            return false;
        }
        return true;
    }

    private Store getStore(ExtensionContext extensionContext)
    {
        return extensionContext.getStore(Namespace.create(getClass(), extensionContext.getRequiredTestMethod()));
    }

    private RetryStateHolder getRetryStateHolder(ExtensionContext extensionContext)
    {
        return Optional.ofNullable(getStore(extensionContext)
                .get(RETRY_STATE_STORE_KEY, RetryStateHolder.class))
                .orElseThrow(() -> new RuntimeException("Unexpected error - could not found flaky retry state"));
    }

    private boolean shouldRetryTest(ExtensionContext extensionContext)
    {
        return getRetryStateHolder(extensionContext).get().shouldRetryTest();
    }

    private static class RetryStateHolder
            extends AtomicReference<FlakyTestRetryState>
    {
        public RetryStateHolder(FlakyTestRetryState initialState)
        {
            super(requireNonNull(initialState, "initialState is null"));
        }
    }
}
