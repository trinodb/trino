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
package io.trino.testing.containers.environment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the ProductTestExtension correctly manages environment lifecycle
 * across multiple test classes.
 * <p>
 * The key requirement: environment must be stored in ROOT context (not class context)
 * so it persists across test classes and is only closed when all tests complete.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestProductTestExtension
{
    private static LauncherDiscoveryRequestBuilder productTestRequest()
    {
        return LauncherDiscoveryRequestBuilder.request()
                .configurationParameter("junit.jupiter.execution.parallel.enabled", "false");
    }

    @Test
    void testEnvironmentSharedAcrossClasses()
    {
        // Reset counters
        ProductTestExtensionFixtures.FakeEnvironment.reset();

        // Run both test classes using JUnit Launcher
        LauncherDiscoveryRequest request = productTestRequest()
                .selectors(
                        DiscoverySelectors.selectClass(ProductTestExtensionFixtures.EnvironmentReuseClassA.class),
                        DiscoverySelectors.selectClass(ProductTestExtensionFixtures.EnvironmentReuseClassB.class))
                .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        // Verify all tests passed
        assertThat(listener.getSummary().getTestsFailedCount())
                .describedAs("All tests should pass")
                .isZero();
        assertThat(listener.getSummary().getTestsSucceededCount())
                .describedAs("Should have run 4 tests (2 per class)")
                .isEqualTo(4);

        // Verify environment lifecycle
        assertThat(ProductTestExtensionFixtures.FakeEnvironment.startCount.get())
                .describedAs("Environment should start exactly once")
                .isEqualTo(1);
        assertThat(ProductTestExtensionFixtures.FakeEnvironment.closeCount.get())
                .describedAs("Environment should close exactly once (at end)")
                .isEqualTo(1);
        assertThat(ProductTestExtensionFixtures.FakeEnvironment.beforeEachCount.get())
                .describedAs("Environment beforeEach hook should run for each test")
                .isEqualTo(4);
        assertThat(ProductTestExtensionFixtures.FakeEnvironment.afterEachCount.get())
                .describedAs("Environment afterEach hook should run for each test")
                .isEqualTo(4);
    }

    @Test
    void testEnvironmentHierarchyValidation()
    {
        // Run a test class with mismatched environment hierarchy
        LauncherDiscoveryRequest request = productTestRequest()
                .selectors(DiscoverySelectors.selectClass(ProductTestExtensionFixtures.MismatchedHierarchyCase.class))
                .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getFailures())
                .describedAs("Should have one failure from hierarchy validation")
                .hasSize(1);

        Throwable failure = listener.getSummary().getFailures().get(0).getException();
        assertThat(failure)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Environment hierarchy mismatch")
                .hasMessageContaining("MismatchedHierarchyCase")
                .hasMessageContaining("UnrelatedEnvironment")
                .hasMessageContaining("FakeEnvironment")
                .hasMessageContaining("must extend");
    }

    @Test
    void testEnvironmentHierarchyValidationWithValidSubclass()
    {
        // Run test class with valid environment hierarchy (subclass extends base)
        LauncherDiscoveryRequest request = productTestRequest()
                .selectors(DiscoverySelectors.selectClass(ProductTestExtensionFixtures.ValidHierarchyCase.class))
                .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        // Verify tests passed (environment hierarchy is valid)
        assertThat(listener.getSummary().getTestsFailedCount())
                .describedAs("All tests should pass with valid hierarchy")
                .isZero();
        assertThat(listener.getSummary().getTestsSucceededCount())
                .describedAs("Should have run 1 test")
                .isEqualTo(1);
    }

    @Test
    void testConcreteProductTestWithoutEnvironmentFailsFast()
    {
        LauncherDiscoveryRequest request = productTestRequest()
                .selectors(DiscoverySelectors.selectClass(ProductTestExtensionFixtures.ConcreteProductCaseWithoutEnvironment.class))
                .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getFailures())
                .describedAs("Concrete @ProductTest class without @RequiresEnvironment must fail")
                .hasSize(1);
        assertThat(listener.getSummary().getFailures().get(0).getException())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must declare @RequiresEnvironment");
    }

    @Test
    void testAbstractProductTestWithoutEnvironmentAllowed()
    {
        ProductTestExtensionFixtures.FakeEnvironment.reset();

        LauncherDiscoveryRequest request = productTestRequest()
                .selectors(DiscoverySelectors.selectClass(ProductTestExtensionFixtures.AbstractProductCaseSubclassWithEnvironment.class))
                .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getTestsFailedCount()).isZero();
        assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(1);
    }

    @Test
    void testEnvironmentHooksWrapTestLifecycle()
    {
        ProductTestExtensionFixtures.HookOrderEnvironment.reset();

        LauncherDiscoveryRequest request = productTestRequest()
                .selectors(DiscoverySelectors.selectClass(ProductTestExtensionFixtures.HookOrderingCase.class))
                .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getTestsFailedCount()).isZero();
        assertThat(ProductTestExtensionFixtures.HookOrderEnvironment.events.toString())
                .isEqualTo("env-before>test-before>test>test-after>env-after>");
    }

    @Test
    void testEnvironmentCleanupFailureIsSuppressedWhenTestFails()
    {
        ProductTestExtensionFixtures.CleanupFailingEnvironment.reset();
        ProductTestExtensionFixtures.CleanupFailingEnvironment.cleanupFailureEnabled = true;

        LauncherDiscoveryRequest request = productTestRequest()
                .selectors(DiscoverySelectors.selectClass(ProductTestExtensionFixtures.CleanupFailureWithPrimaryFailureCase.class))
                .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getTestsFailedCount()).isEqualTo(1);
        Throwable failure = listener.getSummary().getFailures().get(0).getException();
        assertThat(failure)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("primary test failure");
        assertThat(failure.getSuppressed())
                .hasSize(1);
        assertThat(failure.getSuppressed()[0].getMessage())
                .contains("cleanup failure");
    }

    @Test
    void testEnvironmentCleanupFailureFailsTestWhenTestSucceeded()
    {
        ProductTestExtensionFixtures.CleanupFailingEnvironment.reset();
        ProductTestExtensionFixtures.CleanupFailingEnvironment.cleanupFailureEnabled = true;

        LauncherDiscoveryRequest request = productTestRequest()
                .selectors(DiscoverySelectors.selectClass(ProductTestExtensionFixtures.CleanupFailureWithoutPrimaryFailureCase.class))
                .build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        assertThat(listener.getSummary().getTestsFailedCount()).isEqualTo(1);
        Throwable failure = listener.getSummary().getFailures().get(0).getException();
        assertThat(failure)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("cleanup failure");
    }

    @Test
    void testEnvironmentManagerReusesSubclassForSuperclassRequestInStrictMode()
    {
        EnvironmentManager manager = new EnvironmentManager(EnvironmentManager.Mode.STRICT);
        try {
            ProductTestExtensionFixtures.FakeSubEnvironment specific = manager.acquire(ProductTestExtensionFixtures.FakeSubEnvironment.class);
            ProductTestExtensionFixtures.FakeBaseEnvironment generic = manager.acquire(ProductTestExtensionFixtures.FakeBaseEnvironment.class);

            assertThat(generic).isSameAs(specific);
            assertThat(manager.getStats().starts()).isEqualTo(1);
            assertThat(manager.getStats().reuses()).isEqualTo(1);
            assertThat(manager.getStats().switches()).isZero();
        }
        finally {
            manager.close();
        }
    }

    @Test
    void testEnvironmentManagerRejectsSubclassRequestWhenSuperclassIsRunningInStrictMode()
    {
        EnvironmentManager manager = new EnvironmentManager(EnvironmentManager.Mode.STRICT);
        try {
            manager.acquire(ProductTestExtensionFixtures.FakeBaseEnvironment.class);

            assertThatThrownBy(() -> manager.acquire(ProductTestExtensionFixtures.FakeSubEnvironment.class))
                    .isInstanceOf(EnvironmentConflictException.class)
                    .hasMessageContaining("Environment conflict");
        }
        finally {
            manager.close();
        }
    }
}
