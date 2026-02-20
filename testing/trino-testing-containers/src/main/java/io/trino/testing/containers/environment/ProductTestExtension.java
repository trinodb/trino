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
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;

/**
 * JUnit 5 extension that manages product test environment lifecycle.
 * <p>
 * This extension:
 * <ul>
 *   <li>Reads the {@link RequiresEnvironment} annotation to determine which environment a test needs</li>
 *   <li>Acquires the environment via {@link EnvironmentManager} (starting it if needed)</li>
 *   <li>Injects the environment into test methods or constructors via parameter resolution</li>
 *   <li>Ensures only one environment runs at a time (via EnvironmentManager)</li>
 *   <li>Cleans up all environments when the test run completes</li>
 * </ul>
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
 * @ProductTest
 * @RequiresEnvironment(MySqlEnvironment.class)
 * class TestMySqlQueries {
 *
 *     @Test
 *     void testSelect(ProductTestEnvironment env) throws Exception {
 *         try (Connection conn = env.createTrinoConnection();
 *              Statement stmt = conn.createStatement();
 *              ResultSet rs = stmt.executeQuery("SELECT * FROM mysql.test.nation")) {
 *             // ...
 *         }
 *     }
 * }
 * }</pre>
 * <p>
 * The extension stores the {@link EnvironmentManager} in the root extension context,
 * ensuring it persists for the entire test run and is properly closed when complete.
 *
 * @see ProductTest
 * @see RequiresEnvironment
 * @see EnvironmentManager
 */
public class ProductTestExtension
        implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver
{
    private static final Namespace NAMESPACE = Namespace.create(ProductTestExtension.class);
    private static final String ENVIRONMENT_KEY = "environment";

    @Override
    public void beforeAll(ExtensionContext context)
    {
        Class<?> testClass = context.getRequiredTestClass();

        // Find the required environment from the annotation
        Class<? extends ProductTestEnvironment> environmentClass = findRequiredEnvironment(context);
        validateProductTestEnvironmentRequirement(testClass, environmentClass);
        if (environmentClass == null) {
            return;
        }

        // Validate environment hierarchy BEFORE starting anything
        validateEnvironmentHierarchy(testClass, environmentClass);

        // Get or create the EnvironmentManager (stored in root context for cleanup)
        EnvironmentManager manager = getOrCreateManager(context);

        // Acquire the environment (starts it if needed, or reuses existing).
        // In STRICT mode, environment conflicts are surfaced as failures.
        manager.acquire(environmentClass);

        // Store the environment CLASS (not the instance) for parameter resolution.
        // We don't store the environment instance itself because JUnit would try to
        // close it when the class context closes. The EnvironmentManager handles lifecycle.
        context.getStore(NAMESPACE).put(ENVIRONMENT_KEY, environmentClass);
    }

    @Override
    public void beforeEach(ExtensionContext context)
            throws Exception
    {
        ProductTestEnvironment environment = currentEnvironment(context);
        if (environment != null) {
            environment.beforeEachTest();
        }
    }

    @Override
    public void afterEach(ExtensionContext context)
            throws Exception
    {
        ProductTestEnvironment environment = currentEnvironment(context);
        if (environment == null) {
            return;
        }

        Throwable testFailure = context.getExecutionException().orElse(null);
        Throwable environmentCleanupFailure = null;
        try {
            environment.afterEachTest();
        }
        catch (Throwable cleanupFailure) {
            environmentCleanupFailure = cleanupFailure;
        }

        Throwable frameworkCleanupFailure = null;
        try {
            environment.cleanupFrameworkState();
        }
        catch (Throwable cleanupFailure) {
            frameworkCleanupFailure = cleanupFailure;
        }

        if (testFailure != null) {
            if (environmentCleanupFailure != null) {
                testFailure.addSuppressed(environmentCleanupFailure);
            }
            if (frameworkCleanupFailure != null) {
                testFailure.addSuppressed(frameworkCleanupFailure);
            }
            return;
        }

        Throwable failureToThrow = environmentCleanupFailure;
        if (failureToThrow == null) {
            failureToThrow = frameworkCleanupFailure;
        }
        else if (frameworkCleanupFailure != null) {
            failureToThrow.addSuppressed(frameworkCleanupFailure);
        }

        if (failureToThrow == null) {
            return;
        }
        if (failureToThrow instanceof Exception exception) {
            throw exception;
        }
        throw (Error) failureToThrow;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    {
        return ProductTestEnvironment.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    {
        // Get the EnvironmentManager from the root context
        EnvironmentManager manager = extensionContext.getRoot().getStore(NAMESPACE)
                .get(EnvironmentManager.class, EnvironmentManager.class);

        if (manager == null || manager.current() == null) {
            throw new IllegalStateException(
                    "No ProductTestEnvironment available. Ensure the test class is annotated with " +
                    "@RequiresEnvironment(YourEnvironment.class)");
        }

        ProductTestEnvironment currentEnvironment = manager.current();
        Class<?> parameterType = parameterContext.getParameter().getType();

        if (!parameterType.isAssignableFrom(currentEnvironment.getClass())) {
            throw new IllegalStateException(String.format(
                    "Environment type mismatch: test expects %s but manager has %s. " +
                    "This can happen if test classes run in parallel. " +
                    "Add junit-platform.properties with junit.jupiter.execution.parallel.enabled=false " +
                    "to disable parallel execution for product tests.",
                    parameterType.getSimpleName(),
                    currentEnvironment.getClass().getSimpleName()));
        }

        return currentEnvironment;
    }

    private Class<? extends ProductTestEnvironment> findRequiredEnvironment(ExtensionContext context)
    {
        // Look for @RequiresEnvironment on the test class or its hierarchy
        return AnnotationSupport.findAnnotation(context.getRequiredTestClass(), RequiresEnvironment.class)
                .map(RequiresEnvironment::value)
                .orElse(null);
    }

    private EnvironmentManager getOrCreateManager(ExtensionContext context)
    {
        // Use the shared singleton and store reference in ROOT context for cleanup callback.
        // Using shared() ensures Suite runners and this extension use the same manager,
        // preventing duplicate containers when suites pre-start environments.
        return context.getRoot().getStore(NAMESPACE)
                .getOrComputeIfAbsent(EnvironmentManager.class, _ -> EnvironmentManager.shared(), EnvironmentManager.class);
    }

    private ProductTestEnvironment currentEnvironment(ExtensionContext context)
    {
        EnvironmentManager manager = context.getRoot().getStore(NAMESPACE).get(EnvironmentManager.class, EnvironmentManager.class);
        if (manager == null) {
            return null;
        }
        return manager.current();
    }

    private void validateProductTestEnvironmentRequirement(Class<?> testClass, Class<? extends ProductTestEnvironment> environmentClass)
    {
        if (environmentClass != null || Modifier.isAbstract(testClass.getModifiers())) {
            return;
        }

        throw new IllegalStateException(String.format(
                "Concrete @ProductTest class %s must declare @RequiresEnvironment on the class or an abstract superclass.",
                testClass.getName()));
    }

    /**
     * Validates that the @RequiresEnvironment class is assignable to all test method parameter types.
     * This ensures that environment class hierarchies match test class hierarchies when using
     * abstract base test classes with concrete environment subclasses.
     */
    private void validateEnvironmentHierarchy(Class<?> testClass, Class<? extends ProductTestEnvironment> environmentClass)
    {
        for (Method method : getAllTestMethods(testClass)) {
            for (Parameter parameter : method.getParameters()) {
                Class<?> paramType = parameter.getType();

                // Only check ProductTestEnvironment subtypes
                if (ProductTestEnvironment.class.isAssignableFrom(paramType)) {
                    // The required environment must be assignable to the parameter type
                    if (!paramType.isAssignableFrom(environmentClass)) {
                        throw new IllegalStateException(String.format(
                                "Environment hierarchy mismatch in %s: " +
                                "@RequiresEnvironment specifies %s, but method %s expects parameter type %s. " +
                                "%s must extend %s.",
                                testClass.getSimpleName(),
                                environmentClass.getSimpleName(),
                                method.getName(),
                                paramType.getSimpleName(),
                                environmentClass.getSimpleName(),
                                paramType.getSimpleName()));
                    }
                }
            }
        }
    }

    private List<Method> getAllTestMethods(Class<?> testClass)
    {
        List<Method> testMethods = new ArrayList<>();
        Class<?> current = testClass;
        while (current != null && current != Object.class) {
            for (Method method : current.getDeclaredMethods()) {
                if (method.isAnnotationPresent(Test.class) ||
                        method.isAnnotationPresent(ParameterizedTest.class)) {
                    testMethods.add(method);
                }
            }
            current = current.getSuperclass();
        }
        return testMethods;
    }
}
