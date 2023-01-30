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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.testing.ResourcePresence;
import org.intellij.lang.annotations.Language;
import org.testng.ITestClass;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static io.trino.testng.services.ManageTestResources.Stage.AFTER_CLASS;
import static io.trino.testng.services.ManageTestResources.Stage.BEFORE_CLASS;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.lang.reflect.Modifier.isStatic;

public class ManageTestResources
        implements ITestListener
{
    private static final Logger log = Logger.get(ManageTestResources.class);

    private final boolean enabled;
    private final List<Rule> rules;

    public ManageTestResources()
    {
        enabled = isEnabled();
        if (!enabled) {
            log.info("ManageTestResources is disabled!");
            rules = List.of();
            return;
        }

        ImmutableList.Builder<Rule> rules = ImmutableList.builder();

        // E.g. io.trino.testing.QueryRunner, io.trino.sql.query.QueryAssertions
        rules.add(isAutoCloseable());

        isInstanceTransactionManagerField().ifPresent(rules::add);

        // any testcontainers resources
        rules.add(declaredClassPattern("org\\.testcontainers\\..*"));

        // any docker-java API's resources
        rules.add(declaredClassPattern("com\\.github\\.dockerjava\\..*"));

        rules.add(isSomeServer());

        rules.add(isLeftoverExecutorService());

        this.rules = rules.build();
    }

    private static boolean isEnabled()
    {
        if (System.getProperty("ManageTestResources.enabled") != null) {
            return Boolean.getBoolean("ManageTestResources.enabled");
        }
        if (System.getenv("DISABLE_REPORT_RESOURCE_HUNGRY_TESTS_CHECK") != null) {
            return false;
        }
        return true;
    }

    @Override
    public void onStart(ITestContext context)
    {
        try {
            if (!enabled) {
                log.debug("ManageTestResources.onStart ignored, check is disabled");
                return;
            }

            log.info("ManageTestResources.onStart: running checks");
            manageResources(context, BEFORE_CLASS);
        }
        catch (ReflectiveOperationException | Error e) {
            reportListenerFailure(
                    ManageTestResources.class,
                    "Failed to process %s: \n%s",
                    context,
                    getStackTraceAsString(e));
        }
    }

    @Override
    public void onTestStart(ITestResult result) {}

    @Override
    public void onTestSuccess(ITestResult result) {}

    @Override
    public void onTestFailure(ITestResult result) {}

    @Override
    public void onTestSkipped(ITestResult result) {}

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {}

    @Override
    public void onFinish(ITestContext context)
    {
        try {
            if (!enabled) {
                log.debug("ManageTestResources.onFinish ignored, check is disabled");
                return;
            }

            log.info("ManageTestResources.onFinish: running checks");
            manageResources(context, AFTER_CLASS);
        }
        catch (ReflectiveOperationException | Error e) {
            reportListenerFailure(
                    ManageTestResources.class,
                    "Failed to process %s: \n%s",
                    context,
                    getStackTraceAsString(e));
        }
    }

    private void manageResources(ITestContext context, Stage stage)
            throws ReflectiveOperationException
    {
        Set<ITestClass> testClasses = Stream.of(context.getAllTestMethods())
                .map(ITestNGMethod::getTestClass)
                .collect(toImmutableSet());
        for (ITestClass testClass : testClasses) {
            manageResources(testClass, stage);
        }
    }

    private void manageResources(ITestClass testClass, Stage stage)
            throws ReflectiveOperationException
    {
        // This method should not be called when check not enabled.
        checkState(enabled, "Not enabled");

        for (Object instance : testClass.getInstances(false)) {
            for (Class<?> clazz = instance.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
                for (Field field : clazz.getDeclaredFields()) {
                    if (field.isAnnotationPresent(Suppress.class)) {
                        log.debug("Skipping audit of field due to field-level suppression %s: %s", field.getAnnotation(Suppress.class), field);
                        continue;
                    }

                    field.setAccessible(true);
                    Object value = field.get(instance);
                    if (value == null) {
                        continue;
                    }
                    if (value.getClass().isAnnotationPresent(Suppress.class)) {
                        log.debug(
                                "Skipping audit of field due to suppression %s on the field actual type (%s): %s",
                                value.getClass().getAnnotation(Suppress.class),
                                value.getClass(),
                                field);
                        continue;
                    }
                    for (Rule rule : rules) {
                        if (rule.isExpensiveResource(field, value, stage)) {
                            reportListenerFailure(
                                    ManageTestResources.class,
                                    """

                                            \tTest instance field has value that looks like a resource
                                            \t    Test class: %s
                                            \t    Instance: %s
                                            \t    Field: %s
                                            \t    Value: %s
                                            \t    Test execution stage: %s
                                            \t    Matching rule: %s

                                            \tResources must not be allocated in a field initializer or a test constructor,
                                            \tand must be freed when test class is completed.
                                            \tDepending which rule has been violated, if the reported class holds on
                                            \tto resources only conditionally, you can add @ResourcePresence to declare that.
                                            """,
                                    testClass.getRealClass(),
                                    instance,
                                    field,
                                    value,
                                    stage,
                                    rule);
                        }
                    }
                }
            }
        }
    }

    enum Stage
    {
        BEFORE_CLASS,
        AFTER_CLASS,
    }

    @FunctionalInterface
    private interface Rule
    {
        boolean isExpensiveResource(Field field, Object value, Stage stage);
    }

    private static Rule declaredClassPattern(@Language("RegExp") String classNamePattern)
    {
        Pattern pattern = Pattern.compile(classNamePattern);
        return named(
                "declaredClassPattern(%s)".formatted(classNamePattern),
                (field, value, stage) -> pattern.matcher(field.getType().getName()).matches());
    }

    /**
     * Report {@link AutoCloseable} instances.
     */
    private static Rule isAutoCloseable()
    {
        return named("isAutoCloseable", (field, value, stage) -> {
            if (!(value instanceof AutoCloseable)) {
                return false;
            }
            if (value instanceof ExecutorService) {
                // Covered by isLeftoverExecutorService.
                // ExecutorService is AutoCloseable since Java 19.
                return false;
            }

            // Disallow AutoCloseable instances, even if not started, before class is run.
            // Allocation of such resources generally correlates with too eager initialization.
            // Allow stopped instances after class to work nicely with
            // `AbstractTestQueryFramework.closeAfterClass` usages.
            return !(stage == AFTER_CLASS && isResourceStopped(value));
        });
    }

    /**
     * Report anything that looks like a "Server" and does not appear to be stopped after class is finished.
     */
    private static Rule isSomeServer()
    {
        return named("isSomeServer", (field, value, stage) -> {
            Class<?> type = field.getType();
            if (!type.getName().endsWith("Server")) {
                return false;
            }

            // Disallow "*Server" instances, even if not started, before class is run.
            // Allow stopped instances to work nicely with `AbstractTestQueryFramework.closeAfterClass` usages.
            return !(stage == AFTER_CLASS && isResourceStopped(value));
        });
    }

    // see https://github.com/trinodb/trino/commit/9e3b45b743e1cc350e759c1bed3cfb336b1cf610
    private static Optional<Rule> isInstanceTransactionManagerField()
    {
        return tryLoadClass("io.trino.transaction.TransactionManager").map(transactionManagerClass ->
                named(
                        "is-instance-TransactionManager-field",
                        (field, value, stage) -> {
                            // Exclude static fields to allow usages of the form: `static final TransactionManager TRANSACTION_MANAGER = createTestTransactionManager()`
                            if (isStatic(field.getModifiers())) {
                                return false;
                            }
                            return transactionManagerClass.isInstance(value);
                        }));
    }

    private static boolean isResourceStopped(Object resource)
    {
        return Stream.of(resource.getClass().getMethods())
                .filter(method -> method.isAnnotationPresent(ResourcePresence.class))
                .collect(toOptional())
                .map(resourcePresence -> {
                    try {
                        return !(boolean) resourcePresence.invoke(resource);
                    }
                    catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElse(false); // assume not stopped
    }

    /**
     * Report {@link java.util.concurrent.ExecutorService} instances that are left behind.
     * Does not report prematurely initialized instances since typical execuctor service creates
     * threads lazily.
     */
    private static Rule isLeftoverExecutorService()
    {
        return named("isLeftoverExecutorService", (field, value, stage) -> {
            if (!(value instanceof ExecutorService executorService)) {
                return false;
            }

            // TODO we should check for executorService.isTerminated(), i.e. require tests to wait for completion of any background tasks
            return stage == AFTER_CLASS && !executorService.isShutdown();
        });
    }

    private static Rule named(String name, Rule delegate)
    {
        return new Rule()
        {
            @Override
            public boolean isExpensiveResource(Field field, Object value, Stage stage)
            {
                return delegate.isExpensiveResource(field, value, stage);
            }

            @Override
            public String toString()
            {
                return name;
            }
        };
    }

    private static Optional<Class<?>> tryLoadClass(String className)
    {
        try {
            return Optional.of(Thread.currentThread().getContextClassLoader().loadClass(className));
        }
        catch (ClassNotFoundException e) {
            return Optional.empty();
        }
    }

    /**
     * Suppresses checks when
     * <ul>
     *     <li>field is annotated
     *     <li>field actual type is annotated (note that the annotation *IS* inherited)
     * </ul>
     */
    @Retention(RUNTIME)
    @Target({TYPE, FIELD})
    @Inherited
    public @interface Suppress
    {
        String because();
    }
}
