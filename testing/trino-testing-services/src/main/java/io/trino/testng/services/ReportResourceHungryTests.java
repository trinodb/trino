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
import io.trino.testing.ResourcePresence;
import org.intellij.lang.annotations.Language;
import org.testng.ITestClass;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static io.trino.testng.services.ReportResourceHungryTests.Stage.AFTER_CLASS;
import static io.trino.testng.services.ReportResourceHungryTests.Stage.BEFORE_CLASS;

public class ReportResourceHungryTests
        implements ITestListener
{
    private final List<Rule> rules;

    public ReportResourceHungryTests()
    {
        ImmutableList.Builder<Rule> rules = ImmutableList.builder();

        isInstance("io.trino.cli.QueryRunner").ifPresent(rules::add);

        // see https://github.com/trinodb/trino/commit/9e3b45b743e1cc350e759c1bed3cfb336b1cf610
        isInstance("io.trino.transaction.TransactionManager").ifPresent(rules::add);

        // any testcontainers resources
        rules.add(declaredClassPattern("org\\.testcontainers\\..*"));

        // any docker-java API's resources
        rules.add(declaredClassPattern("com\\.github\\.dockerjava\\..*"));

        // anything that looks like a "Server"
        rules.add(ReportResourceHungryTests::isRunningServer);

        this.rules = rules.build();
    }

    @Override
    public void onStart(ITestContext context)
    {
        try {
            reportResources(context, BEFORE_CLASS);
        }
        catch (ReflectiveOperationException | Error e) {
            reportListenerFailure(
                    ReportResourceHungryTests.class,
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
            reportResources(context, AFTER_CLASS);
        }
        catch (ReflectiveOperationException | Error e) {
            reportListenerFailure(
                    ReportResourceHungryTests.class,
                    "Failed to process %s: \n%s",
                    context,
                    getStackTraceAsString(e));
        }
    }

    private void reportResources(ITestContext context, Stage stage)
            throws ReflectiveOperationException
    {
        for (ITestNGMethod testMethod : context.getAllTestMethods()) {
            reportResources(testMethod.getTestClass(), stage);
        }
    }

    private void reportResources(ITestClass testClass, Stage stage)
            throws ReflectiveOperationException
    {
        for (Object instance : testClass.getInstances(false)) {
            for (Class<?> clazz = instance.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
                for (Field field : clazz.getDeclaredFields()) {
                    field.setAccessible(true);
                    Object value = field.get(instance);
                    if (value == null) {
                        continue;
                    }
                    for (Rule rule : rules) {
                        if (rule.isExpensiveResource(field, value, stage)) {
                            reportListenerFailure(
                                    ReportResourceHungryTests.class,
                                    "Field %s of %s [%s] is set (to %s) before class is started, or after class is finished. \n" +
                                            "Resources must not be allocated in field initializer or test constructor, and must be freed when test class is completed. \n" +
                                            "Depending which rule has been violated, if the reported class holds on to resources only conditionally, you can add @ResourcePresence to declare that.",
                                    field,
                                    instance,
                                    testClass.getRealClass(),
                                    value);
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

    private static Optional<Rule> isInstance(String className)
    {
        return tryLoadClass(className)
                .map(clazz -> (field, value, stage) -> clazz.isInstance(value));
    }

    private static Rule declaredClassPattern(@Language("RegExp") String classNamePattern)
    {
        Pattern pattern = Pattern.compile(classNamePattern);
        return (field, value, stage) -> pattern.matcher(field.getType().getName()).matches();
    }

    /**
     * Report anything that looks like a "Server" and does not appear to be stopped after class is finished.
     */
    private static boolean isRunningServer(Field field, Object value, Stage stage)
    {
        if (!field.getType().getName().endsWith("Server")) {
            return false;
        }

        boolean resourceStopped = Stream.of(field.getType().getMethods())
                .filter(method -> method.isAnnotationPresent(ResourcePresence.class))
                .collect(toOptional())
                .map(resourcePresence -> {
                    try {
                        return !(boolean) resourcePresence.invoke(value);
                    }
                    catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElse(false); // assume not stopped

        // Disallow "*Server" instances, even if not started, before class is run.
        // Allow stopped instances to be work nicely with `AbstractTestQueryFramework.closeAfterClass` usages.
        return !(stage == AFTER_CLASS && resourceStopped);
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
}
