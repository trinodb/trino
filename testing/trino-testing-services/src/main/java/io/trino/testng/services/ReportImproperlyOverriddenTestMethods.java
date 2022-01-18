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
import org.junit.platform.commons.annotation.Testable;
import org.testng.IClassListener;
import org.testng.ITestClass;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.joining;
import static org.junit.platform.commons.util.AnnotationUtils.isAnnotated;

/**
 * This checker finds issues in JUnit 5 tests where the test method is overridden in a subclass but
 * not annotated with {@link org.junit.jupiter.api.Test} or any other {@link Testable}. In addition
 * to presence of the annotation, this check requires that the overridden method is annotated with the
 * same annotation with the same parameters, e.g. a {@code @RepeatedTest(5)} method's override needs
 * to be also annotated with {@link org.junit.jupiter.api.RepeatedTest} with the same number of repeats (5).
 * <p>
 * This is only needed for JUnit 5, as TestNG picks up overridden methods just fine.
 */
public class ReportImproperlyOverriddenTestMethods
        implements IClassListener
{
    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        try {
            reportImproperlyOverriddenTestMethods(testClass);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportImproperlyOverriddenTestMethods.class,
                    "Failed to process %s: \n%s",
                    testClass,
                    getStackTraceAsString(e));
        }
    }

    private void reportImproperlyOverriddenTestMethods(ITestClass testClass)
    {
        Class<?> realClass = testClass.getRealClass();

        if (realClass.getName().startsWith("io.trino.testng.services.TestReportImproperlyOverriddenTestMethods")) {
            // ignore test of ReportImproperlyOverriddenTestMethods and internal classes
            return;
        }

        List<Method> privateTestMethods = findImproperlyOverriddenTestMethods(realClass);
        if (!privateTestMethods.isEmpty()) {
            reportListenerFailure(
                    ReportImproperlyOverriddenTestMethods.class,
                    "The following methods in the test class %s should be annotated with the same test annotations as in the superclass:%s",
                    realClass.getName(),
                    privateTestMethods.stream()
                            .map(Method::toString)
                            .collect(joining("\n\t\t", "\n\t\t", "")));
        }
    }

    @VisibleForTesting
    static List<Method> findImproperlyOverriddenTestMethods(Class<?> realClass)
    {
        return Arrays.stream(realClass.getDeclaredMethods())
                .filter(ReportImproperlyOverriddenTestMethods::isImproperlyAnnotated)
                .filter(method -> !method.isAnnotationPresent(Suppress.class))
                .collect(toImmutableList());
    }

    @Override
    public void onAfterClass(ITestClass testClass) {}

    private static boolean isImproperlyAnnotated(Method method)
    {
        return getOverriddenMethod(method)
                .stream()
                .map(Method::getAnnotations)
                .flatMap(Arrays::stream)
                .anyMatch(overriddenAnnotation -> {
                    // Junit 5 @Test-like annotations are all annotated with @Testable
                    if (isAnnotated(overriddenAnnotation.annotationType(), Testable.class)) {
                        // (There are also associated annotations like @MethodSource, but we can allow changing them in the override)
                        return !overriddenAnnotation.equals(method.getAnnotation(overriddenAnnotation.annotationType()));
                    }
                    return false;
                });
    }

    private static Optional<Method> getOverriddenMethod(Method method)
    {
        // we can't rely on Class::getMethod to return an inherited method
        // because in JUnit 5 test methods are not public, and getMethod only returns public
        // on the other hand Class:getDeclaredMethod does not return inherited methods
        return inheritanceChain(method.getDeclaringClass().getSuperclass())
                .map(Class::getDeclaredMethods)
                .flatMap(Arrays::stream)
                .filter(overriddenMethod -> overriddenMethod.getName().equals(method.getName()))
                .filter(overriddenMethod -> Arrays.equals(overriddenMethod.getTypeParameters(), method.getTypeParameters()))
                .findAny();
    }

    private static Stream<Class<?>> inheritanceChain(Class<?> mostDerived)
    {
        return Stream.iterate(mostDerived, clazz -> clazz.getSuperclass() != null, Class::getSuperclass);
    }

    @Retention(RUNTIME)
    @Target(METHOD)
    public @interface Suppress
    {
    }
}
