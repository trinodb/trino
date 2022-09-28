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
import org.testng.IClassListener;
import org.testng.ITestClass;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.joining;

public class ReportPrivateMethods
        implements IClassListener
{
    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        try {
            reportPrivateTestMethods(testClass);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportPrivateMethods.class,
                    "Failed to process %s: \n%s",
                    testClass,
                    getStackTraceAsString(e));
        }
    }

    private void reportPrivateTestMethods(ITestClass testClass)
    {
        Class<?> realClass = testClass.getRealClass();

        List<Method> privateTestMethods = findPrivateTestMethods(realClass);
        if (!privateTestMethods.isEmpty()) {
            reportListenerFailure(
                    ReportPrivateMethods.class,
                    "By convention the following methods in the test class %s should be public:%s",
                    realClass.getName(),
                    privateTestMethods.stream()
                            .map(Method::toString)
                            .collect(joining("\n\t\t", "\n\t\t", "")));
        }
    }

    @VisibleForTesting
    static List<Method> findPrivateTestMethods(Class<?> realClass)
    {
        return inheritanceChain(realClass)
                .flatMap(clazz -> Stream.of(clazz.getDeclaredMethods()))
                .filter(method -> !Modifier.isPublic(method.getModifiers()))
                .filter(method -> !method.isBridge())
                .filter(ReportPrivateMethods::isTestAnnotated)
                .filter(method -> !method.isAnnotationPresent(Suppress.class))
                .collect(toImmutableList());
    }

    @Override
    public void onAfterClass(ITestClass testClass) {}

    private static Stream<Class<?>> inheritanceChain(Class<?> mostDerived)
    {
        return Stream.iterate(mostDerived, clazz -> clazz.getSuperclass() != null, Class::getSuperclass);
    }

    private static boolean isTestAnnotated(Method method)
    {
        return Arrays.stream(method.getAnnotations())
                .map(Annotation::annotationType)
                .anyMatch(annotationClass -> {
                    if (org.testng.annotations.Test.class.getPackage().equals(annotationClass.getPackage())) {
                        // testng annotation (@Test, @Before*, @DataProvider, etc.)
                        return true;
                    }
                    if ("io.trino.tempto".equals(annotationClass.getPackage().getName())) {
                        // tempto annotation (@BeforeTestWithContext, @AfterTestWithContext)
                        return true;
                    }
                    return false;
                });
    }

    @Retention(RUNTIME)
    @Target(METHOD)
    public @interface Suppress
    {
    }
}
