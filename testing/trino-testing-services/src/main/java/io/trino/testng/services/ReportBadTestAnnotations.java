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
import java.util.Optional;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.deepEquals;
import static java.util.stream.Collectors.joining;

public class ReportBadTestAnnotations
        implements IClassListener
{
    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        try {
            reportBadTestAnnotations(testClass);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportBadTestAnnotations.class,
                    "Failed to process %s: \n%s",
                    testClass,
                    getStackTraceAsString(e));
        }
    }

    private void reportBadTestAnnotations(ITestClass testClass)
    {
        Class<?> realClass = testClass.getRealClass();

        if (realClass.getSuperclass() != null &&
                "io.trino.tempto.internal.convention.ConventionBasedTestProxyGenerator$ConventionBasedTestProxy".equals(realClass.getSuperclass().getName())) {
            // Ignore tempto generated convention tests.
            return;
        }

        List<Method> unannotatedTestMethods = findUnannotatedTestMethods(realClass);
        if (!unannotatedTestMethods.isEmpty()) {
            reportListenerFailure(
                    ReportBadTestAnnotations.class,
                    "Test class %s has methods which are public but not explicitly annotated. Are they missing @Test?%s",
                    realClass.getName(),
                    unannotatedTestMethods.stream()
                            .map(Method::toString)
                            .collect(joining("\n\t\t", "\n\t\t", "")));
        }

        if (!realClass.isAnnotationPresent(Suppress.class)) {
            Optional<Class<?>> clazz = classWithMeaninglessTestAnnotation(realClass);
            if (clazz.isPresent()) {
                reportListenerFailure(
                        ReportBadTestAnnotations.class,
                        "Test class %s (%s) has meaningless class-level @Test annotation. We require each test method be explicitly " +
                                "annotated, deliberately not leveraging https://testng.org/doc/documentation-main.html#class-level.",
                        clazz.get().getName(),
                        realClass.getName());
            }
        }
    }

    @VisibleForTesting
    static Optional<Class<?>> classWithMeaninglessTestAnnotation(Class<?> realClass)
    {
        for (Class<?> clazz = realClass; clazz != null; clazz = clazz.getSuperclass()) {
            org.testng.annotations.Test testAnnotation = clazz.getAnnotation(org.testng.annotations.Test.class);
            if (testAnnotation != null && isAllDefaults(testAnnotation)) {
                return Optional.of(clazz);
            }
        }
        return Optional.empty();
    }

    private static boolean isAllDefaults(org.testng.annotations.Test annotationInstance)
    {
        try {
            for (Method method : org.testng.annotations.Test.class.getDeclaredMethods()) {
                if (Modifier.isStatic(method.getModifiers())) {
                    continue;
                }

                Object value = method.invoke(annotationInstance);
                Object defaultValue = method.getDefaultValue();
                if (!deepEquals(value, defaultValue)) {
                    return false;
                }
            }

            return true;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static List<Method> findUnannotatedTestMethods(Class<?> realClass)
    {
        return Arrays.stream(realClass.getMethods())
                .filter(method -> method.getDeclaringClass() != Object.class)
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge())
                .filter(method -> !isAllowedPublicMethodInTest(method))
                .collect(toImmutableList());
    }

    @Override
    public void onAfterClass(ITestClass testClass) {}

    /**
     * Is explicitly annotated as @Test, @BeforeMethod, @DataProvider, or any method that implements Tempto SPI
     */
    private static boolean isAllowedPublicMethodInTest(Method method)
    {
        if (isTestAnnotated(method)) {
            return true;
        }

        if (method.getDeclaringClass() == Object.class) {
            return true;
        }

        if (method.getDeclaringClass().isInterface()) {
            return isTemptoClass(method.getDeclaringClass());
        }

        for (Class<?> interfaceClass : method.getDeclaringClass().getInterfaces()) {
            Optional<Method> overridden = getOverridden(method, interfaceClass);
            if (overridden.isPresent() && isTemptoClass(interfaceClass)) {
                return true;
            }
        }

        return getOverridden(method, method.getDeclaringClass().getSuperclass())
                .map(ReportBadTestAnnotations::isAllowedPublicMethodInTest)
                .orElse(false);
    }

    private static Optional<Method> getOverridden(Method method, Class<?> base)
    {
        try {
            // Simplistic override detection
            return Optional.of(base.getMethod(method.getName(), method.getParameterTypes()));
        }
        catch (NoSuchMethodException _) {
            return Optional.empty();
        }
    }

    private static boolean isTestAnnotated(Method method)
    {
        return Arrays.stream(method.getAnnotations())
                .map(Annotation::annotationType)
                .anyMatch(annotationClass -> {
                    if (Suppress.class.equals(annotationClass)) {
                        return true;
                    }
                    if ("org.openjdk.jmh.annotations.Benchmark".equals(annotationClass.getName())) {
                        return true;
                    }
                    if (org.testng.annotations.Test.class.getPackage().equals(annotationClass.getPackage())) {
                        // testng annotation (@Test, @Before*, @DataProvider, etc.)
                        return true;
                    }
                    if (isJUnitAnnotation(annotationClass)) {
                        // allowed so that we can transition tests gradually to JUnit
                        return true;
                    }
                    if (isTemptoClass(annotationClass)) {
                        // tempto annotation (@BeforeMethodWithContext, @AfterMethodWithContext)
                        return true;
                    }
                    return false;
                });
    }

    private static boolean isJUnitAnnotation(Class<? extends Annotation> clazz)
    {
        return clazz.getPackage().getName().startsWith("org.junit.jupiter.");
    }

    @VisibleForTesting
    static boolean isTemptoClass(Class<?> aClass)
    {
        String temptoPackage = "io.trino.tempto";
        String aPackage = aClass.getPackage().getName();
        return aPackage.equals(temptoPackage) || aPackage.startsWith(temptoPackage + ".");
    }

    @Retention(RUNTIME)
    @Target({TYPE, METHOD})
    public @interface Suppress
    {
    }
}
