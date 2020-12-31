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
import org.testng.annotations.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * Detects test methods which are annotaded with @Flaky annotation but are
 * missing explicit @Test annotation
 */
public class FlakyAnnotationVerifier
        implements IClassListener
{
    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        try {
            reportMethodsWithFlakyAndNoTestAnnotation(testClass);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    FlakyAnnotationVerifier.class,
                    "Failed to process %s: \n%s",
                    testClass,
                    getStackTraceAsString(e));
        }
    }

    private void reportMethodsWithFlakyAndNoTestAnnotation(ITestClass testClass)
    {
        Class<?> realClass = testClass.getRealClass();

        if (realClass.getSuperclass() != null &&
                "io.trino.tempto.internal.convention.ConventionBasedTestProxyGenerator$ConventionBasedTestProxy".equals(realClass.getSuperclass().getName())) {
            // Ignore tempto generated convention tests.
            return;
        }

        if (realClass.getName().startsWith("io.trino.testng.services.TestFlakyAnnotationVerifier")) {
            // ignore test of FlakyAnnotationVerifier and internal classes
            return;
        }

        List<Method> unannotatedTestMethods = findMethodsWithFlakyAndNoTestAnnotation(realClass);
        if (!unannotatedTestMethods.isEmpty()) {
            reportListenerFailure(
                    FlakyAnnotationVerifier.class,
                    "Test class %s has methods which are marked as @Flaky but are not explicitly annotated with @Test:%s",
                    realClass.getName(),
                    unannotatedTestMethods.stream()
                            .map(Method::toString)
                            .collect(joining("\n\t\t", "\n\t\t", "")));
        }

        verifyFlakyAnnotations(realClass).ifPresent(error -> {
            reportListenerFailure(
                    FlakyAnnotationVerifier.class,
                    "%s",
                    error);
        });
    }

    @VisibleForTesting
    static Optional<String> verifyFlakyAnnotations(Class<?> realClass)
    {
        for (Method method : realClass.getMethods()) {
            Optional<Flaky> flaky = findInheritableAnnotation(method, Flaky.class);
            if (flaky.isEmpty()) {
                continue;
            }
            if (flaky.get().issue().isBlank()) {
                return Optional.of(format("Test method %s has empty @Flaky.issue", method));
            }
            try {
                Pattern.compile(flaky.get().match());
            }
            catch (PatternSyntaxException e) {
                return Optional.of(format("Test method %s has invalid @Flaky.match: %s", method, getStackTraceAsString(e)));
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    static List<Method> findMethodsWithFlakyAndNoTestAnnotation(Class<?> realClass)
    {
        return Arrays.stream(realClass.getMethods())
                .filter(method -> findInheritableAnnotation(method, Flaky.class).isPresent())
                .filter(method -> !method.isAnnotationPresent(Test.class))
                .collect(toImmutableList());
    }

    @Override
    public void onAfterClass(ITestClass testClass) {}

    private static <T extends Annotation> Optional<T> findInheritableAnnotation(Method method, Class<T> annotationClass)
    {
        while (method != null) {
            T annotation = method.getAnnotation(annotationClass);
            if (annotation != null) {
                return Optional.of(annotation);
            }
            method = getSuperMethod(method).orElse(null);
        }
        return Optional.empty();
    }

    private static Optional<Method> getSuperMethod(Method method)
    {
        // Simplistic override detection; this is not correct in presence of generics and bridge methods.
        try {
            Class<?> superclass = method.getDeclaringClass().getSuperclass();
            if (superclass == null) {
                return Optional.empty();
            }
            return Optional.of(superclass.getMethod(method.getName(), method.getParameterTypes()));
        }
        catch (NoSuchMethodException e) {
            return Optional.empty();
        }
    }
}
