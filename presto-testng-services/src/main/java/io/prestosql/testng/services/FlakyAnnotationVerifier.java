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
package io.prestosql.testng.services;

import com.google.common.annotations.VisibleForTesting;
import org.testng.IClassListener;
import org.testng.ITestClass;
import org.testng.annotations.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.testng.services.Listeners.reportListenerFailure;
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
                "io.prestosql.tempto.internal.convention.ConventionBasedTestProxyGenerator$ConventionBasedTestProxy".equals(realClass.getSuperclass().getName())) {
            // Ignore tempto generated convention tests.
            return;
        }

        if (realClass.getName().startsWith("io.prestosql.testng.services.TestFlakyAnnotationVerifier")) {
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
    }

    @VisibleForTesting
    static List<Method> findMethodsWithFlakyAndNoTestAnnotation(Class<?> realClass)
    {
        return Arrays.stream(realClass.getMethods())
                .filter(method -> hasOrInheritsAnnotation(method, Flaky.class))
                .filter(method -> !hasAnnotation(method, Test.class))
                .collect(toImmutableList());
    }

    @Override
    public void onAfterClass(ITestClass testClass) {}

    private static boolean hasAnnotation(Method method, Class<? extends Annotation> annotationClass)
    {
        return method.getAnnotation(annotationClass) != null;
    }

    private static boolean hasOrInheritsAnnotation(Method method, Class<? extends Annotation> annotationClass)
    {
        Optional<Method> currentMethod = Optional.of(method);
        while (currentMethod.isPresent()) {
            if (hasAnnotation(currentMethod.get(), annotationClass)) {
                return true;
            }
            currentMethod = getSuperMethod(currentMethod.get());
        }
        return false;
    }

    private static Optional<Method> getSuperMethod(Method method)
    {
        Class<?> declaringClass = method.getDeclaringClass().getSuperclass();
        while (declaringClass != null) {
            try {
                return Optional.of(declaringClass.getDeclaredMethod(method.getName(), method.getParameterTypes()));
            }
            catch (NoSuchMethodException e) {
                declaringClass = declaringClass.getSuperclass();
            }
        }
        return Optional.empty();
    }
}
