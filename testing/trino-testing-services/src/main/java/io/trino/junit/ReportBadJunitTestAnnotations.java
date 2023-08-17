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
package io.trino.junit;

import com.google.common.annotations.VisibleForTesting;
import io.trino.testng.services.ReportBadTestAnnotations;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;
import org.junit.jupiter.api.extension.TestInstancePreConstructCallback;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.Listeners.reportListenerFailure;
import static java.util.stream.Collectors.joining;

public class ReportBadJunitTestAnnotations
        implements TestInstancePreConstructCallback
{
    @Override
    public void preConstructTestInstance(TestInstanceFactoryContext factoryContext, ExtensionContext context)
    {
        Class<?> testClass = factoryContext.getTestClass();
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

    private void reportBadTestAnnotations(Class<?> testClass)
    {
        List<Method> unannotatedTestMethods = findUnannotatedInheritedTestMethods(testClass);
        if (!unannotatedTestMethods.isEmpty()) {
            reportListenerFailure(
                    ReportBadJunitTestAnnotations.class,
                    "Test class %s has methods which are inherited but not explicitly annotated. Are they missing @Test?%s",
                    testClass.getName(),
                    unannotatedTestMethods.stream()
                            .map(Method::toString)
                            .collect(joining("\n\t\t", "\n\t\t", "")));
        }
    }

    @VisibleForTesting
    static List<Method> findUnannotatedInheritedTestMethods(Class<?> realClass)
    {
        return Arrays.stream(realClass.getMethods())
                .filter(method -> method.getDeclaringClass() != Object.class)
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge())
                .filter(method -> isUnannotated(method) && overriddenMethodHasTestAnnotation(method))
                .collect(toImmutableList());
    }

    private static boolean isUnannotated(Method method)
    {
        return Arrays.stream(method.getAnnotations()).map(Annotation::annotationType)
                .noneMatch(ReportBadJunitTestAnnotations::isJUnitAnnotation);
    }

    private static boolean isJUnitAnnotation(Class<? extends Annotation> clazz)
    {
        return clazz.getPackage().getName().startsWith("org.junit.jupiter.api");
    }

    private static boolean overriddenMethodHasTestAnnotation(Method method)
    {
        if (method.isAnnotationPresent(org.junit.jupiter.api.Test.class)) {
            return true;
        }

        // Skip methods in Object class, e.g. toString()
        if (method.getDeclaringClass() == Object.class) {
            return false;
        }

        // The test class may override the default method of the interface
        for (Class<?> interfaceClass : method.getDeclaringClass().getInterfaces()) {
            Optional<Method> overridden = getOverridden(method, interfaceClass);
            if (overridden.isPresent() && overridden.get().isAnnotationPresent(org.junit.jupiter.api.Test.class)) {
                return true;
            }
        }

        Class<?> superClass = method.getDeclaringClass().getSuperclass();
        if (superClass == null) {
            return false;
        }
        return getOverridden(method, superClass)
                .map(ReportBadJunitTestAnnotations::overriddenMethodHasTestAnnotation)
                .orElse(false);
    }

    private static Optional<Method> getOverridden(Method method, Class<?> base)
    {
        try {
            // Simplistic override detection
            return Optional.of(base.getMethod(method.getName(), method.getParameterTypes()));
        }
        catch (NoSuchMethodException ignored) {
            return Optional.empty();
        }
    }
}
