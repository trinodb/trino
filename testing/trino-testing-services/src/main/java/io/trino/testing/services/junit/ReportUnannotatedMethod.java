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
package io.trino.testing.services.junit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;
import org.junit.jupiter.api.extension.TestInstancePreConstructCallback;
import org.junit.platform.commons.annotation.Testable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;

/**
 * Fails a test class that has public or package-private methods JUnit does not invoke.
 * Such a method is usually a test missing {@code @Test}, which JUnit silently does not run.
 * Helpers should be {@code private}, or {@code protected} when shared with subclasses.
 */
public class ReportUnannotatedMethod
        implements TestInstancePreConstructCallback
{
    private static final List<Class<? extends Annotation>> LIFECYCLE_ANNOTATIONS = List.of(BeforeAll.class, BeforeEach.class, AfterEach.class, AfterAll.class);

    // Not a compile-time dependency
    private static final Set<String> PARAMETERIZED_CLASS_LIFECYCLE_ANNOTATIONS = Set.of(
            "org.junit.jupiter.params.BeforeParameterizedClassInvocation",
            "org.junit.jupiter.params.AfterParameterizedClassInvocation");

    @Override
    public void preConstructTestInstance(TestInstanceFactoryContext factoryContext, ExtensionContext extensionContext)
    {
        reportUnannotatedMethods(factoryContext.getTestClass());
    }

    // visible for testing
    static void reportUnannotatedMethods(Class<?> testClass)
    {
        List<Method> unannotated = Stream.concat(Arrays.stream(testClass.getMethods()), packagePrivateMethods(testClass))
                .filter(method -> method.getDeclaringClass() != Object.class)
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge())
                .filter(method -> !method.isSynthetic())
                .filter(method -> !isInvokedByJUnit(method))
                .filter(method -> !implementsContract(testClass, method))
                .toList();

        if (!unannotated.isEmpty()) {
            throw new IllegalStateException("Test class %s has methods which JUnit does not invoke. Are they missing @Test? If not, make them private or protected.%s".formatted(
                    testClass.getName(),
                    unannotated.stream()
                            .map(Method::toString)
                            .collect(joining("\n\t", "\n\t", ""))));
        }
    }

    private static Stream<Method> packagePrivateMethods(Class<?> testClass)
    {
        List<Method> methods = new ArrayList<>();
        for (Class<?> clazz = testClass; clazz != null && clazz != Object.class; clazz = clazz.getSuperclass()) {
            for (Method method : clazz.getDeclaredMethods()) {
                if ((method.getModifiers() & (Modifier.PUBLIC | Modifier.PROTECTED | Modifier.PRIVATE)) == 0) {
                    methods.add(method);
                }
            }
        }
        return methods.stream();
    }

    private static boolean isInvokedByJUnit(Method method)
    {
        // @Test, @ParameterizedTest, @RepeatedTest, @TestFactory, @TestTemplate, and annotations composed of them
        if (isAnnotated(method, Testable.class)) {
            return true;
        }
        if (LIFECYCLE_ANNOTATIONS.stream().anyMatch(annotation -> isAnnotated(method, annotation))) {
            return true;
        }
        return Arrays.stream(method.getAnnotations())
                .map(annotation -> annotation.annotationType().getName())
                // JMH annotations, for tests verifying benchmarks
                .anyMatch(name -> PARAMETERIZED_CLASS_LIFECYCLE_ANNOTATIONS.contains(name) || name.startsWith("org.openjdk.jmh.annotations."));
    }

    /**
     * Whether the method implements an interface or {@link Object} method, which has to be public.
     * A method overriding an interface test method is still reported, as JUnit does not inherit the annotation.
     */
    private static boolean implementsContract(Class<?> testClass, Method method)
    {
        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }
        Class<?> declaringClass = method.getDeclaringClass();
        if (declaringClass.isInterface()) {
            // A default method of a test interface is subject to the same rules as one in a test class
            return Arrays.stream(declaringClass.getDeclaredMethods()).noneMatch(ReportUnannotatedMethod::isInvokedByJUnit);
        }
        return Stream.concat(Stream.of(Object.class), allInterfaces(testClass).stream())
                .flatMap(type -> Arrays.stream(type.getMethods()))
                .filter(declaration -> isOverriddenBy(declaration, method))
                .anyMatch(declaration -> !isInvokedByJUnit(declaration));
    }

    private static boolean isOverriddenBy(Method declaration, Method method)
    {
        if (!declaration.getName().equals(method.getName()) || declaration.getParameterCount() != method.getParameterCount()) {
            return false;
        }
        // Assignable rather than equal, as a generic interface method is declared with erased parameter types
        Class<?>[] declaredTypes = declaration.getParameterTypes();
        Class<?>[] types = method.getParameterTypes();
        for (int i = 0; i < types.length; i++) {
            if (!declaredTypes[i].isAssignableFrom(types[i])) {
                return false;
            }
        }
        return true;
    }

    private static List<Class<?>> allInterfaces(Class<?> testClass)
    {
        List<Class<?>> interfaces = new ArrayList<>();
        for (Class<?> clazz = testClass; clazz != null; clazz = clazz.getSuperclass()) {
            interfaces.addAll(Arrays.asList(clazz.getInterfaces()));
        }
        return interfaces;
    }
}
