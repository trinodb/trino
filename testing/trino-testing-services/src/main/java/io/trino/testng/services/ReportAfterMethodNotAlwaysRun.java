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
import com.google.common.collect.ImmutableSet;
import org.testng.IClassListener;
import org.testng.ITestClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;

public class ReportAfterMethodNotAlwaysRun
        implements IClassListener
{
    private static final Set<AnnotationPredicate<?>> VIOLATIONS = ImmutableSet.of(
            new AnnotationPredicate<>(AfterMethod.class, not(AfterMethod::alwaysRun)),
            new AnnotationPredicate<>(AfterClass.class, not(AfterClass::alwaysRun)),
            new AnnotationPredicate<>(AfterSuite.class, not(AfterSuite::alwaysRun)),
            new AnnotationPredicate<>(AfterGroups.class, not(AfterGroups::alwaysRun)));

    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        try {
            checkHasAfterMethodsNotAlwaysRun(testClass.getRealClass());
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportAfterMethodNotAlwaysRun.class,
                    "Failed to process %s:%n%s",
                    testClass.getName(),
                    getStackTraceAsString(e));
        }
    }

    @VisibleForTesting
    static void checkHasAfterMethodsNotAlwaysRun(Class<?> testClass)
    {
        List<Method> notAlwaysRunMethods = Arrays.stream(testClass.getMethods())
                .filter(ReportAfterMethodNotAlwaysRun::isAfterMethodNotAlwaysRun)
                .collect(toImmutableList());

        if (!notAlwaysRunMethods.isEmpty()) {
            throw new RuntimeException(notAlwaysRunMethods.stream()
                    .map(Method::toString)
                    .sorted()
                    .collect(joining("\n", "The @AfterX methods should have the alwaysRun = true attribute to make sure that they'll run even if tests were skipped:\n", "")));
        }
    }

    private static boolean isAfterMethodNotAlwaysRun(Method method)
    {
        return VIOLATIONS.stream().anyMatch(p -> p.test(method));
    }

    @Override
    public void onAfterClass(ITestClass testClass) {}

    private record AnnotationPredicate<T extends Annotation>(Class<T> annotationType, Predicate<T> predicate)
            implements Predicate<Method>
    {
        AnnotationPredicate
        {
            requireNonNull(annotationType);
            requireNonNull(predicate);
        }

        @Override
        public boolean test(Method method)
        {
            return Optional.ofNullable(method.getAnnotation(annotationType)).filter(predicate).isPresent();
        }
    }
}
