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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static java.util.stream.Collectors.joining;

public class ReportBannedAnnotationUsage
        implements IClassListener
{
    private static final Set<Class<? extends Annotation>> BANNED_ANNOTATIONS = ImmutableSet.of(BeforeTest.class, AfterTest.class);

    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        try {
            checkNoBannedAnnotationsAreUsed(testClass.getRealClass());
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportBannedAnnotationUsage.class,
                    "Failed to process %s:%n%s",
                    testClass.getName(),
                    getStackTraceAsString(e));
        }
    }

    @VisibleForTesting
    static void checkNoBannedAnnotationsAreUsed(Class<?> testClass)
    {
        List<Method> notAlwaysRunMethods = Arrays.stream(testClass.getMethods())
                .filter(ReportBannedAnnotationUsage::isUsingBannedAnnotation)
                .collect(toImmutableList());

        if (!notAlwaysRunMethods.isEmpty()) {
            throw new RuntimeException(notAlwaysRunMethods.stream()
                    .map(Method::toString)
                    .sorted()
                    .collect(joining("\n", "Annotations @BeforeTest and @AfterTests are banned. Use @BeforeClass and @AfterClass instead:\n", "")));
        }
    }

    private static boolean isUsingBannedAnnotation(Method method)
    {
        return BANNED_ANNOTATIONS.stream().anyMatch(p -> method.getAnnotation(p) != null);
    }

    @Override
    public void onAfterClass(ITestClass testClass) {}
}
