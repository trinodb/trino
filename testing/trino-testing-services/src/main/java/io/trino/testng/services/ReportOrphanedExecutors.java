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

import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ITestNGMethod;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static java.lang.String.format;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class ReportOrphanedExecutors
        implements ISuiteListener
{
    @Override
    public void onStart(ISuite suite) {}

    @Override
    public void onFinish(ISuite suite)
    {
        try {
            reportOrphanedExecutors(suite);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportOrphanedExecutors.class,
                    "Failed to process %s [%s]: \n%s",
                    suite,
                    suite.getName(),
                    getStackTraceAsString(e));
        }
    }

    private void reportOrphanedExecutors(ISuite suite)
    {
        Set<?> instances = suite.getAllMethods().stream()
                .map(ITestNGMethod::getInstance)
                .filter(Objects::nonNull)
                .collect(toImmutableSet());

        for (Object instance : instances) {
            reportOrphanedExecutorsOnInstance(instance);
        }
    }

    private void reportOrphanedExecutorsOnInstance(Object instance)
    {
        requireNonNull(instance, "instance is null");

        try {
            for (Class<?> currentClass = instance.getClass(); currentClass != null; currentClass = currentClass.getSuperclass()) {
                Field[] fields = currentClass.getDeclaredFields();
                for (Field field : fields) {
                    if (field.isAnnotationPresent(Suppress.class)) {
                        continue;
                    }

                    field.setAccessible(true);
                    Object value = field.get(instance);
                    if (value instanceof ExecutorService && !((ExecutorService) value).isShutdown()) {
                        throw new RuntimeException(format(
                                "Executor [%s] in [%s] has not been shut down",
                                field,
                                instance));
                    }
                }
            }
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Retention(RUNTIME)
    @Target(FIELD)
    public @interface Suppress
    {
        String because();
    }
}
