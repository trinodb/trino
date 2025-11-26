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

import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.testing.services.junit.Listeners.reportListenerFailure;
import static java.lang.String.format;

public class ReportOverriddenMethods
        implements TestExecutionListener
{
    @Override
    public void testPlanExecutionStarted(TestPlan testPlan)
    {
        try {
            testPlan.accept(new TestPlan.Visitor()
            {
                @Override
                public void visit(TestIdentifier testIdentifier)
                {
                    testIdentifier.getSource().ifPresent(source -> {
                        if (source instanceof MethodSource methodSource) {
                            if (!Modifier.isPublic(methodSource.getJavaMethod().getModifiers()) &&
                                    !Modifier.isProtected(methodSource.getJavaMethod().getModifiers())) {
                                List<Class<?>> declaringClasses = Stream.<Class<?>>iterate(methodSource.getJavaClass(), clazz -> clazz.getSuperclass() != null, Class::getSuperclass)
                                        .filter(clazz ->
                                                Arrays.stream(clazz.getDeclaredMethods())
                                                        .anyMatch(method -> method.getName().equals(methodSource.getJavaMethod().getName())))
                                        .toList();
                                if (declaringClasses.size() > 1) {
                                    throw new IllegalStateException(format(
                                            """
                                                    Method %s is not public. Similar methods are defined by %s. \
                                                    When tests are non-public, they do not @Override in Java sense, but they still interact with each other in JUnit, \
                                                    and only one of declared tests gets executed. \
                                                    This leads to tests being silently skipped without any source-level indication.""",
                                            methodSource.getJavaMethod(),
                                            declaringClasses));
                                }
                            }
                        }
                    });
                }
            });
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(getClass(), "%s", getStackTraceAsString(e));
        }
    }
}
