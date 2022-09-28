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

import org.testng.IClassListener;
import org.testng.ITestClass;

import java.util.Optional;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.testng.services.Listeners.reportListenerFailure;

/**
 * Detects test classes which are defined as inner classes
 * TestNG support is poor for these inner test classes: https://github.com/trinodb/trino/pull/11185
 */
public class ReportInnerTestClasses
        implements IClassListener
{
    @Override
    public void onBeforeClass(ITestClass iTestClass)
    {
        try {
            reportInnerTestClasses(iTestClass);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportInnerTestClasses.class,
                    "Failed to process %s: \n%s",
                    iTestClass,
                    getStackTraceAsString(e));
        }
    }

    private void reportInnerTestClasses(ITestClass testClass)
    {
        Class<?> realClass = testClass.getRealClass();

        if (realClass.getName().startsWith("io.trino.testng.services")) {
            // ignore internal testcases
            return;
        }

        Optional<Class<?>> maybeEnclosingClass = Optional.ofNullable(realClass.getEnclosingClass());
        maybeEnclosingClass.ifPresent(enclosingClass ->
                reportListenerFailure(ReportInnerTestClasses.class,
                        "Test class %s is defined as an inner class, has an enclosing class %s",
                        realClass.getName(),
                        enclosingClass.getName()));
    }

    @Override
    public void onAfterClass(ITestClass iTestClass)
    {
    }
}
