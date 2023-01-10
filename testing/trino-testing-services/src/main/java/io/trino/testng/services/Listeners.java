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

import com.google.common.base.Joiner;
import com.google.errorprone.annotations.FormatMethod;
import org.testng.ITestClass;
import org.testng.ITestNGListener;
import org.testng.ITestResult;

import static java.lang.String.format;

final class Listeners
{
    private Listeners() {}

    /**
     * Print error to standard error and exit JVM.
     *
     * @apiNote A TestNG listener cannot throw an exception, as this are not currently properly handled by TestNG.
     */
    @FormatMethod
    public static void reportListenerFailure(Class<? extends ITestNGListener> listenerClass, String format, Object... args)
    {
        System.err.println(format("FATAL: %s: ", listenerClass.getName()) + format(format, args));
        System.err.println("JVM will be terminated");

        // TestNG may or may not propagate listener's exception as test execution exception.
        // Therefore, instead of throwing, we terminate the JVM.
        System.exit(1);
    }

    public static String formatTestName(ITestClass testClass)
    {
        return testClass.getName();
    }

    public static String formatTestName(ITestResult testCase)
    {
        // See LogTestDurationListener.getName
        return format("%s.%s%s", testCase.getTestClass().getName(), testCase.getName(), formatTestParameters(testCase));
    }

    private static String formatTestParameters(ITestResult testCase)
    {
        Object[] parameters = testCase.getParameters();
        if (parameters == null || parameters.length == 0) {
            return "";
        }
        return format(" [%s]", Joiner.on(", ").useForNull("null").join(parameters));
    }
}
