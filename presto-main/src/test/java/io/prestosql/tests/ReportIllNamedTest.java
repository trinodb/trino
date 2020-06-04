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
package io.prestosql.tests;

import org.testng.IClassListener;
import org.testng.ITestClass;

import static java.lang.String.format;

public class ReportIllNamedTest
        implements IClassListener
{
    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        String testClassName = testClass.getRealClass().getSimpleName();
        if (testClassName.startsWith("Test") || testClassName.startsWith("Benchmark")) {
            return;
        }
        if (testClassName.endsWith("IT")) {
            // integration test
            return;
        }

        // TestNG may or may not propagate listener's exception as test execution exception.
        // Therefore, instead of throwing, we terminate the JVM.
        System.err.println(format(
                "FATAL: Test class %s's name should start with Test",
                testClass.getRealClass().getName()));
        System.err.println("JVM will be terminated");
        System.exit(1);
    }

    @Override
    public void onAfterClass(ITestClass iTestClass) {}
}
