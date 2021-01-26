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

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.testng.services.Listeners.reportListenerFailure;
import static io.trino.testng.services.ReportUnannotatedMethods.isTemptoClass;

public class ReportIllNamedTest
        implements IClassListener
{
    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        try {
            reportIllNamedTest(testClass);
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportIllNamedTest.class,
                    "Failed to process %s: \n%s",
                    testClass,
                    getStackTraceAsString(e));
        }
    }

    private void reportIllNamedTest(ITestClass testClass)
    {
        Class<?> realClass = testClass.getRealClass();
        String testClassName = realClass.getSimpleName();
        if (testClassName.startsWith("Test") || testClassName.startsWith("Benchmark")) {
            return;
        }
        if (testClassName.endsWith("IT")) {
            // integration test
            return;
        }
        if (isTemptoClass(realClass)) {
            return;
        }

        reportListenerFailure(
                ReportIllNamedTest.class,
                "Test class %s's name should start with Test",
                realClass.getName());
    }

    @Override
    public void onAfterClass(ITestClass iTestClass) {}
}
