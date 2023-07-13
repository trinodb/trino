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

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReportAfterMethodNotAlwaysRun
{
    @Test(dataProvider = "correctCases")
    public void testCorrectCases(Class<?> testClass)
    {
        assertThatNoException().isThrownBy(() -> ReportAfterMethodNotAlwaysRun.checkHasAfterMethodsNotAlwaysRun(testClass));
    }

    @DataProvider
    public static Object[][] correctCases()
    {
        return new Object[][] {
                {NoAfterMethods.class},
                {AllAlwaysRun.class},
                {SubClassWithBaseAlwaysRunNoOverride.class},
                {SubClassAlwaysRunWithBaseAlwaysRun.class},
                {SubClassAlwaysRunWithBaseNotAlwaysRun.class},
                {SubClassAddsAlwaysRunWithBaseAlwaysRun.class},
        };
    }

    @Test(dataProvider = "incorrectCases")
    public void testIncorrectCases(Class<?> testClass, String... failMethods)
    {
        assertThatThrownBy(() -> ReportAfterMethodNotAlwaysRun.checkHasAfterMethodsNotAlwaysRun(testClass))
                .hasMessage(
                        "The @AfterX methods should have the alwaysRun = true attribute to make sure that they'll run even if tests were skipped:%n%s",
                        String.join("\n", failMethods));
    }

    @DataProvider
    public static Object[][] incorrectCases()
            throws NoSuchMethodException
    {
        return new Object[][] {
                {
                        ClassNotAlwaysRun.class,
                        ClassNotAlwaysRun.class.getMethod("afterClass").toString(),
                },
                {
                        GroupNotAlwaysRun.class,
                        GroupNotAlwaysRun.class.getMethod("afterGroup").toString(),
                },
                {
                        MethodNotAlwaysRun.class,
                        MethodNotAlwaysRun.class.getMethod("afterMethod").toString(),
                },
                {
                        SuiteNotAlwaysRun.class,
                        SuiteNotAlwaysRun.class.getMethod("afterSuite").toString(),
                },
                {
                        AllNotAlwaysRunTwice.class,
                        AllNotAlwaysRunTwice.class.getMethod("afterClass1").toString(),
                        AllNotAlwaysRunTwice.class.getMethod("afterClass2").toString(),
                        AllNotAlwaysRunTwice.class.getMethod("afterGroup1").toString(),
                        AllNotAlwaysRunTwice.class.getMethod("afterGroup2").toString(),
                        AllNotAlwaysRunTwice.class.getMethod("afterMethod1").toString(),
                        AllNotAlwaysRunTwice.class.getMethod("afterMethod2").toString(),
                        AllNotAlwaysRunTwice.class.getMethod("afterSuite1").toString(),
                        AllNotAlwaysRunTwice.class.getMethod("afterSuite2").toString(),
                },
                {
                        SubClassWithBaseNotAlwaysRunNoOverride.class,
                        SubClassWithBaseNotAlwaysRunNoOverride.class.getMethod("afterMethod").toString(),
                },
                {
                        SubClassNotAlwaysRunWithBaseAlwaysRun.class,
                        SubClassNotAlwaysRunWithBaseAlwaysRun.class.getMethod("afterMethod").toString(),
                },
                {
                        SubClassNotAlwaysRunWithBaseNotAlwaysRun.class,
                        SubClassNotAlwaysRunWithBaseNotAlwaysRun.class.getMethod("afterMethod").toString(),
                },
                {
                        SubClassAddsNotAlwaysRunWithBaseAlwaysRun.class,
                        SubClassAddsNotAlwaysRunWithBaseAlwaysRun.class.getMethod("afterMethodSub").toString(),
                },
                {
                        SubClassAddsAlwaysRunWithBaseNotAlwaysRun.class,
                        SubClassAddsAlwaysRunWithBaseNotAlwaysRun.class.getMethod("afterMethod").toString(),
                },
                {
                        SubClassAddsNotAlwaysRunWithBaseNotAlwaysRun.class,
                        SubClassAddsNotAlwaysRunWithBaseNotAlwaysRun.class.getMethod("afterMethod").toString(),
                        SubClassAddsNotAlwaysRunWithBaseNotAlwaysRun.class.getMethod("afterMethodSub").toString(),
                },
        };
    }

    private static class NoAfterMethods
    {
    }

    private static class AllAlwaysRun
    {
        @AfterClass(alwaysRun = true)
        public void afterClass() {}

        @AfterGroups(alwaysRun = true)
        public void afterGroup() {}

        @AfterMethod(alwaysRun = true)
        public void afterMethod() {}

        @AfterSuite(alwaysRun = true)
        public void afterSuite() {}
    }

    private static class ClassNotAlwaysRun
    {
        @AfterClass
        public void afterClass() {}

        @AfterGroups(alwaysRun = true)
        public void afterGroup() {}

        @AfterMethod(alwaysRun = true)
        public void afterMethod() {}

        @AfterSuite(alwaysRun = true)
        public void afterSuite() {}
    }

    private static class GroupNotAlwaysRun
    {
        @AfterClass(alwaysRun = true)
        public void afterClass() {}

        @AfterGroups
        public void afterGroup() {}

        @AfterMethod(alwaysRun = true)
        public void afterMethod() {}

        @AfterSuite(alwaysRun = true)
        public void afterSuite() {}
    }

    private static class MethodNotAlwaysRun
    {
        @AfterClass(alwaysRun = true)
        public void afterClass() {}

        @AfterGroups(alwaysRun = true)
        public void afterGroup() {}

        @AfterMethod
        public void afterMethod() {}

        @AfterSuite(alwaysRun = true)
        public void afterSuite() {}
    }

    private static class SuiteNotAlwaysRun
    {
        @AfterClass(alwaysRun = true)
        public void afterClass() {}

        @AfterGroups(alwaysRun = true)
        public void afterGroup() {}

        @AfterMethod(alwaysRun = true)
        public void afterMethod() {}

        @AfterSuite
        public void afterSuite() {}
    }

    private static class AllNotAlwaysRunTwice
    {
        @AfterClass
        public void afterClass1() {}

        @AfterClass
        public void afterClass2() {}

        @AfterGroups
        public void afterGroup1() {}

        @AfterGroups
        public void afterGroup2() {}

        @AfterMethod
        public void afterMethod1() {}

        @AfterMethod
        public void afterMethod2() {}

        @AfterSuite
        public void afterSuite1() {}

        @AfterSuite
        public void afterSuite2() {}
    }

    private abstract static class BaseClassAlwaysRun
    {
        @AfterMethod(alwaysRun = true)
        public void afterMethod() {}
    }

    private abstract static class BaseClassNotAlwaysRun
    {
        @AfterMethod
        public void afterMethod() {}
    }

    private static class SubClassWithBaseAlwaysRunNoOverride
            extends BaseClassAlwaysRun
    {
    }

    private static class SubClassWithBaseNotAlwaysRunNoOverride
            extends BaseClassNotAlwaysRun
    {
    }

    private static class SubClassAlwaysRunWithBaseAlwaysRun
            extends BaseClassAlwaysRun
    {
        @SuppressWarnings("RedundantMethodOverride")
        @Override
        @AfterMethod(alwaysRun = true)
        public void afterMethod() {}
    }

    private static class SubClassNotAlwaysRunWithBaseAlwaysRun
            extends BaseClassAlwaysRun
    {
        @Override
        @AfterMethod
        public void afterMethod() {}
    }

    private static class SubClassAlwaysRunWithBaseNotAlwaysRun
            extends BaseClassNotAlwaysRun
    {
        @Override
        @AfterMethod(alwaysRun = true)
        public void afterMethod() {}
    }

    private static class SubClassNotAlwaysRunWithBaseNotAlwaysRun
            extends BaseClassNotAlwaysRun
    {
        @SuppressWarnings("RedundantMethodOverride")
        @Override
        @AfterMethod
        public void afterMethod() {}
    }

    private static class SubClassAddsAlwaysRunWithBaseAlwaysRun
            extends BaseClassAlwaysRun
    {
        @AfterMethod(alwaysRun = true)
        public void afterMethodSub() {}
    }

    private static class SubClassAddsNotAlwaysRunWithBaseAlwaysRun
            extends BaseClassAlwaysRun
    {
        @AfterMethod
        public void afterMethodSub() {}
    }

    private static class SubClassAddsAlwaysRunWithBaseNotAlwaysRun
            extends BaseClassNotAlwaysRun
    {
        @AfterMethod(alwaysRun = true)
        public void afterMethodSub() {}
    }

    private static class SubClassAddsNotAlwaysRunWithBaseNotAlwaysRun
            extends BaseClassNotAlwaysRun
    {
        @AfterMethod
        public void afterMethodSub() {}
    }
}
