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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReportBannedAnnotationUsage
{
    @Test(dataProvider = "correctCases")
    public void testCorrectCases(Class<?> testClass)
    {
        assertThatNoException().isThrownBy(() -> ReportBannedAnnotationUsage.checkNoBannedAnnotationsAreUsed(testClass));
    }

    @DataProvider
    public static Object[][] correctCases()
    {
        return new Object[][] {
                {NoMethods.class},
                {NonBannedAnnotations.class},
                {SubClassWithNonBannedAnnotations.class},
        };
    }

    @Test(dataProvider = "incorrectCases")
    public void testIncorrectCases(Class<?> testClass, String... failMethods)
    {
        assertThatThrownBy(() -> ReportBannedAnnotationUsage.checkNoBannedAnnotationsAreUsed(testClass))
                .hasMessage(
                        "Annotations @BeforeTest and @AfterTests are banned. Use @BeforeClass and @AfterClass instead:%n%s",
                        String.join("\n", failMethods));
    }

    @DataProvider
    public static Object[][] incorrectCases()
            throws NoSuchMethodException
    {
        return new Object[][] {
                {
                        BannedBeforeTestAnnotation.class,
                        BannedBeforeTestAnnotation.class.getMethod("beforeTest").toString(),
                },
                {
                        BannedAfterTestAnnotation.class,
                        BannedAfterTestAnnotation.class.getMethod("afterTest").toString(),
                },
                {
                        SubClassWithBannedBeforeTestAnnotation.class,
                        BannedBeforeTestAnnotation.class.getMethod("beforeTest").toString(),
                },
                {
                        SubClassWithBannedAfterTestAnnotation.class,
                        BannedAfterTestAnnotation.class.getMethod("afterTest").toString(),
                },
        };
    }

    private static class NoMethods
    {
    }

    private static class NonBannedAnnotations
    {
        @AfterClass
        public void afterClass() {}

        @AfterGroups
        public void afterGroup() {}

        @AfterMethod
        public void afterMethod() {}

        @AfterSuite
        public void afterSuite() {}
    }

    private static class BannedBeforeTestAnnotation
    {
        @BeforeTest
        public void beforeTest() {}

        @AfterSuite
        public void afterSuite() {}
    }

    private static class BannedAfterTestAnnotation
    {
        @AfterTest
        public void afterTest() {}

        @AfterSuite
        public void afterSuite() {}
    }

    private static class SubClassWithNonBannedAnnotations
            extends NonBannedAnnotations {}

    private static class SubClassWithBannedBeforeTestAnnotation
            extends BannedBeforeTestAnnotation {}

    private static class SubClassWithBannedAfterTestAnnotation
            extends BannedAfterTestAnnotation {}
}
