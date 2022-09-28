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

import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static io.trino.testng.services.ReportPrivateMethods.findPrivateTestMethods;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReportPrivateMethods
{
    @Test
    public void testTest()
    {
        assertThat(findPrivateTestMethods(TestReportPrivateMethods.class))
                .isEmpty();
    }

    @Test
    public void testPackagePrivate()
    {
        assertThat(findPrivateTestMethods(PackagePrivateTest.class))
                .extracting(Method::getName)
                .containsExactly("testPackagePrivate");
    }

    @Test
    public void testDataProvider()
    {
        assertThat(findPrivateTestMethods(PrivateDataProviderTest.class))
                .extracting(Method::getName)
                .containsExactly("foosDataProvider");
    }

    @Test
    public void testDerived()
    {
        assertThat(findPrivateTestMethods(DerivedTest.class))
                .extracting(Method::getName)
                .containsExactly("testPackagePrivateInBase");
    }

    @Test
    public void testSuppression()
    {
        assertThat(findPrivateTestMethods(PackagePrivateSuppressedTest.class))
                .isEmpty();
    }

    private static class PackagePrivateTest
    {
        // using @Test would make the class a test, and fail the build
        @BeforeTest
        void testPackagePrivate() {}
    }

    private static class PrivateDataProviderTest
    {
        @DataProvider
        private Object[][] foosDataProvider()
        {
            return new Object[][] {
                    {"foo1"},
                    {"foo2"},
            };
        }
    }

    private static class BasePackagePrivateTest
    {
        // using @Test would make the class a test, and fail the build
        @BeforeTest
        void testPackagePrivateInBase() {}
    }

    private static class DerivedTest
            extends BasePackagePrivateTest
    {
        // using @Test would make the class a test, and fail the build
        @BeforeTest
        public void testPublic() {}
    }

    private static class PackagePrivateSuppressedTest
    {
        // using @Test would make the class a test, and fail the build
        @BeforeTest
        @ReportPrivateMethods.Suppress
        void testPackagePrivate() {}
    }
}
