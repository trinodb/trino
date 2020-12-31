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

import io.trino.tempto.Requirement;
import io.trino.tempto.Requirements;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.testmarkers.WithName;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static io.trino.testng.services.ReportUnannotatedMethods.findUnannotatedTestMethods;
import static io.trino.testng.services.ReportUnannotatedMethods.isTemptoClass;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestReportUnannotatedMethods
{
    @Test
    public void testTest()
    {
        assertThat(findUnannotatedTestMethods(TestingTest.class))
                .isEmpty();
        assertThat(findUnannotatedTestMethods(TestingTestWithProxy.class))
                .isEmpty();
    }

    @Test
    public void testTestWithoutTestAnnotation()
    {
        assertThat(findUnannotatedTestMethods(TestingTestWithoutTestAnnotation.class))
                .extracting(Method::getName)
                .containsExactly("testWithMissingTestAnnotation", "methodInInterface");
    }

    @Test
    public void testTemptoRequirementsProvider()
    {
        assertThat(findUnannotatedTestMethods(TestingRequirementsProvider.class))
                .extracting(Method::getName)
                .containsExactly("testWithMissingTestAnnotation");
        assertThat(findUnannotatedTestMethods(TestingRequirementsProviderWithProxyClass.class))
                .extracting(Method::getName)
                .containsExactly("testWithMissingTestAnnotation", "testWithMissingTestAnnotationInProxy");
    }

    @Test
    public void testTemptoPackage()
    {
        assertTrue(isTemptoClass(RequirementsProvider.class));
        assertTrue(isTemptoClass(WithName.class));
        assertFalse(isTemptoClass(getClass()));
    }

    @Test
    public void testSuppressedMethods()
    {
        assertThat(findUnannotatedTestMethods(TestingTestWithSuppressedPublicMethod.class))
                .isEmpty();
        assertThat(findUnannotatedTestMethods(TestingTestWithSuppressedPublicMethodInInterface.class))
                .isEmpty();
    }

    private static class TestingTest
            implements TestingInterfaceWithTest
    {
        @Test
        public void test() {}
    }

    private static class TestingTestWithProxy
            extends TestingInterfaceWithTestProxy
    {
        @Test
        public void test() {}
    }

    private static class TestingTestWithoutTestAnnotation
            implements TestingInterface
    {
        public void testWithMissingTestAnnotation() {}

        @Override
        public String toString()
        {
            return "test override";
        }
    }

    private static class TestingRequirementsProvider
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.allOf();
        }

        public void testWithMissingTestAnnotation() {}
    }

    private static class TestingRequirementsProviderWithProxyClass
            extends RequirementsProviderProxy
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.allOf();
        }

        public void testWithMissingTestAnnotation() {}
    }

    private abstract static class RequirementsProviderProxy
            implements RequirementsProvider
    {
        public void testWithMissingTestAnnotationInProxy() {}
    }

    private static class TestingInterfaceWithTestProxy
            implements TestingInterfaceWithTest {}

    private interface TestingInterfaceWithTest
    {
        @Test
        default void testInInterface() {}
    }

    private interface TestingInterface
    {
        default void methodInInterface() {}
    }

    private static class TestingTestWithSuppressedPublicMethod
    {
        @Test
        public void test() {}

        @ReportUnannotatedMethods.Suppress
        public void method() {}
    }

    private static class TestingTestWithSuppressedPublicMethodInInterface
            implements InterfaceWithSuppressedPublicMethod
    {
        @Test
        public void test() {}
    }

    private interface InterfaceWithSuppressedPublicMethod
    {
        @ReportUnannotatedMethods.Suppress
        default void method() {}
    }
}
