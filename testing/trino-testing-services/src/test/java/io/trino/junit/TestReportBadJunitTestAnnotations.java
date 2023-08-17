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
package io.trino.junit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.lang.reflect.Method;

import static io.trino.junit.ReportBadJunitTestAnnotations.findUnannotatedInheritedTestMethods;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReportBadJunitTestAnnotations
{
    @Test
    public void testTest()
    {
        assertThat(findUnannotatedInheritedTestMethods(TestingTest.class))
                .isEmpty();
        assertThat(findUnannotatedInheritedTestMethods(TestingTestWithProxy.class))
                .isEmpty();
        assertThat(findUnannotatedInheritedTestMethods(TestingBeforeAfterAnnotations.class))
                .isEmpty();
        assertThat(findUnannotatedInheritedTestMethods(TestingTestWithoutTestAnnotation.class))
                .isEmpty();
    }

    @Test
    public void testTestWithoutTestAnnotation()
    {
        assertThat(findUnannotatedInheritedTestMethods(TestingTestWithoutAnnotation.class))
                .extracting(Method::getName)
                .containsExactly("testInInterface");
    }

    private static class TestingTest
            implements TestingInterfaceWithTest
    {
        @Test
        public void test() {}
    }

    private static class TestingTestWithoutAnnotation
            implements TestingInterfaceWithTest
    {
        @Override
        public void testInInterface()
        {
            TestingInterfaceWithTest.super.testInInterface();
        }
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

    private static class TestingBeforeAfterAnnotations
            extends BaseTest {}

    private static class BaseTest
    {
        @BeforeAll
        @BeforeClass
        public final void initialize() {}

        @AfterAll
        @AfterClass(alwaysRun = true)
        public final void destroy() {}
    }
}
