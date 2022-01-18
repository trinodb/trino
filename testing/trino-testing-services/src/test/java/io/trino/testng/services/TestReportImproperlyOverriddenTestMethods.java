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

import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static io.trino.testng.services.ReportImproperlyOverriddenTestMethods.findImproperlyOverriddenTestMethods;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReportImproperlyOverriddenTestMethods
{
    @Test
    public void testTest()
    {
        assertThat(findImproperlyOverriddenTestMethods(TestReportImproperlyOverriddenTestMethods.class))
                .isEmpty();
    }

    @Test
    public void testNotOverridden()
    {
        assertThat(findImproperlyOverriddenTestMethods(TestNothingOverridden.class))
                .isEmpty();
    }

    @Test
    public void testOverriddenProperly()
    {
        assertThat(findImproperlyOverriddenTestMethods(TestOverriddenProperly.class))
                .isEmpty();
    }

    @Test
    public void testOverriddenProperlyViaIntermediate()
    {
        assertThat(findImproperlyOverriddenTestMethods(TestOverriddenProperlyViaIntermediate.class))
                .isEmpty();
    }

    @Test
    public void testOverriddenImproperly()
    {
        assertThat(findImproperlyOverriddenTestMethods(TestOverriddenImproperly.class))
                .extracting(Method::getName)
                .containsExactly("test", "repeatedTest", "factoryTest", "templateTest", "parameterizedTest");
    }

    @Test
    public void testOverriddenImproperlyAndSuppressed()
    {
        assertThat(findImproperlyOverriddenTestMethods(TestOverriddenImproperlyAndSuppressed.class))
                .isEmpty();
    }

    @Test
    public void testOverriddenImproperlyWithDifferentType()
    {
        assertThat(findImproperlyOverriddenTestMethods(TestOverriddenImproperlyWithDifferentType.class))
                .extracting(Method::getName)
                .containsExactly("test");
    }

    @Test
    public void testOverriddenImproperlyWithSameTypeDifferentParameters()
    {
        assertThat(findImproperlyOverriddenTestMethods(TestOverriddenImproperlyWithSameTypeDifferentParameters.class))
                .extracting(Method::getName)
                .containsExactly("repeatedTest");
    }

    private static class BaseTest
    {
        @org.junit.jupiter.api.Test
        void test() {}

        @org.junit.jupiter.api.RepeatedTest(1)
        void repeatedTest() {}

        @org.junit.jupiter.api.TestFactory
        void factoryTest() {}

        @org.junit.jupiter.api.TestTemplate
        void templateTest() {}

        @org.junit.jupiter.params.ParameterizedTest
        @org.junit.jupiter.params.provider.EmptySource
        void parameterizedTest() {}
    }

    private static class TestNothingOverridden
            extends BaseTest
    {
    }

    private static class TestOverriddenProperly
            extends BaseTest
    {
        @Override
        @org.junit.jupiter.api.Test
        void test() {}

        @Override
        @org.junit.jupiter.api.RepeatedTest(1)
        void repeatedTest() {}

        @Override
        @org.junit.jupiter.api.TestFactory
        void factoryTest() {}

        @Override
        @org.junit.jupiter.api.TestTemplate
        void templateTest() {}

        @Override
        @org.junit.jupiter.params.ParameterizedTest
        // source type changed: this is OK
        @org.junit.jupiter.params.provider.ValueSource(strings = {})
        void parameterizedTest() {}
    }

    private abstract static class TestIntermediate
            extends BaseTest
    {
    }

    private static class TestOverriddenProperlyViaIntermediate
            extends TestIntermediate
    {
        @Override
        @org.junit.jupiter.api.Test
        public void test() {}
    }

    private static class TestOverriddenImproperly
            extends BaseTest
    {
        @Override
        void test() {}

        @Override
        void repeatedTest() {}

        @Override
        void factoryTest() {}

        @Override
        void templateTest() {}

        @Override
        void parameterizedTest() {}
    }

    private static class TestOverriddenImproperlyAndSuppressed
            extends BaseTest
    {
        @Override
        @ReportImproperlyOverriddenTestMethods.Suppress
        void test() {}
    }

    private static class TestOverriddenImproperlyWithDifferentType
            extends BaseTest
    {
        @Override
        @org.junit.jupiter.api.RepeatedTest(1)
        public void test() {}
    }

    private static class TestOverriddenImproperlyWithSameTypeDifferentParameters
            extends BaseTest
    {
        @Override
        @org.junit.jupiter.api.RepeatedTest(5)
        public void repeatedTest() {}
    }
}
