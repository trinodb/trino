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
package io.prestosql.testng.services;

import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.Requirements;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.configuration.Configuration;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

public class TestReportUnannotatedMethods
{
    private final ReportUnannotatedMethods instance = new ReportUnannotatedMethods();

    @Test
    public void testTest()
    {
        assertThat(instance.findUnannotatedTestMethods(TestingTest.class))
                .isEmpty();
    }

    @Test
    public void testTestWithoutTestAnnotation()
    {
        assertThat(instance.findUnannotatedTestMethods(TestingTestWithoutTestAnnotation.class))
                .extracting(Method::getName)
                .containsExactly("testWithMissingTestAnnotation");
    }

    @Test
    public void testTemptoRequirementsProvider()
    {
        assertThat(instance.findUnannotatedTestMethods(TestingRequirementsProvider.class))
                .isEmpty();
    }

    private static class TestingTest
    {
        @Test
        public void test() {}
    }

    private static class TestingTestWithoutTestAnnotation
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
    }
}
