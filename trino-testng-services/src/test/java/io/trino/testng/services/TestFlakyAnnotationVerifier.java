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

import static io.trino.testng.services.FlakyAnnotationVerifier.findMethodsWithFlakyAndNoTestAnnotation;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFlakyAnnotationVerifier
{
    @Test
    public void testOk()
    {
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestParentTestAndFlaky.class))
                .isEmpty();
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestChildTestAndFlaky.class))
                .isEmpty();
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestParentJustTest.class))
                .isEmpty();
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestChildJustTest.class))
                .isEmpty();
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestChildJustTestFlakyInParent.class))
                .isEmpty();
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestChildNoDeclaration.class))
                .isEmpty();
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestChildNoDeclarationFlakyInParent.class)) // Should that be ok?
                .isEmpty();
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestGrandChildChildJustTestChildNoDeclaration.class))
                .isEmpty();
    }

    @Test
    public void testMissingTest()
    {
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestChildJustFlaky.class))
                .extracting(Method::getName)
                .contains("test");
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestChildNoTestNoFlakyFlakyInParent.class))
                .extracting(Method::getName)
                .contains("test");
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestGrandChildChildJustFlakyChildNoDeclaration.class))
                .extracting(Method::getName)
                .contains("test");
        assertThat(findMethodsWithFlakyAndNoTestAnnotation(TestNotTestMethodWithFlaky.class))
                .extracting(Method::getName)
                .contains("test");
    }

    @Test
    public void testInvalidPattern()
    {
        assertThat(FlakyAnnotationVerifier.verifyFlakyAnnotations(TestFlakyInvalidPattern.class))
                .hasValueSatisfying(value -> {
                    assertThat(value)
                            .startsWith("Test method public void io.trino.testng.services.TestFlakyAnnotationVerifier$TestFlakyInvalidPattern.test() has invalid @Flaky.match: java.util.regex.PatternSyntaxException: Unclosed group near");
                });
    }

    private static class TestNotTestMethodWithFlaky
    {
        @Flaky(issue = "Blah", match = "Blah")
        @ReportUnannotatedMethods.Suppress
        public void test() {}
    }

    private static class TestParentJustTest
    {
        @Test
        public void test() {}
    }

    private static class TestChildJustTest
            extends TestParentJustTest
    {
        @Override
        @Test
        public void test() {}
    }

    private static class TestChildJustFlaky
            extends TestParentJustTest
    {
        @Override
        @Flaky(issue = "Blah", match = "Blah")
        public void test() {}
    }

    private static class TestChildTestAndFlaky
            extends TestParentJustTest
    {
        @Override
        @Test
        @Flaky(issue = "Blah", match = "Blah")
        public void test() {}
    }

    private static class TestParentTestAndFlaky
    {
        @Test
        @Flaky(issue = "Blah", match = "Blah")
        public void test() {}
    }

    private static class TestChildJustTestFlakyInParent
            extends TestParentTestAndFlaky
    {
        @Override
        @Test
        public void test() {}
    }

    private static class TestChildNoTestNoFlakyFlakyInParent
            extends TestParentTestAndFlaky
    {
        @Override
        public void test() {}
    }

    private static class TestChildNoDeclaration
            extends TestParentJustTest
    {}

    private static class TestGrandChildChildJustTestChildNoDeclaration
            extends TestChildNoDeclaration
    {
        @Override
        @Test
        public void test() {}
    }

    private static class TestGrandChildChildJustFlakyChildNoDeclaration
            extends TestChildNoDeclaration
    {
        @Override
        @Flaky(issue = "Blah", match = "Blah")
        public void test() {}
    }

    private static class TestChildNoDeclarationFlakyInParent
            extends TestParentTestAndFlaky
    {}

    private static class TestFlakyInvalidPattern
    {
        @Test
        @Flaky(match = "unbalanaced (", issue = "x")
        public void test() {}
    }
}
