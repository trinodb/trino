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
package io.trino.tests.framework;

import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Minimal test to verify JUnit 5 and TestContainers integration is working.
 * This test exists to validate the framework setup during the migration from TestNG.
 */
@Testcontainers
class TestJUnit5FrameworkVerification
{
    @Test
    void testJUnit5Works()
    {
        assertThat(true).isTrue();
    }

    @Test
    void testTestcontainersAnnotationPresent()
    {
        // Verify that @Testcontainers annotation is recognized
        assertThat(getClass().isAnnotationPresent(Testcontainers.class)).isTrue();
    }

    @Test
    void testFlakyAnnotationCanBeApplied()
    {
        // Verify that @Flaky annotation exists and has expected attributes
        assertThat(Flaky.class.isAnnotation()).isTrue();

        try {
            Flaky flaky = TestWithFlakyAnnotation.class.getMethod("flakyTest").getAnnotation(Flaky.class);
            assertThat(flaky).isNotNull();
            assertThat(flaky.issue()).isEqualTo("https://example.com/issue/123");
            assertThat(flaky.match()).isEqualTo(".*RuntimeException.*");
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testFlakyAnnotationHasExtendWith()
    {
        // Verify that @Flaky has @ExtendWith annotation
        assertThat(Flaky.class.getAnnotation(ExtendWith.class)).isNotNull();
    }

    @Test
    @TestGroup.Smoke
    void testTagAnnotationWorks()
    {
        // Verify that @TestGroup.Smoke annotation works (meta-annotation includes @Tag("smoke"))
        Tag tag = TestGroup.Smoke.class.getAnnotation(Tag.class);
        assertThat(tag).isNotNull();
        assertThat(tag.value()).isEqualTo("smoke");
    }

    /**
     * Helper class with a @Flaky annotated test method for reflection testing.
     */
    @SuppressWarnings("deprecation")
    static class TestWithFlakyAnnotation
    {
        @Flaky(issue = "https://example.com/issue/123", match = ".*RuntimeException.*")
        public void flakyTest()
        {
            // This method exists for annotation inspection
        }
    }
}
