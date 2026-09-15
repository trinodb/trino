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

import com.google.common.collect.ImmutableList;
import io.trino.testing.containers.RequireVersionedImageSubstitutor;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.ImageNameSubstitutor;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link RequireVersionedImageSubstitutor} is registered as a Testcontainers
 * {@link ImageNameSubstitutor} service, so every container started by product tests is checked
 * for a pinned image. Guards against the service registration being lost or misspelled.
 */
class TestImageNameSubstitutorRegistration
{
    @Test
    void testSubstitutorIsRegistered()
    {
        assertThat(ImmutableList.copyOf(ServiceLoader.load(ImageNameSubstitutor.class)))
                .hasAtLeastOneElementOfType(RequireVersionedImageSubstitutor.class);
    }
}
