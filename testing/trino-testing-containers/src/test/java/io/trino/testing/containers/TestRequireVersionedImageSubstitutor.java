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
package io.trino.testing.containers;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRequireVersionedImageSubstitutor
{
    private final RequireVersionedImageSubstitutor substitutor = new RequireVersionedImageSubstitutor();

    @Test
    void testVersionedImagePasses()
    {
        DockerImageName image = DockerImageName.parse("ghcr.io/trinodb/testing/hive3.1:123");
        assertThat(substitutor.apply(image)).isSameAs(image);
    }

    @Test
    void testDigestPinnedImagePasses()
    {
        DockerImageName image = DockerImageName.parse("trinodb/trino@sha256:0000000000000000000000000000000000000000000000000000000000000000");
        assertThat(substitutor.apply(image)).isSameAs(image);
    }

    @Test
    void testExplicitLatestRejected()
    {
        assertThatThrownBy(() -> substitutor.apply(DockerImageName.parse("trinodb/trino:latest")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be pinned")
                .hasMessageContaining("trinodb/trino:latest");
    }

    @Test
    void testUntaggedImageRejected()
    {
        // An untagged reference resolves to the version part "latest"
        assertThatThrownBy(() -> substitutor.apply(DockerImageName.parse("trinodb/trino")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be pinned");
    }
}
