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
package io.starburst.tests.autoconfig;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

public class TestEcrPullThroughNameSubstitutor
{
    @Test
    public void testSubstitutions()
    {
        // Official library
        assertImageName("ecr-registry.com/docker-hub/", "mysql:8.15", "ecr-registry.com/docker-hub/library/mysql:8.15");
        // Rewrite to ECR
        assertImageName("ecr-registry.com/docker-hub/", "repository/mysql:8.15", "ecr-registry.com/docker-hub/repository/mysql:8.15");
        // No rewrite - custom registry
        assertImageName("ecr-registry.com/docker-hub/", "registry.com/repository/mysql:8.15", "registry.com/repository/mysql:8.15");
        // No rewrite - no target registry
        assertImageName(null, "mysql:8.15", "mysql:8.15");
    }

    private void assertImageName(String registry, String imageName, String expectedImageName)
    {
        assertThat(new EcrPullThroughNameSubstitutor(registry).apply(DockerImageName.parse(imageName)))
                .isEqualTo(DockerImageName.parse(expectedImageName));
    }
}
