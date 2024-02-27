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

import jakarta.annotation.Nullable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import static java.lang.System.getenv;

public class EcrPullThroughNameSubstitutor
        extends ImageNameSubstitutor
{
    private static final String ENV_KEY_NAME = "TESTCONTAINER_DOCKER_PULL_THROUGH_REGISTRY";
    @Nullable
    private final String registry;

    @SuppressWarnings("unused") // Loaded by testcontainers
    public EcrPullThroughNameSubstitutor()
    {
        this(getenv(ENV_KEY_NAME));
    }

    EcrPullThroughNameSubstitutor(@Nullable String registry)
    {
        if (registry != null && registry.isBlank()) {
            throw new IllegalArgumentException("Registry must be null or non-empty");
        }
        this.registry = registry;
    }

    @Override
    public DockerImageName apply(DockerImageName dockerImageName)
    {
        // Already belongs to some registry, or the pull-through registry is configured
        if (!isNullOrEmpty(dockerImageName.getRegistry()) || registry == null) {
            return dockerImageName;
        }

        String repository = dockerImageName.getRepository();
        if (!repository.contains("/")) {
            repository = "library/" + repository;
        }

        return DockerImageName
                .parse(registry + repository + ":" + dockerImageName.getVersionPart())
                .asCompatibleSubstituteFor(dockerImageName);
    }

    @Override
    protected String getDescription()
    {
        return "ECR pull-through cache for hub.docker.com";
    }

    private static boolean isNullOrEmpty(String s)
    {
        return s == null || s.isEmpty();
    }
}
