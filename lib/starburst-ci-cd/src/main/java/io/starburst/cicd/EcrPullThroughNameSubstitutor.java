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
package io.starburst.cicd;

import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.System.getenv;

public class EcrPullThroughNameSubstitutor
        extends ImageNameSubstitutor
{
    private static final String ENV_KEY_NAME = "TESTCONTAINER_DOCKER_PULL_THROUGH_REGISTRY";
    private final String registry;

    public EcrPullThroughNameSubstitutor()
    {
        this(getenv(ENV_KEY_NAME));
    }

    EcrPullThroughNameSubstitutor(String registry)
    {
        this.registry = registry;
    }

    @Override
    public DockerImageName apply(DockerImageName dockerImageName)
    {
        // Already belongs to some registry
        if (!isNullOrEmpty(dockerImageName.getRegistry())) {
            return dockerImageName;
        }

        if (isNullOrEmpty(registry)) {
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
}
