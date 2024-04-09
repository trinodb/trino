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
package io.trino.tests.product.launcher.env.environment;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_JVM_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeSnowflake
        extends EnvironmentProvider
{
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeSnowflake(DockerFiles dockerFiles, Standard standard)
    {
        super(standard);
        configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/multinode-snowflake");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                container
                        .withEnv("SNOWFLAKE_URL", requireEnv("SNOWFLAKE_URL"))
                        .withEnv("SNOWFLAKE_USER", requireEnv("SNOWFLAKE_USER"))
                        .withEnv("SNOWFLAKE_PASSWORD", requireEnv("SNOWFLAKE_PASSWORD"))
                        .withEnv("SNOWFLAKE_DATABASE", requireEnv("SNOWFLAKE_DATABASE"))
                        .withEnv("SNOWFLAKE_ROLE", requireEnv("SNOWFLAKE_ROLE"))
                        .withEnv("SNOWFLAKE_WAREHOUSE", requireEnv("SNOWFLAKE_WAREHOUSE"));

                container.withCopyFileToContainer(forHostPath(configDir.getPath("jvm.config")), CONTAINER_TRINO_JVM_CONFIG);
            }
        });

        builder.addConnector("snowflake", forHostPath(configDir.getPath("snowflake.properties")));
    }

    private static String requireEnv(String variable)
    {
        return requireNonNull(System.getenv(variable), () -> "environment variable not set: " + variable);
    }
}
