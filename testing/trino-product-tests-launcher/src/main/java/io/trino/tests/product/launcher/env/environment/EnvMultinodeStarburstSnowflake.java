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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_JVM_CONFIG;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeStarburstSnowflake
        extends EnvironmentProvider
{
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeStarburstSnowflake(StandardMultinode standard, DockerFiles dockerFiles)
    {
        super(standard);
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null")
                .getDockerFilesHostDirectory("conf/environment/multinode-starburst-snowflake");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector("snowflake_jdbc")
                .configureContainer(COORDINATOR, this::configureTrinoContainer)
                .configureContainer(WORKER, this::configureTrinoContainer);
    }

    private DockerContainer configureTrinoContainer(DockerContainer container)
    {
        return container
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("snowflake_jdbc.properties")),
                        CONTAINER_TRINO_ETC + "/catalog/snowflake_jdbc.properties")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("jvm.config")),
                        CONTAINER_TRINO_JVM_CONFIG)
                .withEnv("SNOWFLAKE_JDBC_URL", getSnowflakeEndpoint())
                .withEnv("SNOWFLAKE_PASSWORD", getSnowflakePassword());
    }

    private String getSnowflakeEndpoint()
    {
        String accountName = requireNonNull(System.getenv("SNOWFLAKE_ACCOUNT_NAME"), "Expected SNOWFLAKE_ACCOUNT_NAME environment variable to be set");
        return format("jdbc:snowflake://%s.snowflakecomputing.com/", accountName);
    }

    private String getSnowflakePassword()
    {
        return requireNonNull(System.getenv("SNOWFLAKE_PASSWORD"), "Expected SNOWFLAKE_PASSWORD environment variable to be set");
    }
}
