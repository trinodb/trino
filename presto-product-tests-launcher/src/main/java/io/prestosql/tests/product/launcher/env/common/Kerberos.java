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
package io.prestosql.tests.product.launcher.env.common;

import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;

import javax.inject.Inject;

import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_CONFIG_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static io.prestosql.tests.product.launcher.testcontainers.TestcontainersUtil.exposePort;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public class Kerberos
        implements EnvironmentExtender
{
    private final DockerFiles dockerFiles;

    private final String hadoopBaseImage;
    private final String imagesVersion;

    @Inject
    public Kerberos(
            DockerFiles dockerFiles,
            EnvironmentOptions environmentOptions)
    {
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        requireNonNull(environmentOptions, "environmentOptions is null");
        hadoopBaseImage = requireNonNull(environmentOptions.hadoopBaseImage, "environmentOptions.hadoopBaseImage is null");
        imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = hadoopBaseImage + "-kerberized:" + imagesVersion;
        builder.configureContainer("hadoop-master", container -> {
            container.setDockerImageName(dockerImageName);
            exposePort(container, 88);
        });
        builder.configureContainer("presto-master", container -> {
            container.setDockerImageName(dockerImageName);
            exposePort(container, 7778);
            container
                    .withNetworkAliases("presto-master.docker.cluster")
                    .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                    .withFileSystemBind(dockerFiles.getDockerFilesHostPath("common/kerberos/config.properties"), CONTAINER_PRESTO_CONFIG_PROPERTIES, READ_ONLY);
        });
        builder.configureContainer("tests", container -> {
            container.setDockerImageName(dockerImageName);
            container.withFileSystemBind(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-docker-kerberos.yaml"), CONTAINER_TEMPTO_PROFILE_CONFIG, READ_ONLY);
        });
    }
}
