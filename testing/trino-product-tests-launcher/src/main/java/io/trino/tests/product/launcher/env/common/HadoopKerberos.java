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
package io.trino.tests.product.launcher.env.common;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import javax.inject.Inject;

import java.util.List;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class HadoopKerberos
        implements EnvironmentExtender
{
    private final DockerFiles.ResourceProvider configDir;
    private final PortBinder portBinder;

    private final String hadoopBaseImage;
    private final String hadoopImagesVersion;

    private final Hadoop hadoop;

    @Inject
    public HadoopKerberos(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            EnvironmentConfig environmentConfig,
            Hadoop hadoop)
    {
        this.configDir = dockerFiles.getDockerFilesHostDirectory("common/hadoop-kerberos/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        hadoopBaseImage = requireNonNull(environmentConfig, "environmentConfig is null").getHadoopBaseImage();
        hadoopImagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getHadoopImagesVersion();
        this.hadoop = requireNonNull(hadoop, "hadoop is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = hadoopBaseImage + "-kerberized:" + hadoopImagesVersion;
        builder.configureContainers(container -> true, container -> {
            container.setDockerImageName(dockerImageName);
        });
        builder.configureContainer(HADOOP, container -> {
            portBinder.exposePort(container, 88);
        });
        builder.configureCoordinator(container -> {
            portBinder.exposePort(container, 7778);
            container
                    .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                    .withCopyFileToContainer(forHostPath(configDir.getPath("coordinator/config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES);
        });
        builder.configureWorkers(container -> {
            portBinder.exposePort(container, 8443);
            container
                    .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                    .withCopyFileToContainer(forHostPath(configDir.getPath("worker/config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES);
        });
        configureTempto(builder, configDir);
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(hadoop);
    }
}
