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

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.MultinodeProvider;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_ICEBERG_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeTls
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeTls(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            MultinodeProvider multinodeProvider,
            Hadoop hadoop)
    {
        super(ImmutableList.of(multinodeProvider.workers(2), hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    @SuppressWarnings("resource")
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureCoordinator(container -> {
            container
                    .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls/config-master.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES);

            portBinder.exposePort(container, 7778);
        });

        builder.configureWorkers(this::configureTrinoWorker);
        builder.configureTests(container ->
                container.withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-docker-tls.yaml")), CONTAINER_TEMPTO_PROFILE_CONFIG));
    }

    private DockerContainer configureTrinoWorker(DockerContainer container)
    {
        return container
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls/config-worker.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/iceberg.properties")), CONTAINER_TRINO_ICEBERG_PROPERTIES);
    }
}
