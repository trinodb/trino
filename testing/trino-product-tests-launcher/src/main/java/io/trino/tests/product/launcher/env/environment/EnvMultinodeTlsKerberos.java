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
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.HadoopKerberos;
import io.trino.tests.product.launcher.env.common.MultinodeProvider;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import java.util.Objects;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_ICEBERG_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeTlsKerberos
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final String trinoDockerImage;

    @Inject
    public EnvMultinodeTlsKerberos(
            DockerFiles dockerFiles,
            MultinodeProvider multinodeProvider,
            HadoopKerberos hadoopKerberos,
            EnvironmentConfig config)
    {
        super(ImmutableList.of(multinodeProvider.workers(2), hadoopKerberos));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        String hadoopBaseImage = requireNonNull(config, "config is null").getHadoopBaseImage();
        String hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
        this.trinoDockerImage = hadoopBaseImage + "-kerberized:" + hadoopImagesVersion;
    }

    @Override
    @SuppressWarnings("resource")
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureTrinoContainers(container -> container.setDockerImageName(trinoDockerImage));

        builder.configureCoordinator(container -> {
            verify(Objects.equals(container.getDockerImageName(), trinoDockerImage), "Expected image '%s', but is '%s'", trinoDockerImage, container.getDockerImageName());
            container
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/config-master.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES)
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES)
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/iceberg.properties")), CONTAINER_TRINO_ICEBERG_PROPERTIES);
        });

        builder.configureWorkers(this::configureWorker);
    }

    @SuppressWarnings("resource")
    private DockerContainer configureWorker(DockerContainer container)
    {
        return container
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/config-worker.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/iceberg.properties")), CONTAINER_TRINO_ICEBERG_PROPERTIES);
    }
}
