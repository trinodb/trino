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
package io.prestosql.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.ServerPackage;
import io.prestosql.tests.product.launcher.env.common.AbstractEnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Kerberos;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import java.io.File;
import java.util.Objects;

import static com.google.common.base.Verify.verify;
import static io.prestosql.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_HIVE_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_ICEBERG_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_CONFIG_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Standard.createPrestoContainer;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class MultinodeTlsKerberos
        extends AbstractEnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final String prestoDockerImageName;
    private final File serverPackage;

    @Inject
    public MultinodeTlsKerberos(
            DockerFiles dockerFiles,
            Standard standard,
            Hadoop hadoop,
            Kerberos kerberos,
            EnvironmentConfig config,
            @ServerPackage File serverPackage)
    {
        super(ImmutableList.of(standard, hadoop, kerberos));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        String hadoopBaseImage = requireNonNull(config, "config is null").getHadoopBaseImage();
        String hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
        this.prestoDockerImageName = hadoopBaseImage + "-kerberized:" + hadoopImagesVersion;
        this.serverPackage = requireNonNull(serverPackage, "serverPackage is null");
    }

    @Override
    @SuppressWarnings("resource")
    protected void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer("presto-master", container -> {
            verify(Objects.equals(container.getDockerImageName(), prestoDockerImageName), "Expected image '%s', but is '%s'", prestoDockerImageName, container.getDockerImageName());
            container
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/config-master.properties")), CONTAINER_PRESTO_CONFIG_PROPERTIES)
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/hive.properties")), CONTAINER_PRESTO_HIVE_PROPERTIES)
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/iceberg.properties")), CONTAINER_PRESTO_ICEBERG_PROPERTIES);
        });

        addPrestoWorker(builder, "presto-worker-1");
        addPrestoWorker(builder, "presto-worker-2");
    }

    @SuppressWarnings("resource")
    private void addPrestoWorker(Environment.Builder builder, String workerName)
    {
        DockerContainer container = createPrestoContainer(dockerFiles, serverPackage, prestoDockerImageName)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                .withNetworkAliases(workerName + ".docker.cluster")
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/config-worker.properties")), CONTAINER_PRESTO_CONFIG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/hive.properties")), CONTAINER_PRESTO_HIVE_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/iceberg.properties")), CONTAINER_PRESTO_ICEBERG_PROPERTIES);

        builder.addContainer(workerName, container);
    }
}
