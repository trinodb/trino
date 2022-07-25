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
import io.trino.tests.product.launcher.env.Debug;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.ServerPackage;
import io.trino.tests.product.launcher.env.SupportedTrinoJdk;
import io.trino.tests.product.launcher.env.common.HadoopKerberos;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import java.io.File;
import java.util.Objects;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.worker;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_ICEBERG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.createPrestoContainer;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeTlsKerberos
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final String prestoDockerImageName;
    private final SupportedTrinoJdk jdkVersion;
    private final File serverPackage;
    private final boolean debug;

    @Inject
    public EnvMultinodeTlsKerberos(
            DockerFiles dockerFiles,
            Standard standard,
            HadoopKerberos hadoopKerberos,
            EnvironmentConfig config,
            @ServerPackage File serverPackage,
            SupportedTrinoJdk jdkVersion,
            @Debug boolean debug)
    {
        super(ImmutableList.of(standard, hadoopKerberos));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        String hadoopBaseImage = requireNonNull(config, "config is null").getHadoopBaseImage();
        String hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
        this.prestoDockerImageName = hadoopBaseImage + "-kerberized:" + hadoopImagesVersion;
        this.jdkVersion = requireNonNull(jdkVersion, "jdkVersion is null");
        this.serverPackage = requireNonNull(serverPackage, "serverPackage is null");
        this.debug = debug;
    }

    @Override
    @SuppressWarnings("resource")
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> {
            verify(Objects.equals(container.getDockerImageName(), prestoDockerImageName), "Expected image '%s', but is '%s'", prestoDockerImageName, container.getDockerImageName());
            container
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/config-master.properties")), CONTAINER_PRESTO_CONFIG_PROPERTIES)
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/hive.properties")), CONTAINER_PRESTO_HIVE_PROPERTIES)
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/iceberg.properties")), CONTAINER_PRESTO_ICEBERG_PROPERTIES);
        });

        builder.addContainers(createPrestoWorker(worker(1)), createPrestoWorker(worker(2)));
    }

    @SuppressWarnings("resource")
    private DockerContainer createPrestoWorker(String workerName)
    {
        return createPrestoContainer(dockerFiles, serverPackage, jdkVersion, debug, prestoDockerImageName, workerName)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/config-worker.properties")), CONTAINER_PRESTO_CONFIG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/hive.properties")), CONTAINER_PRESTO_HIVE_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/iceberg.properties")), CONTAINER_PRESTO_ICEBERG_PROPERTIES);
    }
}
