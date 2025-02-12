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
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Debug;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.Ipv6;
import io.trino.tests.product.launcher.env.ServerPackage;
import io.trino.tests.product.launcher.env.Tracing;
import io.trino.tests.product.launcher.env.common.HadoopKerberos;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.env.jdk.JdkProvider;

import java.io.File;
import java.util.Objects;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.worker;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_ICEBERG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.createTrinoContainer;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeTlsKerberosDelegation
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final DockerFiles.ResourceProvider configDir;
    private final String trinoDockerImageName;
    private final JdkProvider jdkProvider;
    private final File serverPackage;
    private final boolean debug;
    private final boolean tracing;
    private final boolean ipv6;

    @Inject
    public EnvMultinodeTlsKerberosDelegation(
            DockerFiles dockerFiles,
            Standard standard,
            HadoopKerberos hadoopKerberos,
            EnvironmentConfig config,
            @ServerPackage File serverPackage,
            JdkProvider jdkProvider,
            @Debug boolean debug,
            @Tracing boolean tracing,
            @Ipv6 boolean ipv6)
    {
        super(ImmutableList.of(standard, hadoopKerberos));
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-tls-kerberos-delegation");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.ipv6 = ipv6;
        String hadoopBaseImage = config.getHadoopBaseImage();
        String hadoopImagesVersion = config.getHadoopImagesVersion();
        this.trinoDockerImageName = hadoopBaseImage + "-kerberized:" + hadoopImagesVersion;
        this.jdkProvider = requireNonNull(jdkProvider, "jdkProvider is null");
        this.serverPackage = requireNonNull(serverPackage, "serverPackage is null");
        this.debug = debug;
        this.tracing = tracing;
    }

    @Override
    @SuppressWarnings("resource")
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> {
            verify(Objects.equals(container.getDockerImageName(), trinoDockerImageName), "Expected image '%s', but is '%s'", trinoDockerImageName, container.getDockerImageName());
            container
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/config-master.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES);
        });
        builder.addConnector("hive", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES);
        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/iceberg.properties")), CONTAINER_TRINO_ICEBERG_PROPERTIES);

        builder.addContainers(createTrinoWorker(worker(1)), createTrinoWorker(worker(2)));
        builder.configureContainer(TESTS, container -> {
            // Configures a low ticket lifetime to ensure tickets get expired during tests
            container.withCopyFileToContainer(forHostPath(configDir.getPath("krb5_client.conf")), "/etc/krb5.conf");
        });
        configureTempto(builder, configDir);
    }

    @SuppressWarnings("resource")
    private DockerContainer createTrinoWorker(String workerName)
    {
        return createTrinoContainer(dockerFiles, serverPackage, jdkProvider, debug, tracing, ipv6, trinoDockerImageName, workerName)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"))
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/config-worker.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-tls-kerberos/iceberg.properties")), CONTAINER_TRINO_ICEBERG_PROPERTIES);
    }
}
