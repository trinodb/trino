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
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.ServerPackage;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.env.jdk.JdkProvider;

import java.io.File;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.common.Standard.createTrinoContainer;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeStargateWithRemoteSinglenodeStarburst
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final String imagesVersion;
    private final File serverPackage;
    private final JdkProvider jdkProvider;

    @Inject
    public EnvSinglenodeStargateWithRemoteSinglenodeStarburst(
            Standard standard,
            DockerFiles dockerFiles,
            EnvironmentConfig environmentConfig,
            @ServerPackage File serverPackage,
            JdkProvider jdkProvider)
    {
        super(standard);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.imagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getImagesVersion();
        this.serverPackage = requireNonNull(serverPackage, "serverPackage is null");
        this.jdkProvider = requireNonNull(jdkProvider, "jdkProvider is null");
        checkArgument(serverPackage.getName().endsWith(".tar.gz"), "Currently only server .tar.gz package is supported");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerFiles.ResourceProvider resourceProvider = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-stargate-singlenode-starburst");

        builder.addConnector(
                "stargate",
                forHostPath(resourceProvider.getPath("remote_tpch.properties")),
                CONTAINER_TRINO_ETC + "/catalog/remote_tpch.properties");

        // TODO(https://starburstdata.atlassian.net/browse/SEP-4889) Allow enabling java debugging
        DockerContainer remotePresto =
                createTrinoContainer(dockerFiles, serverPackage, jdkProvider, false, false, "ghcr.io/trinodb/testing/centos7-oj17:" + imagesVersion, "remote-starburst")
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/standard/access-control.properties")), Standard.CONTAINER_TRINO_ACCESS_CONTROL_PROPERTIES)
                        .withCopyFileToContainer(forHostPath(resourceProvider.getPath("remote-starburst-config.properties")), Standard.CONTAINER_TRINO_CONFIG_PROPERTIES)
                        // TODO: should I use PortBinder for it?
                        .withFixedExposedPort(18080, 8080)
                        .withExposedPorts(8080);
        builder.addContainer(remotePresto);
    }
}
