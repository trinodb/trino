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
import io.trino.tests.product.launcher.env.Debug;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.ServerPackage;
import io.trino.tests.product.launcher.env.SupportedTrinoJdk;

import javax.inject.Inject;

import java.io.File;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.createTrinoContainer;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class StandardMultinode
        implements EnvironmentExtender
{
    private final Standard standard;
    private final DockerFiles dockerFiles;
    private final DockerFiles.ResourceProvider configDir;
    private final String imagesVersion;
    private final File serverPackage;
    private final SupportedTrinoJdk jdkVersion;
    private final boolean debug;

    @Inject
    public StandardMultinode(
            Standard standard,
            DockerFiles dockerFiles,
            EnvironmentConfig environmentConfig,
            @ServerPackage File serverPackage,
            SupportedTrinoJdk jdkVersion,
            @Debug boolean debug)
    {
        this.standard = requireNonNull(standard, "standard is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("common/standard-multinode");
        this.imagesVersion = environmentConfig.getImagesVersion();
        this.jdkVersion = requireNonNull(jdkVersion, "jdkVersion is null");
        this.serverPackage = requireNonNull(serverPackage, "serverPackage is null");
        this.debug = debug;
        checkArgument(serverPackage.getName().endsWith(".tar.gz"), "Currently only server .tar.gz package is supported");
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(standard);
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode-master-config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES));
        builder.addContainers(createTrinoWorker());
    }

    @SuppressWarnings("resource")
    private DockerContainer createTrinoWorker()
    {
        return createTrinoContainer(dockerFiles, serverPackage, jdkVersion, debug, "ghcr.io/trinodb/testing/centos7-oj17:" + imagesVersion, WORKER)
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode-worker-config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES);
    }
}
