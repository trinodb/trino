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
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Trino with Apache Ranger authorizer plugin
 */
@TestsEnvironment
public class EnvMultinodeApacheRanger
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    @Inject
    public EnvMultinodeApacheRanger(StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(standardMultinode);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerFiles.ResourceProvider configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-apache-ranger/");

        builder.addConnector("memory", forHostPath(configDir.getPath("memory.properties")));

        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("access-control.properties")), CONTAINER_TRINO_ETC + "/access-control.properties"));
        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-trino-security.xml")), CONTAINER_TRINO_ETC + "/ranger-trino-security.xml"));
        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-trino-audit.xml")), CONTAINER_TRINO_ETC + "/ranger-trino-audit.xml"));
        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-policymgr-ssl.xml")), CONTAINER_TRINO_ETC + "/ranger-policymgr-ssl.xml"));
        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("trino-policies.json")), "/tmp/trino_dev_trino.json"));
    }
}
