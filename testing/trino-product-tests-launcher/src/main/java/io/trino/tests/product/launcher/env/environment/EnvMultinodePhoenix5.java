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

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Phoenix;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.isPrestoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodePhoenix5
        extends EnvironmentProvider
{
    private final ResourceProvider configDir;

    @Inject
    public EnvMultinodePhoenix5(StandardMultinode standardMultinode, Phoenix phoenix, DockerFiles dockerFiles)
    {
        super(standardMultinode, phoenix);
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/multinode-phoenix5/");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainers(container -> {
            if (isPrestoContainer(container.getLogicalName())) {
                container.withCopyFileToContainer(forHostPath(configDir.getPath("phoenix.properties")), CONTAINER_PRESTO_ETC + "/catalog/phoenix.properties");
            }
        });
        builder.addConnector("phoenix5");
    }
}
