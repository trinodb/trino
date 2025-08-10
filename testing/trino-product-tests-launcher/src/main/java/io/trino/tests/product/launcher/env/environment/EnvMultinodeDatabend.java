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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeDatabend
        extends EnvironmentProvider
{
    public static final int DATABEND_PORT = 8000;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeDatabend(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(ImmutableList.of(standardMultinode));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector("databend", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-databend/databend.properties")));
        builder.addContainer(createDatabend());
    }

    @SuppressWarnings("resource")
    private DockerContainer createDatabend()
    {
        DockerContainer container = new DockerContainer("datafuselabs/databend:v1.2.615", "databend")
                .withEnv("QUERY_DEFAULT_USER", "databend")
                .withEnv("QUERY_DEFAULT_PASSWORD", "databend");
        portBinder.exposePort(container, DATABEND_PORT);

        return container;
    }
}
