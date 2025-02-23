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
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment.Builder;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeScyllaDB
        extends EnvironmentProvider
{
    public static final int SCYLLA_PORT = 9042;

    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeScyllaDB(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/multinode-scylladb/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Builder builder)
    {
        builder.addConnector("scylladb", forHostPath(configDir.getPath("scylladb.properties")));
        builder.addContainer(createScyllaDB());
    }

    private DockerContainer createScyllaDB()
    {
        DockerContainer container = new DockerContainer("scylladb/scylla:6.2", "scylladb")
                // Limit SMP to run in a machine having many cores https://github.com/scylladb/scylla/issues/5638
                .withCommand("--smp", "1")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SCYLLA_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, SCYLLA_PORT);

        return container;
    }
}
