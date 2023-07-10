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
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeSqlserver
        extends EnvironmentProvider
{
    public static final int SQLSERVER_PORT = 1433;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeSqlserver(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(ImmutableList.of(standardMultinode));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector("sqlserver", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-sqlserver/sqlserver.properties")));
        builder.addContainer(createSqlServer());
    }

    @SuppressWarnings("resource")
    private DockerContainer createSqlServer()
    {
        DockerContainer container = new DockerContainer("mcr.microsoft.com/mssql/server:2017-CU13", "sqlserver")
                .withEnv("ACCEPT_EULA", "Y")
                .withEnv("SA_PASSWORD", "SQLServerPass1")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SQLSERVER_PORT));

        portBinder.exposePort(container, 1433);

        return container;
    }
}
