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
import io.prestosql.tests.product.launcher.env.common.AbstractEnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;
import io.prestosql.tests.product.launcher.testcontainers.SelectedPortWaitStrategy;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static io.prestosql.tests.product.launcher.testcontainers.TestcontainersUtil.exposePort;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

@TestsEnvironment
public final class SinglenodeSqlserver
        extends AbstractEnvironmentProvider
{
    public static final int SQLSERVER_PORT = 1433;

    private final DockerFiles dockerFiles;

    @Inject
    public SinglenodeSqlserver(Standard standard, DockerFiles dockerFiles)
    {
        super(ImmutableList.of(standard));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    protected void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer("presto-master", container -> container
                .withFileSystemBind(
                        dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-sqlserver/sqlserver.properties"),
                        CONTAINER_PRESTO_ETC + "/catalog/sqlserver.properties",
                        READ_ONLY));

        builder.addContainer("sqlserver", createSqlServer());
    }

    @SuppressWarnings("resource")
    private DockerContainer createSqlServer()
    {
        DockerContainer container = new DockerContainer("microsoft/mssql-server-linux:2017-CU13")
                .withEnv("ACCEPT_EULA", "Y")
                .withEnv("SA_PASSWORD", "SQLServerPass1")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(SQLSERVER_PORT));

        exposePort(container, 1433);

        return container;
    }
}
