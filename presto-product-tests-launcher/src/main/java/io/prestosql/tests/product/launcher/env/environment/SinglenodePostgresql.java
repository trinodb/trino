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
public final class SinglenodePostgresql
        extends AbstractEnvironmentProvider
{
    // Use non-default PostgreSQL port to avoid conflicts with locally installed PostgreSQL if any.
    public static final int POSTGRESQL_PORT = 15432;

    private final DockerFiles dockerFiles;

    @Inject
    public SinglenodePostgresql(Standard standard, DockerFiles dockerFiles)
    {
        super(ImmutableList.of(standard));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    protected void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer("presto-master", container -> container
                .withFileSystemBind(
                        dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-postgresql/postgresql.properties"),
                        CONTAINER_PRESTO_ETC + "/catalog/postgresql.properties",
                        READ_ONLY));

        builder.addContainer("postgresql", createPostgreSql());
    }

    @SuppressWarnings("resource")
    private DockerContainer createPostgreSql()
    {
        DockerContainer container = new DockerContainer("postgres:10.3")
                .withEnv("POSTGRES_PASSWORD", "test")
                .withEnv("POSTGRES_USER", "test")
                .withEnv("POSTGRES_DB", "test")
                .withEnv("PGPORT", Integer.toString(POSTGRESQL_PORT))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(POSTGRESQL_PORT));

        exposePort(container, POSTGRESQL_PORT);

        return container;
    }
}
