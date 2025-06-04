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
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeSecretsProvider
        extends EnvironmentProvider
{
    // Use non-default PostgreSQL port to avoid conflicts with locally installed PostgreSQL if any.
    public static final int POSTGRESQL_PORT = 15432;

    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeSecretsProvider(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/multinode-secrets-provider/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> configureTrinoContainer(container, "master"));
        builder.configureContainer(WORKER, container -> configureTrinoContainer(container, "worker"));

        builder.addPasswordAuthenticator(
                "file",
                forHostPath(configDir.getPath("authenticator.properties")),
                CONTAINER_TRINO_ETC + "/authenticator.properties");
        builder.configureContainer(
                COORDINATOR,
                container -> container
                        .withCopyFileToContainer(
                                forHostPath(configDir.getPath("password.db")),
                                CONTAINER_TRINO_ETC + "/password.db"));

        builder.addContainer(createPostgreSql());
        builder.addConnector(
                "postgresql",
                forHostPath(configDir.getPath("catalog/postgresql.properties")));
        configureTempto(builder, configDir);
    }

    @SuppressWarnings("resource")
    private DockerContainer createPostgreSql()
    {
        // Use the oldest supported PostgreSQL version
        DockerContainer container = new DockerContainer("postgres:11", "postgresql")
                .withEnv("POSTGRES_PASSWORD", "test")
                .withEnv("POSTGRES_USER", "test")
                .withEnv("POSTGRES_DB", "test")
                .withEnv("PGPORT", Integer.toString(POSTGRESQL_PORT))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(POSTGRESQL_PORT));

        portBinder.exposePort(container, POSTGRESQL_PORT);

        return container;
    }

    private void configureTrinoContainer(DockerContainer container, String role)
    {
        container
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("secrets.toml")),
                        "/docker/trino-product-tests/conf/trino/etc/secrets.toml")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("generateSecrets.sh")),
                        "/docker/presto-init.d/generateSecrets.sh")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("config-%s.properties".formatted(role))),
                        CONTAINER_TRINO_CONFIG_PROPERTIES)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withDomainName("docker.cluster"));
    }
}
