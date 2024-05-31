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
package io.trino.plugin.openlineage;

import com.google.common.io.Closer;
import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static java.lang.String.format;

public class MarquezServer
        implements Closeable
{
    private static final String MARQUEZ_CONFIG;
    private static final String MARQUEZ_HOST;
    private static final int MARQUEZ_PORT;
    private static final int MARQUEZ_UI_PORT;
    private static final int MARQUEZ_ADMIN_PORT;
    private static final String MARQUEZ_HEALTCHECK_API;
    private static final String POSTGRES_HOST;
    private static final String POSTGRES_DB;
    private static final int POSTGRES_PORT;
    private static final String POSTGRES_USER;
    private static final String POSTGRES_PASSWORD;
    private static final String DEFAULT_VERSION;

    private Network network;
    private final Closer closer = Closer.create();

    static {
        MARQUEZ_CONFIG = "/opt/marquez/marquez.test.yaml";
        MARQUEZ_HOST = "marquez";
        MARQUEZ_PORT = 5000;
        MARQUEZ_UI_PORT = 3000;
        MARQUEZ_ADMIN_PORT = 5001;
        MARQUEZ_HEALTCHECK_API = "/ping";
        POSTGRES_HOST = "postgres";
        POSTGRES_DB = "marquez";
        POSTGRES_PORT = 5432;
        POSTGRES_USER = "marquez";
        POSTGRES_PASSWORD = "marquez";
        DEFAULT_VERSION = "0.46.0";
    }

    private final GenericContainer<?> dockerContainerAPI;
    private final PostgreSQLContainer<?> dockerContainerPostgres;
    private final Optional<GenericContainer<?>> dockerWebUIContainerAPI;

    public MarquezServer()
    {
        this(DEFAULT_VERSION);
    }

    public MarquezServer(String version)
    {
        network = Network.newNetwork();
        closer.register(this.network::close);

        this.dockerContainerPostgres = new PostgreSQLContainer<>("postgres:14")
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_HOST)
                .withDatabaseName(POSTGRES_DB)
                .withUsername(POSTGRES_USER)
                .withPassword(POSTGRES_PASSWORD)
                .withStartupTimeout(Duration.ofSeconds(360));

        this.dockerContainerPostgres.start();
        closer.register(this.dockerContainerPostgres::close);

        this.dockerContainerAPI = new GenericContainer<>("marquezproject/marquez:" + version)
                .withNetwork(network)
                .withNetworkAliases(MARQUEZ_HOST)
                .withExposedPorts(MARQUEZ_PORT, MARQUEZ_ADMIN_PORT)
                .dependsOn(this.dockerContainerPostgres)
                .withEnv("MARQUEZ_PORT", String.valueOf(MARQUEZ_PORT))
                .withEnv("MARQUEZ_ADMIN_PORT", String.valueOf(MARQUEZ_ADMIN_PORT))
                .withEnv("POSTGRES_URL", getPostgresUri())
                .withEnv("POSTGRES_USER", "marquez")
                .withEnv("POSTGRES_PASSWORD", "marquez")
                .withEnv("MARQUEZ_CONFIG", MARQUEZ_CONFIG)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("marquez.yaml"),
                        MARQUEZ_CONFIG)
                .withCommand("./entrypoint.sh")
                .waitingFor(Wait.forHttp(MARQUEZ_HEALTCHECK_API)
                        .forPort(MARQUEZ_ADMIN_PORT)
                        .forStatusCode(200))
                .withStartupTimeout(Duration.ofSeconds(360));

        this.dockerContainerAPI.start();
        closer.register(this.dockerContainerAPI::close);

        this.dockerWebUIContainerAPI = Optional.of(
                new GenericContainer<>("marquezproject/marquez-web:" + version)
                .withNetwork(network)
                .withExposedPorts(MARQUEZ_UI_PORT)
                .dependsOn(this.dockerContainerAPI)
                .withEnv("MARQUEZ_HOST", MARQUEZ_HOST)
                .withEnv("MARQUEZ_PORT", String.valueOf(MARQUEZ_PORT))
                .withStartupTimeout(Duration.ofSeconds(360)));

        this.dockerWebUIContainerAPI.ifPresent(container ->
        {
            container.start();
            closer.register(container::close);
        });
    }

    private String getPostgresUri()
    {
        return format("jdbc:postgresql://%s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB);
    }

    public URI getMarquezUri()
    {
        return URI.create(format("http://%s:%s", dockerContainerAPI.getHost(), dockerContainerAPI.getMappedPort(MARQUEZ_PORT)));
    }

    public Optional<URI> getMarquezWebUIUri()
    {
        if (this.dockerWebUIContainerAPI.isPresent()) {
            return Optional.of(
                URI.create(format("http://%s:%s", dockerWebUIContainerAPI.get().getHost(), dockerWebUIContainerAPI.get().getMappedPort(MARQUEZ_UI_PORT))));
        }
        return Optional.empty();
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return dockerContainerAPI.getContainerId() != null;
    }
}
