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
package io.trino.testing.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * A reusable Nessie catalog server container for Iceberg product tests.
 * <p>
 * Nessie provides a Git-like version control system for data lakes, enabling:
 * <ul>
 *   <li>Branch-based isolation for data changes</li>
 *   <li>Commits and history tracking for table operations</li>
 *   <li>Multi-table transactions</li>
 * </ul>
 * <p>
 * This container runs the Nessie server with in-memory storage, suitable for testing.
 * The Nessie API is available on port 19120.
 */
public class NessieContainer
        extends GenericContainer<NessieContainer>
{
    private static final String IMAGE = "ghcr.io/projectnessie/nessie";
    private static final String VERSION = "0.107.0";

    public static final String HOST_NAME = "nessie-server";
    public static final int NESSIE_PORT = 19120;

    public NessieContainer()
    {
        this(IMAGE + ":" + VERSION);
    }

    public NessieContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(NESSIE_PORT);
        withEnv("NESSIE_VERSION_STORE_TYPE", "IN_MEMORY");
        withEnv("QUARKUS_HTTP_PORT", String.valueOf(NESSIE_PORT));
        waitingFor(Wait.forHttp("/api/v2/config")
                .forPort(NESSIE_PORT)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Returns the Nessie API URI for use within the Docker network.
     */
    public String getNessieUri()
    {
        return "http://" + HOST_NAME + ":" + NESSIE_PORT + "/api/v2";
    }

    /**
     * Returns the externally accessible Nessie API URI for connecting from the host.
     */
    public String getExternalNessieUri()
    {
        return "http://" + getHost() + ":" + getMappedPort(NESSIE_PORT) + "/api/v2";
    }
}
