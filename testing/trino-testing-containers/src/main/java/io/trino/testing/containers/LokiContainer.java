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

import com.google.common.net.HostAndPort;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * A Grafana Loki container for testing log queries.
 * <p>
 * Loki is a log aggregation system designed to store and query logs.
 * This container provides the Loki HTTP API for pushing and querying logs
 * during testing.
 */
public class LokiContainer
        extends GenericContainer<LokiContainer>
{
    private static final String IMAGE = "grafana/loki";
    private static final String VERSION = "2.9.0";

    public static final String HOST_NAME = "loki";
    public static final int LOKI_HTTP_PORT = 3100;

    public LokiContainer()
    {
        this(IMAGE + ":" + VERSION);
    }

    public LokiContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(LOKI_HTTP_PORT);
        waitingFor(Wait.forHttp("/ready")
                .forPort(LOKI_HTTP_PORT)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofSeconds(60)));
    }

    /**
     * Returns the HTTP endpoint as HostAndPort for connecting from the host.
     */
    public HostAndPort getHttpEndpoint()
    {
        return HostAndPort.fromParts(getHost(), getMappedPort(LOKI_HTTP_PORT));
    }

    /**
     * Returns the full HTTP URL for connecting from the host.
     */
    public String getHttpUrl()
    {
        return "http://" + getHost() + ":" + getMappedPort(LOKI_HTTP_PORT);
    }

    /**
     * Returns the internal HTTP URL for connecting from other containers on the same network.
     */
    public String getInternalHttpUrl()
    {
        return "http://" + HOST_NAME + ":" + LOKI_HTTP_PORT;
    }
}
