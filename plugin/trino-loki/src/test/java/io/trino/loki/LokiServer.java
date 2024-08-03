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
package io.trino.loki;

import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;

import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class LokiServer
        implements Closeable
{
    private static final int LOKI_PORT = 3100;
    private static final String DEFAULT_VERSION = "3.1.0";

    public static final String USER = "admin";
    public static final String PASSWORD = "password";
    public static final String LOKI_QUERY_API = "/loki/api/v1/query";

    private final GenericContainer<?> dockerContainer;

    public LokiServer()
    {
        this(DEFAULT_VERSION, false);
    }

    public LokiServer(String version, boolean enableBasicAuth)
    {
        this.dockerContainer = new GenericContainer<>("grafana/loki:" + version)
                .withExposedPorts(LOKI_PORT);
                //.waitingFor(Wait.forHttp(LOKI_QUERY_API).forResponsePredicate(response -> response.contains("\"status\"")))
                //.withStartupTimeout(Duration.ofSeconds(360));
        // Basic authentication was introduced in v2.24.0
        if (enableBasicAuth) {
            this.dockerContainer
                    .withCommand("--config.file=/etc/prometheus/prometheus.yml", "--web.config.file=/etc/prometheus/web.yml")
                    .withCopyFileToContainer(forClasspathResource("web.yml"), "/etc/prometheus/web.yml")
                    .waitingFor(Wait.forHttp(LOKI_QUERY_API).forResponsePredicate(response -> response.contains("\"values\"")).withBasicCredentials(USER, PASSWORD))
                    .withStartupTimeout(Duration.ofSeconds(360));
        }
        this.dockerContainer.start();
    }

    public URI getUri()
    {
        return URI.create("http://" + dockerContainer.getHost() + ":" + dockerContainer.getMappedPort(LOKI_PORT) + "/");
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return dockerContainer.getContainerId() != null;
    }
}
