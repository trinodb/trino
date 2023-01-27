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
package io.trino.plugin.prometheus;

import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;

import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class PrometheusServer
        implements Closeable
{
    private static final int PROMETHEUS_PORT = 9090;
    private static final String DEFAULT_VERSION = "v2.15.1";
    public static final String LATEST_VERSION = "v2.35.0";

    public static final String USER = "admin";
    public static final String PASSWORD = "password";
    public static final String PROMETHEUS_QUERY_API = "/api/v1/query?query=up[1d]";

    private final GenericContainer<?> dockerContainer;

    public PrometheusServer()
    {
        this(DEFAULT_VERSION, false);
    }

    public PrometheusServer(String version, boolean enableBasicAuth)
    {
        this.dockerContainer = new GenericContainer<>("prom/prometheus:" + version)
                .withExposedPorts(PROMETHEUS_PORT)
                .waitingFor(Wait.forHttp(PROMETHEUS_QUERY_API).forResponsePredicate(response -> response.contains("\"values\"")))
                .withStartupTimeout(Duration.ofSeconds(360));
        // Basic authentication was introduced in v2.24.0
        if (enableBasicAuth) {
            this.dockerContainer
                    .withCommand("--config.file=/etc/prometheus/prometheus.yml", "--web.config.file=/etc/prometheus/web.yml")
                    .withCopyFileToContainer(forClasspathResource("web.yml"), "/etc/prometheus/web.yml")
                    .waitingFor(Wait.forHttp(PROMETHEUS_QUERY_API).forResponsePredicate(response -> response.contains("\"values\"")).withBasicCredentials(USER, PASSWORD))
                    .withStartupTimeout(Duration.ofSeconds(360));
        }
        this.dockerContainer.start();
    }

    public URI getUri()
    {
        return URI.create("http://" + dockerContainer.getHost() + ":" + dockerContainer.getMappedPort(PROMETHEUS_PORT) + "/");
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
