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

import com.google.common.io.Closer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class PrometheusServer
        implements Closeable
{
    private static final int PROXY_PORT = 8080;
    private static final String DEFAULT_NGINX_PROXY_VERSION = "1.28.0-alpine";

    private static final int PROMETHEUS_PORT = 9090;
    public static final String DEFAULT_PROMETHEUS_VERSION = "v2.15.1";
    public static final String LATEST_PROMETHEUS_VERSION = "v2.35.0";

    public static final String USER = "admin";
    public static final String PASSWORD = "password";
    public static final String PROMETHEUS_QUERY_API = "/api/v1/query?query=up[1d]";

    private final boolean enableLoggingProxy;
    private final Network network;
    private final GenericContainer<?> prometheusContainer;
    private final GenericContainer<?> proxyContainer;
    private final Set<Consumer<OutputFrame>> requestConsumers = ConcurrentHashMap.newKeySet();
    private final Closer closer = Closer.create();

    public PrometheusServer()
    {
        this(DEFAULT_PROMETHEUS_VERSION, false, false);
    }

    @SuppressWarnings("resource")
    public PrometheusServer(String prometheusVersion, boolean enableBasicAuth, boolean enableLoggingProxy)
    {
        this.enableLoggingProxy = enableLoggingProxy;
        this.network = Network.newNetwork();
        closer.register(network::close);

        this.prometheusContainer = new GenericContainer<>("prom/prometheus:" + prometheusVersion)
                .withNetwork(network)
                .withNetworkAliases("prometheus")
                .withExposedPorts(PROMETHEUS_PORT)
                .waitingFor(Wait.forHttp(PROMETHEUS_QUERY_API).forResponsePredicate(response -> response.contains("\"values\"")))
                .withStartupTimeout(Duration.ofMinutes(6));
        // Basic authentication was introduced in v2.24.0
        if (enableBasicAuth) {
            this.prometheusContainer
                    .withNetwork(network)
                    .withCommand("--config.file=/etc/prometheus/prometheus.yml", "--web.config.file=/etc/prometheus/web.yml")
                    .withCopyFileToContainer(forClasspathResource("web.yml"), "/etc/prometheus/web.yml")
                    .waitingFor(Wait.forHttp(PROMETHEUS_QUERY_API).forResponsePredicate(response -> response.contains("\"values\"")).withBasicCredentials(USER, PASSWORD))
                    .withStartupTimeout(Duration.ofMinutes(6));
        }
        this.prometheusContainer.start();
        closer.register(prometheusContainer::close);

        if (enableLoggingProxy) {
            // Nginx-based HTTP logging proxy
            this.proxyContainer = new GenericContainer<>("nginx:" + DEFAULT_NGINX_PROXY_VERSION)
                    .withNetwork(network)
                    .withNetworkAliases("proxy")
                    .withExposedPorts(PROXY_PORT)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("nginx.conf"),
                            "/etc/nginx/nginx.conf")
                    .dependsOn(prometheusContainer)
                    .withLogConsumer(frame ->
                            requestConsumers.forEach(consumer -> consumer.accept(frame)));

            this.proxyContainer.start();
            closer.register(proxyContainer::close);
        }
        else {
            this.proxyContainer = null;
        }
    }

    public URI getUri()
    {
        if (enableLoggingProxy) {
            return URI.create("http://" + proxyContainer.getHost() + ":" + proxyContainer.getMappedPort(PROXY_PORT) + "/");
        }
        return URI.create("http://" + prometheusContainer.getHost() + ":" + prometheusContainer.getMappedPort(PROMETHEUS_PORT) + "/");
    }

    /**
     * Records all incoming HTTP requests during the operation and returns its result.
     */
    public <T> T recordRequestsFor(RequestsRecorder recorder, Supplier<T> operation)
    {
        startTracingRequests(recorder);
        try {
            return operation.get();
        }
        finally {
            stopTracingRequests(recorder);
        }
    }

    private void startTracingRequests(Consumer<OutputFrame> consumer)
    {
        requestConsumers.add(consumer);
    }

    private void stopTracingRequests(Consumer<OutputFrame> consumer)
    {
        requestConsumers.remove(consumer);
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
