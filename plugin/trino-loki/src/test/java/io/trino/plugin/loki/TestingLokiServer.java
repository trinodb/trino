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
package io.trino.plugin.loki;

import io.github.jeschkies.loki.client.LokiClient;
import io.github.jeschkies.loki.client.LokiClientConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;

public class TestingLokiServer
        implements Closeable
{
    private static final int LOKI_PORT = 3100;
    private static final String DEFAULT_VERSION = "3.2.0";

    private final GenericContainer<?> loki;

    public TestingLokiServer()
    {
        this(DEFAULT_VERSION);
    }

    private TestingLokiServer(String version)
    {
        //noinspection resource
        loki = new GenericContainer<>("grafana/loki:%s".formatted(version))
                .withExposedPorts(LOKI_PORT)
                .waitingFor(Wait.forHttp("/ready").forResponsePredicate(response -> response.contains("ready")))
                .withStartupTimeout(Duration.ofMinutes(6));
        loki.start();
    }

    public LokiClient createLokiClient()
    {
        LokiClientConfig config = new LokiClientConfig(getUri(), Duration.ofSeconds(10));
        return new LokiClient(config);
    }

    public URI getUri()
    {
        return URI.create("http://localhost:" + loki.getMappedPort(LOKI_PORT));
    }

    @Override
    public void close()
    {
        loki.close();
    }
}
