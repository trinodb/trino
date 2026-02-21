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
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A Trino worker container for multinode product tests.
 * <p>
 * Worker nodes are configured with:
 * <ul>
 *   <li>{@code coordinator=false} - runs as worker only</li>
 *   <li>{@code discovery.uri} - points to the coordinator for cluster discovery</li>
 *   <li>Same catalog configurations as the coordinator</li>
 * </ul>
 * <p>
 * Use {@link MultiNodeTrinoCluster} to orchestrate a coordinator with workers.
 *
 * @see MultiNodeTrinoCluster
 */
public class TrinoWorkerContainer
        extends GenericContainer<TrinoWorkerContainer>
{
    public static final int HTTP_PORT = 8080;
    private static final String DEFAULT_IMAGE = "trinodb/trino";

    private static final String CONFIG_PROPERTIES_TEMPLATE = """
            coordinator=false
            http-server.http.port=%d
            discovery.uri=%s
            """;

    private final Map<String, Map<String, String>> catalogs = new HashMap<>();
    private final Map<String, String> configProperties = new HashMap<>();
    private Map<String, String> exchangeManagerProperties;
    private String discoveryUri;
    private String hdfsSiteXml;

    public TrinoWorkerContainer()
    {
        this(DEFAULT_IMAGE);
    }

    public TrinoWorkerContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(HTTP_PORT);
        waitingFor(Wait.forHttp("/v1/info")
                .forPort(HTTP_PORT)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofSeconds(90)));
    }

    /**
     * Sets the discovery URI (coordinator address) for this worker.
     * Must be called before start().
     *
     * @param discoveryUri the URI of the coordinator (e.g., "http://coordinator:8080")
     * @return this container for chaining
     */
    public TrinoWorkerContainer withDiscoveryUri(String discoveryUri)
    {
        this.discoveryUri = requireNonNull(discoveryUri, "discoveryUri is null");
        return this;
    }

    /**
     * Adds a catalog configuration. Must match the coordinator's catalog configuration.
     *
     * @param catalogName the catalog name
     * @param properties the catalog properties
     * @return this container for chaining
     */
    public TrinoWorkerContainer withCatalog(String catalogName, Map<String, String> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(properties, "properties is null");
        catalogs.put(catalogName, new HashMap<>(properties));
        return this;
    }

    /**
     * Adds a config property to config.properties.
     * These are appended to the base worker configuration.
     *
     * @param key the property key
     * @param value the property value
     * @return this container for chaining
     */
    public TrinoWorkerContainer withConfigProperty(String key, String value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        configProperties.put(key, value);
        return this;
    }

    /**
     * Adds multiple config properties to config.properties.
     *
     * @param properties the properties to add
     * @return this container for chaining
     */
    public TrinoWorkerContainer withConfigProperties(Map<String, String> properties)
    {
        requireNonNull(properties, "properties is null");
        configProperties.putAll(properties);
        return this;
    }

    /**
     * Configures the exchange manager for this worker.
     * Required for fault-tolerant execution.
     *
     * @param properties the exchange manager properties
     * @return this container for chaining
     */
    public TrinoWorkerContainer withExchangeManager(Map<String, String> properties)
    {
        requireNonNull(properties, "properties is null");
        this.exchangeManagerProperties = new HashMap<>(properties);
        return this;
    }

    /**
     * Configures the container with HDFS client settings.
     *
     * @param hdfsSiteXml HDFS configuration XML from HadoopContainer
     * @return this container for chaining
     */
    public TrinoWorkerContainer withHdfsConfiguration(String hdfsSiteXml)
    {
        this.hdfsSiteXml = requireNonNull(hdfsSiteXml, "hdfsSiteXml is null");
        return this;
    }

    /**
     * Sets the Docker network for this container.
     *
     * @param network the Docker network
     * @return this container for chaining
     */
    @Override
    public TrinoWorkerContainer withNetwork(Network network)
    {
        return super.withNetwork(requireNonNull(network, "network is null"));
    }

    @Override
    public void start()
    {
        if (discoveryUri == null) {
            throw new IllegalStateException("discoveryUri must be set before starting worker container");
        }

        // Configure config.properties for worker mode
        StringBuilder configBuilder = new StringBuilder(CONFIG_PROPERTIES_TEMPLATE.formatted(HTTP_PORT, discoveryUri));
        for (Map.Entry<String, String> entry : configProperties.entrySet()) {
            configBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        withCopyToContainer(
                Transferable.of(configBuilder.toString()),
                "/etc/trino/config.properties");

        // Add catalog configurations
        for (Map.Entry<String, Map<String, String>> catalog : catalogs.entrySet()) {
            String catalogName = catalog.getKey();
            Map<String, String> properties = catalog.getValue();
            String propertiesContent = formatProperties(properties);
            withCopyToContainer(
                    Transferable.of(propertiesContent),
                    "/etc/trino/catalog/" + catalogName + ".properties");
        }

        // Add exchange manager configuration if provided
        if (exchangeManagerProperties != null) {
            withCopyToContainer(
                    Transferable.of(formatProperties(exchangeManagerProperties)),
                    "/etc/trino/exchange-manager.properties");
        }

        // Add HDFS configuration if provided
        if (hdfsSiteXml != null) {
            withCopyToContainer(
                    Transferable.of(hdfsSiteXml),
                    "/etc/trino/hdfs-site.xml");
        }

        super.start();
    }

    private static String formatProperties(Map<String, String> properties)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }
}
