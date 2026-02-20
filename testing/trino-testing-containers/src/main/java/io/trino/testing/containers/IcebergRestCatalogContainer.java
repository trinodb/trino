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
 * A container for the Iceberg REST Catalog server.
 * <p>
 * This container provides a REST catalog implementation that can be used by both
 * Trino and Spark for Iceberg table metadata operations. The REST catalog stores
 * metadata independently from the Hive Metastore, providing a unified catalog
 * interface across multiple engines.
 * <p>
 * Key features:
 * <ul>
 *   <li>REST API on port 8181</li>
 *   <li>HDFS warehouse support</li>
 *   <li>Compatible with Iceberg REST catalog specification</li>
 * </ul>
 */
public class IcebergRestCatalogContainer
        extends GenericContainer<IcebergRestCatalogContainer>
{
    private static final String IMAGE = "tabulario/iceberg-rest:1.5.0";

    public static final String HOST_NAME = "iceberg-with-rest";
    public static final int REST_PORT = 8181;

    private static final String DEFAULT_WAREHOUSE = "hdfs://hadoop-master:9000/user/hive/warehouse";

    private final String warehouse;

    public IcebergRestCatalogContainer()
    {
        this(DEFAULT_WAREHOUSE);
    }

    public IcebergRestCatalogContainer(String warehouse)
    {
        super(DockerImageName.parse(IMAGE));
        this.warehouse = warehouse;
        withExposedPorts(REST_PORT);
        withEnv("CATALOG_WAREHOUSE", warehouse);
        withEnv("REST_PORT", String.valueOf(REST_PORT));
        withEnv("HADOOP_USER_NAME", "hive");
        waitingFor(Wait.forListeningPort()
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Returns the internal REST catalog URI for use by containers on the same network.
     */
    public String getRestUri()
    {
        return "http://" + HOST_NAME + ":" + REST_PORT + "/";
    }

    /**
     * Returns the externally accessible REST catalog URI for connecting from the host.
     */
    public String getExternalRestUri()
    {
        return "http://" + getHost() + ":" + getMappedPort(REST_PORT) + "/";
    }

    /**
     * Returns the warehouse location configured for this REST catalog.
     */
    public String getWarehouse()
    {
        return warehouse;
    }
}
