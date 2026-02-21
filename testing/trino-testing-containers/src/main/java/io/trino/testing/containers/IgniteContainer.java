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

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * An Apache Ignite container for Trino testing.
 * <p>
 * Apache Ignite is an in-memory computing platform that provides distributed
 * caching and SQL capabilities. This container exposes the thin client port
 * which is also used for JDBC connections.
 */
public class IgniteContainer
        extends GenericContainer<IgniteContainer>
{
    private static final String IMAGE = "apacheignite/ignite";
    private static final String AMD64_VERSION = "2.17.0";
    private static final String ARM64_VERSION = "2.17.0-arm64";
    private static final String IMAGE_OVERRIDE_PROPERTY = "trino.product-tests.ignite.image";

    public static final String HOST_NAME = "ignite";
    public static final int IGNITE_PORT = 10800;

    public IgniteContainer()
    {
        this(resolveImageName());
    }

    private IgniteContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(IGNITE_PORT);
        waitingFor(Wait.forListeningPort()
                .withStartupTimeout(Duration.ofSeconds(60)));
    }

    private static String resolveImageName()
    {
        String override = System.getProperty(IMAGE_OVERRIDE_PROPERTY);
        if (override != null && !override.isBlank()) {
            return override;
        }

        String dockerArch = DockerClientFactory.lazyClient().versionCmd().exec().getArch();
        if ("arm64".equalsIgnoreCase(dockerArch) || "linux/arm64".equalsIgnoreCase(dockerArch)) {
            return IMAGE + ":" + ARM64_VERSION;
        }
        return IMAGE + ":" + AMD64_VERSION;
    }

    /**
     * Returns the JDBC URL for connecting to Ignite from the host.
     */
    public String getJdbcUrl()
    {
        return "jdbc:ignite:thin://" + getHost() + ":" + getMappedPort(IGNITE_PORT);
    }

    /**
     * Returns the internal JDBC URL for connecting from other containers on the same network.
     */
    public String getInternalJdbcUrl()
    {
        return "jdbc:ignite:thin://" + HOST_NAME + ":" + IGNITE_PORT;
    }
}
