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
package io.trino.plugin.ydb;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static java.lang.String.format;

public class TestingYdbServer
        implements AutoCloseable
{
    private static final DockerImageName IMAGE = DockerImageName.parse("ydbplatform/local-ydb").withTag("26.3");
    private static final int GRPC_PORT = 2136;

    private final GenericContainer<?> container;

    public TestingYdbServer()
    {
        container = new GenericContainer<>(IMAGE)
                .withExposedPorts(GRPC_PORT)
                .withEnv("GRPC_PORT", String.valueOf(GRPC_PORT))
                .withEnv("GRPC_TLS_PORT", "2135")
                .withEnv("MON_PORT", "8765")
                .withEnv("YDB_USE_IN_MEMORY_PDISKS", "true")
                .waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(5)));
        container.start();
    }

    public String getJdbcUrl()
    {
        // Discovery returns the address inside the container, which is not reachable from the host
        return format("jdbc:ydb:grpc://%s:%s/local?useDiscovery=false", container.getHost(), container.getMappedPort(GRPC_PORT));
    }

    @Override
    public void close()
    {
        container.close();
    }
}
