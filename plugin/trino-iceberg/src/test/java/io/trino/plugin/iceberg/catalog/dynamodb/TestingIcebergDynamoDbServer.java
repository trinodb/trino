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
package io.trino.plugin.iceberg.catalog.dynamodb;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.time.Duration;

public class TestingIcebergDynamoDbServer
        implements Closeable
{
    private static final int PORT = 8000;

    private final GenericContainer<?> dockerContainer;

    public TestingIcebergDynamoDbServer()
    {
        dockerContainer = new GenericContainer<>("amazon/dynamodb-local:latest")
                .withExposedPorts(PORT)
                .withStartupAttempts(3)
                .waitingFor(Wait.forLogMessage(".*Initializing DynamoDB Local with the following configuration.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        dockerContainer.start();
    }

    public String getEndpointUrl()
    {
        return "http://localhost:" + dockerContainer.getMappedPort(PORT);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
