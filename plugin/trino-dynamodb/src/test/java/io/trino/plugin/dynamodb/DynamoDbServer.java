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
package io.trino.plugin.dynamodb;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.io.Closeable;
import java.net.URI;

public class DynamoDbServer
        implements Closeable
{
    private static final int DYNAMODB_PORT = 8000;
    private static final String DYNAMODB_LOCAL_IMAGE = "amazon/dynamodb-local:2.5.0";

    private final GenericContainer<?> container;

    public DynamoDbServer()
    {
        container = new GenericContainer<>(DYNAMODB_LOCAL_IMAGE)
                .withExposedPorts(DYNAMODB_PORT)
                .withCommand("-jar DynamoDBLocal.jar -inMemory -sharedDb")
                .waitingFor(Wait.forListeningPort());
        container.start();
    }

    public URI getEndpointUrl()
    {
        return URI.create("http://%s:%d".formatted(container.getHost(), container.getMappedPort(DYNAMODB_PORT)));
    }

    public DynamoDbClient createClient()
    {
        return DynamoDbClient.builder()
                .endpointOverride(getEndpointUrl())
                .region(Region.of("us-east-1"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();
    }

    @Override
    public void close()
    {
        container.close();
    }
}
