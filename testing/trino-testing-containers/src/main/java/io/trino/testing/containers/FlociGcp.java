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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static java.net.http.HttpResponse.BodyHandlers.ofString;

public final class FlociGcp
        extends GenericContainer<FlociGcp>
{
    public static final String FLOCI_GCP_IMAGE = "floci/floci-gcp:0.3.0";
    public static final String FLOCI_GCP_PROJECT_ID = "floci-local";

    private static final String FLOCI_GCP_NETWORK_ALIAS = "floci-gcp";
    private static final int FLOCI_GCP_PORT = 4588;

    public FlociGcp()
    {
        super(DockerImageName.parse(FLOCI_GCP_IMAGE));
        addExposedPort(FLOCI_GCP_PORT);
        withNetworkAliases(FLOCI_GCP_NETWORK_ALIAS);
        waitingFor(Wait.forLogMessage(".*=== floci-gcp Ready ===.*\\n", 1));
    }

    public URI getEndpoint()
    {
        return URI.create("http://%s:%s".formatted(getHost(), getMappedPort(FLOCI_GCP_PORT)));
    }

    public URI getContainerEndpoint()
    {
        return URI.create("http://%s:%s".formatted(FLOCI_GCP_NETWORK_ALIAS, FLOCI_GCP_PORT));
    }

    public void createBucket(String bucketName)
    {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(getEndpoint().resolve("/storage/v1/b?project=" + FLOCI_GCP_PROJECT_ID))
                .header("Content-Type", "application/json")
                .POST(ofString("{\"name\":\"%s\"}".formatted(bucketName)))
                .build();
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(request, ofString());
            if (response.statusCode() / 100 != 2) {
                throw new IllegalStateException("Failed to create bucket: " + response.body());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
