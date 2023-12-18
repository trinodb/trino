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
package io.trino.plugin.opa;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

public class OpaContainer
        implements Startable
{
    private static final int OPA_PORT = 8181;
    private static final String OPA_BASE_PATH = "v1/data/trino/";
    private static final String OPA_POLICY_PUSH_BASE_PATH = "v1/policies/trino";

    private final GenericContainer<?> container;
    private URI resolvedUri;

    public OpaContainer()
    {
        this.container = new GenericContainer<>(DockerImageName.parse("openpolicyagent/opa:latest"))
                .withCommand("run", "--server", "--addr", ":%d".formatted(OPA_PORT), "--set", "decision_logs.console=true")
                .withExposedPorts(OPA_PORT)
                .waitingFor(Wait.forListeningPort());
    }

    @Override
    public synchronized void start()
    {
        this.container.start();
        this.resolvedUri = null;
    }

    @Override
    public synchronized void stop()
    {
        this.container.stop();
        this.resolvedUri = null;
    }

    public synchronized URI getOpaServerUri()
    {
        if (!container.isRunning()) {
            this.resolvedUri = null;
            throw new IllegalStateException("Container is not running");
        }
        if (this.resolvedUri == null) {
            this.resolvedUri = URI.create(String.format("http://%s:%d/", container.getHost(), container.getMappedPort(OPA_PORT)));
        }
        return this.resolvedUri;
    }

    public URI getOpaUriForPolicyPath(String relativePath)
    {
        return getOpaServerUri().resolve(OPA_BASE_PATH + relativePath);
    }

    public void submitPolicy(String policyString)
            throws IOException, InterruptedException
    {
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> policyResponse =
                httpClient.send(
                        HttpRequest.newBuilder(getOpaServerUri().resolve(OPA_POLICY_PUSH_BASE_PATH))
                                   .PUT(HttpRequest.BodyPublishers.ofString(policyString))
                                   .header("Content-Type", "text/plain").build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(policyResponse.statusCode()).withFailMessage("Failed to submit policy: %s", policyResponse.body()).isEqualTo(200);
    }
}
