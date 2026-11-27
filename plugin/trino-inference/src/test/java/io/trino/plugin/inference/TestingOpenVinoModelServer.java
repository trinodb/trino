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
package io.trino.plugin.inference;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;

/**
 * OpenVINO Model Server, which implements the Open Inference Protocol, serving the models in {@code openvino/models} in the test resources.
 * See {@code openvino/generate_models.py} for the models.
 */
public final class TestingOpenVinoModelServer
        implements Closeable
{
    private static final String IMAGE = "openvino/model_server:2026.4.1";
    private static final int HTTP_PORT = 8000;
    private static final Pattern INFER_REQUESTS = Pattern.compile(
            "^ovms_requests_success\\{api=\"KServe\",interface=\"REST\",method=\"ModelInfer\",name=\"([^\"]+)\",version=\"\\d+\"} (\\d+)$",
            Pattern.MULTILINE);

    private final GenericContainer<?> container;
    private final HttpClient httpClient = new JettyHttpClient();

    public TestingOpenVinoModelServer()
    {
        //noinspection resource
        container = new GenericContainer<>(IMAGE)
                .withCopyFileToContainer(MountableFile.forClasspathResource("openvino/models", 0755), "/models")
                .withCommand("--config_path", "/models/config.json", "--rest_port", String.valueOf(HTTP_PORT), "--port", "9000", "--metrics_enable")
                .withExposedPorts(HTTP_PORT)
                .waitingFor(Wait.forHttp("/v2/health/ready").forPort(HTTP_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));
        container.start();
    }

    public URI getUri()
    {
        return URI.create("http://%s:%s".formatted(container.getHost(), container.getMappedPort(HTTP_PORT)));
    }

    /**
     * @return the number of successful inference requests for all versions of the model
     */
    public long getInferenceRequests(String modelName)
    {
        Request request = prepareGet()
                .setUri(getUri().resolve("/metrics"))
                .build();
        String metrics = httpClient.execute(request, createStringResponseHandler()).getBody();
        long requests = 0;
        Matcher matcher = INFER_REQUESTS.matcher(metrics);
        while (matcher.find()) {
            if (matcher.group(1).equals(modelName)) {
                requests += Long.parseLong(matcher.group(2));
            }
        }
        return requests;
    }

    @Override
    public void close()
    {
        httpClient.close();
        container.close();
    }
}
