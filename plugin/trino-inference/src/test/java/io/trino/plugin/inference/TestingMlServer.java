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

import com.google.common.collect.ImmutableList;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.List;

/**
 * MLServer with its scikit-learn runtime, serving the models that {@code mlserver/create_models.py} in the test resources creates when the container starts.
 */
public final class TestingMlServer
        implements Closeable
{
    private static final String IMAGE = "seldonio/mlserver:1.7.1-sklearn";
    private static final int HTTP_PORT = 8080;
    private static final List<String> MODELS = ImmutableList.of(
            "house-price",
            "iris",
            "iris-probabilities",
            "iris-variable-length",
            "undeclared-metadata",
            "fixed-rows",
            "unsupported-data-type",
            "wrong-output-shape",
            "wrong-output-data-type");

    private final GenericContainer<?> container;

    public TestingMlServer()
    {
        // MLServer reports that it is ready while it still loads the models, so wait until every model is ready
        WaitAllStrategy modelsReady = new WaitAllStrategy();
        for (String model : MODELS) {
            modelsReady.withStrategy(Wait.forHttp("/v2/models/%s/ready".formatted(model)).forPort(HTTP_PORT));
        }
        //noinspection resource
        container = new GenericContainer<>(IMAGE)
                .withCopyFileToContainer(MountableFile.forClasspathResource("mlserver/create_models.py"), "/tmp/create_models.py")
                .withCommand("bash", "-c", "python /tmp/create_models.py /tmp/models && mlserver start /tmp/models")
                .withExposedPorts(HTTP_PORT)
                .waitingFor(modelsReady)
                .withStartupTimeout(Duration.ofMinutes(5));
        container.start();
    }

    public URI getUri()
    {
        return URI.create("http://%s:%s".formatted(container.getHost(), container.getMappedPort(HTTP_PORT)));
    }

    @Override
    public void close()
    {
        container.close();
    }
}
