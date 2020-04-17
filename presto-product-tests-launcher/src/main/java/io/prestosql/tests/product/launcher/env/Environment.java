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
package io.prestosql.tests.product.launcher.env;

import com.github.dockerjava.api.model.Bind;
import io.prestosql.tests.product.launcher.testcontainers.PrintingLogConsumer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.shaded.com.google.common.base.Preconditions.checkState;

public final class Environment
{
    public static final String PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME = Environment.class.getName() + ".ptl-started";
    public static final String PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE = "true";
    public static final String PRODUCT_TEST_LAUNCHER_NETWORK = "ptl-network";

    private final Map<String, DockerContainer> containers;

    public Environment(Map<String, DockerContainer> containers)
    {
        this.containers = requireNonNull(containers, "containers is null");
    }

    public void start()
    {
        try {
            Startables.deepStart(ImmutableList.copyOf(containers.values())).get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Container<?> getContainer(String name)
    {
        return Optional.ofNullable(containers.get(requireNonNull(name, "name is null")))
                .orElseThrow(() -> new IllegalArgumentException("No container with name " + name));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        @SuppressWarnings("resource")
        private Network network = Network.builder()
                .createNetworkCmdModifier(createNetworkCmd ->
                        createNetworkCmd
                                .withName(PRODUCT_TEST_LAUNCHER_NETWORK)
                                .withLabels(ImmutableMap.of(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)))
                .build();
        private Map<String, DockerContainer> containers = new HashMap<>();

        public Builder addContainer(String name, DockerContainer container)
        {
            requireNonNull(name, "name is null");
            checkState(!containers.containsKey(name), "Container with name %s is already registered", name);
            containers.put(name, requireNonNull(container, "container is null"));

            String containerName = "ptl-" + name;
            container
                    .withNetwork(network)
                    .withNetworkAliases(name)
                    .withLabel(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)
                    .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                            .withName(containerName)
                            .withHostName(name));

            return this;
        }

        public Builder configureContainer(String name, Consumer<DockerContainer> configurer)
        {
            requireNonNull(name, "name is null");
            checkState(containers.containsKey(name), "Container with name %s is not registered", name);
            requireNonNull(configurer, "configurer is null").accept(containers.get(name));
            return this;
        }

        public Builder removeContainer(String name)
        {
            requireNonNull(name, "name is null");
            GenericContainer<?> container = containers.remove(name);
            if (container != null) {
                container.close();
            }
            return this;
        }

        public Environment build()
        {
            // write directly to System.out, bypassing logging & io.airlift.log.Logging#rewireStdStreams
            PrintStream out;
            try {
                //noinspection resource
                out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            containers.forEach((name, container) -> {
                container
                        .withLogConsumer(new PrintingLogConsumer(out, format("%-20s| ", name)))
                        .withCreateContainerCmdModifier(createContainerCmd -> {
                            Map<String, Bind> binds = new HashMap<>();
                            for (Bind bind : firstNonNull(createContainerCmd.getBinds(), new Bind[0])) {
                                binds.put(bind.getVolume().getPath(), bind); // last bind wins
                            }
                            createContainerCmd.withBinds(binds.values().toArray(new Bind[0]));
                        });
            });

            return new Environment(containers);
        }
    }
}
