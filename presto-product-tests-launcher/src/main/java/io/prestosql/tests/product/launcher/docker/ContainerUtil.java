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
package io.prestosql.tests.product.launcher.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.ListNetworksCmd;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Network;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import org.testcontainers.DockerClientFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.tests.product.launcher.docker.DockerFiles.createTemporaryDirectoryForDocker;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.isRegularFile;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.Files.write;
import static java.util.Objects.requireNonNull;

public final class ContainerUtil
{
    private ContainerUtil() {}

    public static void killContainers(DockerClient dockerClient, Function<ListContainersCmd, ListContainersCmd> filter)
    {
        while (true) {
            ListContainersCmd listContainersCmd = filter.apply(dockerClient.listContainersCmd()
                    .withShowAll(true));

            List<Container> containers = listContainersCmd.exec();
            if (containers.isEmpty()) {
                break;
            }
            for (Container container : containers) {
                try {
                    dockerClient.removeContainerCmd(container.getId())
                            .withForce(true)
                            .exec();
                }
                catch (ConflictException | NotFoundException ignored) {
                }
            }
        }
    }

    public static void removeNetworks(DockerClient dockerClient, Function<ListNetworksCmd, ListNetworksCmd> filter)
    {
        ListNetworksCmd listNetworksCmd = filter.apply(dockerClient.listNetworksCmd());
        List<Network> networks = listNetworksCmd.exec();
        for (Network network : networks) {
            dockerClient.removeNetworkCmd(network.getId())
                    .exec();
        }
    }

    public static void killContainersReaperContainer(DockerClient dockerClient)
    {
        @SuppressWarnings("resource")
        Void ignore = dockerClient.removeContainerCmd("testcontainers-ryuk-" + DockerClientFactory.SESSION_ID)
                .withForce(true)
                .exec();
    }

    public static void exposePort(DockerContainer container, int port)
    {
        container.addExposedPort(port);
        container.withFixedExposedPort(port, port);
    }

    public static void enableJavaDebugger(DockerContainer container, String jvmConfigPath, int debugPort)
    {
        requireNonNull(jvmConfigPath, "jvmConfigPath is null");
        container.withCreateContainerCmdModifier(createContainerCmd -> enableDebuggerInJvmConfig(createContainerCmd, jvmConfigPath, debugPort));
        exposePort(container, debugPort);
    }

    private static void enableDebuggerInJvmConfig(CreateContainerCmd createContainerCmd, String jvmConfigPath, int debugPort)
    {
        try {
            Bind[] binds = firstNonNull(createContainerCmd.getBinds(), new Bind[0]);
            boolean found = false;

            // Last bind wins, so we can find the last one only
            for (int bindIndex = binds.length - 1; bindIndex >= 0; bindIndex--) {
                Bind bind = binds[bindIndex];
                if (!bind.getVolume().getPath().equals(jvmConfigPath)) {
                    continue;
                }

                Path hostJvmConfig = Paths.get(bind.getPath());
                checkState(isRegularFile(hostJvmConfig), "Bind for %s is not a file", jvmConfigPath);

                Path temporaryDirectory = createTemporaryDirectoryForDocker();
                Path newJvmConfig = temporaryDirectory.resolve("jvm.config");

                List<String> jvmOptions = new ArrayList<>(readAllLines(hostJvmConfig, UTF_8));
                jvmOptions.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:" + debugPort);
                write(newJvmConfig, jvmOptions, UTF_8);

                binds[bindIndex] = new Bind(newJvmConfig.toString(), bind.getVolume(), AccessMode.ro, bind.getSecMode(), bind.getNoCopy(), bind.getPropagationMode());
                found = true;
                break;
            }

            checkState(found, "Could not find %s bind", jvmConfigPath);
            createContainerCmd.withBinds(binds);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
