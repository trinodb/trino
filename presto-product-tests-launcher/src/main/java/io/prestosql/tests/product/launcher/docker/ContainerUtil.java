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
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.ListNetworksCmd;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Network;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.testcontainers.SelectedPortWaitStrategy;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.util.List;
import java.util.function.Function;

public final class ContainerUtil
{
    private static final Logger log = Logger.get(ContainerUtil.class);

    private ContainerUtil() {}

    public static void killContainers(DockerClient dockerClient, Function<ListContainersCmd, ListContainersCmd> filter)
    {
        while (true) {
            ListContainersCmd listContainersCmd = filter.apply(dockerClient.listContainersCmd()
                    .withShowAll(true));

            List<Container> containers = listContainersCmd.exec();
            if (containers.isEmpty()) {
                log.info("There are no running containers to kill");
                break;
            }
            for (Container container : containers) {
                try {
                    log.info("Removing container %s", container.getId());
                    dockerClient.removeContainerCmd(container.getId())
                            .withForce(true)
                            .exec();
                }
                catch (ConflictException | NotFoundException e) {
                    log.warn("Could not force remove container: %s", e);
                }
            }
        }
    }

    public static void removeNetworks(DockerClient dockerClient, Function<ListNetworksCmd, ListNetworksCmd> filter)
    {
        ListNetworksCmd listNetworksCmd = filter.apply(dockerClient.listNetworksCmd());
        List<Network> networks = listNetworksCmd.exec();
        for (Network network : networks) {
            try {
                dockerClient.removeNetworkCmd(network.getId())
                        .exec();
            }
            catch (NotFoundException e) {
                // Possible when previous tests invocation leaves a network behind and it is being garbage collected by Ryuk in the background.
                log.warn("Could not remove network", e);
            }
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

    public static WaitStrategy forSelectedPorts(int... ports)
    {
        return new SelectedPortWaitStrategy(ports);
    }
}
