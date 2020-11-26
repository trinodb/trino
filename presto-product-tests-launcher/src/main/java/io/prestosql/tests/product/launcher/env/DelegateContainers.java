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

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.PostgreSQLContainer;

import static java.util.Objects.requireNonNull;

public class DelegateContainers
{
    private DelegateContainers()
    {
    }

    public static DelegateContainerFactory<FixedHostPortGenericContainer> fixedHostPort(String dockerImageName)
    {
        return (listener, portsAdapter) -> {
            FixedHostPortGenericContainer container = new FixedHostPortGenericContainer(dockerImageName)
            {
                @Override
                protected void containerIsStarting(InspectContainerResponse containerInfo)
                {
                    listener.containerIsStarting(containerInfo, info -> super.containerIsStarting(info));
                }

                @Override
                protected void containerIsStarted(InspectContainerResponse containerInfo)
                {
                    listener.containerIsStarted(containerInfo, info -> super.containerIsStarted(info));
                }

                @Override
                protected void containerIsStopping(InspectContainerResponse containerInfo)
                {
                    listener.containerIsStopping(containerInfo, info -> super.containerIsStopping(info));
                }

                @Override
                protected void containerIsStopped(InspectContainerResponse containerInfo)
                {
                    listener.containerIsStopped(containerInfo, info -> super.containerIsStopped(info));
                }
            };
            portsAdapter.attachPortController(container::withFixedExposedPort);
            return container;
        };
    }

    /**
     * This factory returns FixedPortPostgreSQLContainer (instead of PosgreSQLContainer) to not force @SuppressWarnings("unchecked") on its every usage.
     */
    public static DelegateContainerFactory<FixedPortPostgreSQLContainer> postgreSQLContainer(String dockerImageName)
    {
        return (listener, portsAdapter) -> {
            FixedPortPostgreSQLContainer container = new FixedPortPostgreSQLContainer(dockerImageName, listener);
            portsAdapter.attachPortController(container::withFixedExposedPort);
            return container;
        };
    }

    public static class FixedPortPostgreSQLContainer
            extends PostgreSQLContainer<FixedPortPostgreSQLContainer>
    {
        private final EnvironmentListenerAdapter listenerAdapter;

        public FixedPortPostgreSQLContainer(String dockerImageName, EnvironmentListenerAdapter listenerAdapter)
        {
            super(dockerImageName);
            this.listenerAdapter = requireNonNull(listenerAdapter, "listenerAdapter is null");
        }

        public void withFixedExposedPort(int hostPort, int containerPort, InternetProtocol protocol)
        {
            super.addFixedExposedPort(hostPort, containerPort, protocol);
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo)
        {
            listenerAdapter.containerIsStarting(containerInfo, info -> super.containerIsStarting(info));
        }

        @Override
        protected void containerIsStarted(InspectContainerResponse containerInfo)
        {
            listenerAdapter.containerIsStarted(containerInfo, info -> super.containerIsStarted(info));
        }

        @Override
        protected void containerIsStopping(InspectContainerResponse containerInfo)
        {
            listenerAdapter.containerIsStopping(containerInfo, info -> super.containerIsStopping(info));
        }

        @Override
        protected void containerIsStopped(InspectContainerResponse containerInfo)
        {
            listenerAdapter.containerIsStopped(containerInfo, info -> super.containerIsStopped(info));
        }
    }

    public interface DelegateContainerFactory<T extends GenericContainer>
    {
        T create(EnvironmentListenerAdapter listener, FixedPortsAdapter portsAdapter);
    }
}
