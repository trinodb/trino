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
package io.trino.tests.product.launcher.testcontainers;

import io.trino.tests.product.launcher.env.DockerContainer;

import static java.util.Objects.requireNonNull;

public interface PortBinder
{
    default void exposePort(DockerContainer container, int containerPort)
    {
        exposePort(container, containerPort, containerPort);
    }

    void exposePort(DockerContainer container, int hostPort, int containerPort);

    class DefaultPortBinder
            implements PortBinder
    {
        @Override
        public void exposePort(DockerContainer container, int hostPort, int containerPort)
        {
            // By default expose container port on random host port
            container.addExposedPort(containerPort);
        }
    }

    class FixedPortBinder
            implements PortBinder
    {
        @Override
        public void exposePort(DockerContainer container, int hostPort, int containerPort)
        {
            container.addExposedPort(containerPort); // Still export port, at a random free number, as certain startup checks require this.
            container.withFixedExposedPort(hostPort, containerPort);
        }
    }

    // This method exposes port unconditionally on the host machine.
    // It should be used for exposing debugging ports only as product tests
    // containers are communicating on a Docker network between each other.
    static void unsafelyExposePort(DockerContainer container, int port)
    {
        container.addExposedPort(port);
        // This could lead to a conflict when port is already bound on the host machine
        // preventing product tests environment from starting properly.
        container.withFixedExposedPort(port, port);
    }

    class ShiftingPortBinder
            implements PortBinder
    {
        private final PortBinder delegate;
        private final int portBase;

        public ShiftingPortBinder(PortBinder delegate, int portBase)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.portBase = portBase;
        }

        @Override
        public void exposePort(DockerContainer container, int hostPort, int containerPort)
        {
            delegate.exposePort(container, portBase + hostPort, containerPort);
        }
    }
}
