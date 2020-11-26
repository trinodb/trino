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

import org.testcontainers.containers.InternetProtocol;

import static java.util.Objects.requireNonNull;

public class FixedPortsAdapter
{
    private PortController portController = (hostPort, containerPort, protocol) -> {};

    void attachPortController(PortController portController)
    {
        this.portController = requireNonNull(portController, "portController is null");
    }

    public void addFixedExposedPort(int hostPort, int containerPort)
    {
        this.addFixedExposedPort(hostPort, containerPort, InternetProtocol.TCP);
    }

    public void addFixedExposedPort(int hostPort, int containerPort, InternetProtocol protocol)
    {
        portController.addFixedExposedPort(hostPort, containerPort, protocol);
    }

    interface PortController
    {
        void addFixedExposedPort(int hostPort, int containerPort, InternetProtocol protocol);
    }
}
