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
import io.trino.tests.product.launcher.env.EnvironmentOptions;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PortBinder
{
    private final boolean bindPorts;

    @Inject
    public PortBinder(EnvironmentOptions environmentOptions)
    {
        this.bindPorts = requireNonNull(environmentOptions, "environmentOptions is null").bindPorts;
    }

    public void exposePort(DockerContainer container, int port)
    {
        container.addExposedPort(port); // Still export port, at a random free number, as certain startup checks require this.
        if (bindPorts) {
            container.withFixedExposedPort(port, port);
        }
    }

    // This method exposes port unconditionally on the host machine.
    // It should be used for exposing debugging ports only as product tests
    // containers are communicating on a Docker network between each other.
    public static void unsafelyExposePort(DockerContainer container, int port)
    {
        container.addExposedPort(port);
        // This could lead to a conflict when port is already bound on the host machine
        // preventing product tests environment from starting properly.
        container.withFixedExposedPort(port, port);
    }
}
