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
package io.prestosql.tests.product.launcher.testcontainers;

import io.prestosql.tests.product.launcher.docker.ContainerUtil;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;

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
        if (bindPorts) {
            ContainerUtil.exposePort(container, port);
        }
        else {
            // Still export port, at a random free number, as certain startup checks require this.
            container.addExposedPort(port);
        }
    }
}
