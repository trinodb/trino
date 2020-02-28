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

import com.github.dockerjava.api.DockerClient;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import org.testcontainers.DockerClientFactory;

public final class TestcontainersUtil
{
    private TestcontainersUtil() {}

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
}
