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
package io.prestosql.tests.product.launcher.cli;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.testcontainers.DockerClientFactory;

import static org.testcontainers.containers.output.OutputFrame.OutputType.STDERR;
import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;
import static org.testcontainers.utility.LogUtils.followOutput;

public class DockerUtils
{
    private static final DockerClient dockerClient = DockerClientFactory.lazyClient();

    private DockerUtils() {}

    /**
     * Simulates {@code docker run --rm}.
     */
    static int dockerRun(CreateContainerCmd createContainerCmd)
    {
        String containerId = createContainerCmd.exec().getId();
        try {
            dockerClient.startContainerCmd(containerId).exec();
            followOutput(dockerClient, containerId, f -> System.out.print(f.getUtf8String()), STDOUT);
            followOutput(dockerClient, containerId, f -> System.err.print(f.getUtf8String()), STDERR);
            return dockerClient.waitContainerCmd(containerId).start().awaitStatusCode();
        }
        finally {
            dockerClient.removeContainerCmd(containerId).exec();
        }
    }
}
