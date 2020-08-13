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
import com.github.dockerjava.api.command.CreateNetworkCmd;
import com.google.common.collect.ImmutableMap;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

import static io.prestosql.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME;
import static io.prestosql.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE;

public class ReusableNetwork
        implements Network
{
    private final String name;
    private String id;

    public ReusableNetwork(String name)
    {
        this.name = name;
    }

    @Override
    public synchronized String getId()
    {
        if (id == null) {
            DockerClient dockerClient = DockerClientFactory.instance().client();
            id = dockerClient.listNetworksCmd().withNameFilter(name).exec().stream()
                    .findAny().map(com.github.dockerjava.api.model.Network::getId).orElse(null);
            if (id == null) {
                CreateNetworkCmd createNetworkCmd = dockerClient.createNetworkCmd();
                createNetworkCmd.withName(name);
                createNetworkCmd.withCheckDuplicate(true);
                createNetworkCmd.withLabels(ImmutableMap.<String, String>builder().putAll(DockerClientFactory.DEFAULT_LABELS)
                        .put(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)
                        .build());
                id = createNetworkCmd.exec().getId();
            }
        }

        return id;
    }

    @Override
    public void close()
    {
        // closed by Environments.pruneEnvironment
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        throw new UnsupportedOperationException();
    }
}
