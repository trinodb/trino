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

import com.github.dockerjava.api.DockerClient;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.docker.DockerUtil;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.prestosql.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_NETWORK;
import static io.prestosql.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME;
import static io.prestosql.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE;

public final class Environments
{
    private Environments() {}

    private static final Logger log = Logger.get(Environments.class);

    public static void pruneEnvironment()
    {
        log.info("Shutting down previous containers");
        try (DockerClient dockerClient = DockerClientFactory.lazyClient()) {
            DockerUtil.killContainers(
                    dockerClient,
                    listContainersCmd -> listContainersCmd.withLabelFilter(ImmutableMap.of(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)));
            DockerUtil.removeNetworks(
                    dockerClient,
                    listNetworksCmd -> listNetworksCmd.withNameFilter(PRODUCT_TEST_LAUNCHER_NETWORK));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
