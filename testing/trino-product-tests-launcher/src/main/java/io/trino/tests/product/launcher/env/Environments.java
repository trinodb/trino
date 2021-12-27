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
package io.trino.tests.product.launcher.env;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.testcontainers.DockerClientFactory;

import static io.trino.tests.product.launcher.docker.ContainerUtil.killContainers;
import static io.trino.tests.product.launcher.docker.ContainerUtil.removeNetworks;
import static io.trino.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_NETWORK;
import static io.trino.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME;
import static io.trino.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE;

public final class Environments
{
    private Environments() {}

    private static final Logger log = Logger.get(Environments.class);

    public static void pruneEnvironment()
    {
        pruneContainers();
        pruneNetworks();
    }

    public static void pruneContainers()
    {
        log.info("Shutting down previous containers");
        try {
            killContainers(
                    DockerClientFactory.lazyClient(),
                    listContainersCmd -> listContainersCmd.withLabelFilter(ImmutableMap.of(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)));
        }
        catch (RuntimeException e) {
            log.warn(e, "Could not prune containers correctly");
        }
    }

    public static void pruneNetworks()
    {
        log.info("Removing previous networks");
        try {
            removeNetworks(
                    DockerClientFactory.lazyClient(),
                    listNetworksCmd -> listNetworksCmd.withNameFilter(PRODUCT_TEST_LAUNCHER_NETWORK));
        }
        catch (RuntimeException e) {
            log.warn(e, "Could not prune networks correctly");
        }
    }
}
