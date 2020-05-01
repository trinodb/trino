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
import com.google.common.base.CaseFormat;
import com.google.common.reflect.ClassPath;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.tests.product.launcher.docker.ContainerUtil.killContainers;
import static io.prestosql.tests.product.launcher.docker.ContainerUtil.removeNetworks;
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
            killContainers(
                    dockerClient,
                    listContainersCmd -> listContainersCmd.withLabelFilter(ImmutableMap.of(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)));
            removeNetworks(
                    dockerClient,
                    listNetworksCmd -> listNetworksCmd.withNameFilter(PRODUCT_TEST_LAUNCHER_NETWORK));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<Class<? extends EnvironmentProvider>> findByBasePackage(String packageName)
    {
        try {
            return ClassPath.from(Environments.class.getClassLoader()).getTopLevelClassesRecursive(packageName).stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(clazz -> clazz.isAnnotationPresent(TestsEnvironment.class))
                    .map(clazz -> (Class<? extends EnvironmentProvider>) clazz.asSubclass(EnvironmentProvider.class))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String nameForClass(Class<? extends EnvironmentProvider> clazz)
    {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, clazz.getSimpleName());
    }
}
