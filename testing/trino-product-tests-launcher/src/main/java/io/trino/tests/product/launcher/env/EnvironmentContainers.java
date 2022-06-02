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

import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;

import java.nio.file.Path;
import java.security.SecureRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Integer.parseInt;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static org.testcontainers.utility.MountableFile.forHostPath;

public final class EnvironmentContainers
{
    private static final SecureRandom random = new SecureRandom();
    private static final int RANDOM_SUFFIX_LENGTH = 5;

    private static final String TRINO = "presto";
    public static final String COORDINATOR = TRINO + "-master";
    public static final String WORKER_NTH = TRINO + "-worker-";
    public static final String HADOOP = "hadoop-master";
    public static final String TESTS = "tests";
    public static final String LDAP = "ldapserver";

    public static final String CONTAINER_HEALTH_D = "/etc/health.d/";
    public static final String CONTAINER_CONF_ROOT = "/docker/presto-product-tests/";
    public static final String CONTAINER_TRINO_ETC = CONTAINER_CONF_ROOT + "conf/presto/etc";
    public static final String CONTAINER_TRINO_JVM_CONFIG = CONTAINER_TRINO_ETC + "/jvm.config";
    public static final String CONTAINER_TRINO_ACCESS_CONTROL_PROPERTIES = CONTAINER_TRINO_ETC + "/access-control.properties";
    public static final String CONTAINER_TRINO_CONFIG_PROPERTIES = CONTAINER_TRINO_ETC + "/config.properties";
    public static final String CONTAINER_TEMPTO_PROFILE_CONFIG = CONTAINER_CONF_ROOT + "conf/tempto/tempto-configuration-profile-config-file.yaml";

    private EnvironmentContainers() {}

    public static String worker(int number)
    {
        return WORKER_NTH + number;
    }

    public static boolean isTrinoContainer(DockerContainer container)
    {
        return container.getLogicalName().startsWith(TRINO);
    }

    public static boolean isCoordinator(DockerContainer container)
    {
        return container.getLogicalName().equals(COORDINATOR);
    }

    public static boolean isWorker(DockerContainer container)
    {
        return container.getLogicalName().startsWith(WORKER_NTH);
    }

    public static int getWorkerNumber(DockerContainer container)
    {
        String logicalName = container.getLogicalName();
        checkState(logicalName.startsWith(WORKER_NTH), "Provided container '%s' is not a Trino worker", logicalName);
        return parseInt(logicalName.substring(WORKER_NTH.length()));
    }

    /**
     * Use this method only when you place `tempto-configuration.yaml' in environment configuration directory.
     */
    public static void configureTempto(Environment.Builder builder, ResourceProvider configDir)
    {
        builder.configureContainer(TESTS, dockerContainer -> {
            Path path = configDir.getPath("tempto-configuration.yaml");
            String suffix = getParentDirectoryName(path) + "-" + randomSuffix();
            String temptoConfig = "/docker/presto-product-tests/conf/tempto/tempto-configuration-for-" + suffix + ".yaml";
            dockerContainer
                    .withCopyFileToContainer(forHostPath(path), temptoConfig)
                    .withEnv("TEMPTO_CONFIG_FILES", temptoConfigFiles ->
                            temptoConfigFiles
                                    .map(files -> files + "," + temptoConfig)
                                    .orElse(temptoConfig));
        });
    }

    private static String getParentDirectoryName(Path path)
    {
        checkArgument(path.getNameCount() >= 2, "Cannot determine parent directory of: %s", path);
        return path.getName(path.getNameCount() - 2).toString();
    }

    private static String randomSuffix()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }
}
