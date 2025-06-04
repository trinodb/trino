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

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.testcontainers.utility.MountableFile.forHostPath;

public final class EnvironmentContainers
{
    public static final String TRINO = "presto";
    public static final String COORDINATOR = TRINO + "-master";
    public static final String WORKER = TRINO + "-worker";
    public static final String WORKER_NTH = WORKER + "-";
    public static final String HADOOP = "hadoop-master";
    public static final String TESTS = "tests";
    public static final String OPENTRACING_COLLECTOR = "opentracing-collector";
    public static final String LDAP = "ldapserver";

    private EnvironmentContainers() {}

    public static String worker(int number)
    {
        return WORKER_NTH + number;
    }

    public static boolean isTrinoContainer(String name)
    {
        return name.startsWith(TRINO);
    }

    public static boolean isTrinoWorker(String name)
    {
        return name.startsWith(WORKER);
    }

    /**
     * Use this method only when you place `tempto-configuration.yaml' in environment configuration directory.
     */
    public static void configureTempto(Environment.Builder builder, ResourceProvider configDir)
    {
        builder.configureContainer(TESTS, dockerContainer -> {
            Path path = configDir.getPath("tempto-configuration.yaml");
            String suffix = getParentDirectoryName(path) + "-" + randomNameSuffix();
            String temptoConfig = "/docker/trino-product-tests/conf/tempto/tempto-configuration-for-" + suffix + ".yaml";
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
}
