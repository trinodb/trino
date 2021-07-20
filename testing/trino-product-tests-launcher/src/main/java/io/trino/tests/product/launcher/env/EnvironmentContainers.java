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

import static org.testcontainers.utility.MountableFile.forHostPath;

public final class EnvironmentContainers
{
    public static final String PRESTO = "presto";
    public static final String COORDINATOR = PRESTO + "-master";
    public static final String WORKER = PRESTO + "-worker";
    public static final String WORKER_NTH = WORKER + "-";
    public static final String HADOOP = "hadoop-master";
    public static final String TESTS = "tests";
    public static final String LDAP = "ldapserver";

    private EnvironmentContainers() {}

    public static String worker(int number)
    {
        return WORKER_NTH + number;
    }

    public static boolean isPrestoContainer(String name)
    {
        return name.startsWith(PRESTO);
    }

    /**
     * Use this method only when you place `tempto-configuration.yaml' in environment configuration directory.
     */
    public static void configureTempto(Environment.Builder builder, ResourceProvider configDir)
    {
        builder.configureContainer(TESTS, dockerContainer -> {
            String environmentName = configDir.getPath("..").toFile().getName();
            String temptoConfig = "/docker/presto-product-tests/conf/tempto/tempto-configuration-for-" + environmentName + ".yaml";
            dockerContainer
                    .withCopyFileToContainer(
                            forHostPath(configDir.getPath("tempto-configuration.yaml")),
                            temptoConfig)
                    .withEnv("TEMPTO_CONFIG_FILES", temptoConfigFiles ->
                            temptoConfigFiles
                                    .map(files -> files + "," + temptoConfig)
                                    .orElse(temptoConfig));
        });
    }
}
