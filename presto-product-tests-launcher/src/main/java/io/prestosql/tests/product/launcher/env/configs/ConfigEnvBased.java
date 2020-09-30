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
package io.prestosql.tests.product.launcher.env.configs;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.Environment;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.PRESTO;
import static io.prestosql.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class ConfigEnvBased
        extends ConfigDefault
{
    private final DockerFiles dockerFiles;

    @Inject
    public ConfigEnvBased(DockerFiles dockerFiles)
    {
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public String getHadoopBaseImage()
    {
        return getEnvOrDefault("HADOOP_BASE_IMAGE", super.getHadoopBaseImage());
    }

    @Override
    public String getImagesVersion()
    {
        return getEnvOrDefault("DOCKER_IMAGES_VERSION", super.getImagesVersion());
    }

    @Override
    public String getHadoopImagesVersion()
    {
        return getEnvOrDefault("HADOOP_IMAGES_VERSION", super.getHadoopImagesVersion());
    }

    @Override
    public String getTemptoEnvironmentConfigFile()
    {
        return getEnvOrDefault("TEMPTO_ENVIRONMENT_CONFIG_FILE", super.getTemptoEnvironmentConfigFile());
    }

    @Override
    public List<String> getExcludedGroups()
    {
        return Optional
                .ofNullable(getenv("DISTRO_SKIP_GROUP"))
                .map(value -> Splitter.on(',')
                        .omitEmptyStrings()
                        .trimResults()
                        .splitToList(value))
                .orElse(super.getExcludedGroups());
    }

    @Override
    public List<String> getExcludedTests()
    {
        return Optional
                .ofNullable(getenv("DISTRO_SKIP_TEST"))
                .map(value -> Splitter.on(',')
                        .omitEmptyStrings()
                        .trimResults()
                        .splitToList(value))
                .orElse(super.getExcludedTests());
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainers(container -> {
            if (container.getLogicalName().startsWith(PRESTO)) {
                String prestoInitScript = getenv("HADOOP_PRESTO_INIT_SCRIPT");

                if (!Strings.isNullOrEmpty(prestoInitScript)) {
                    container.withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath(prestoInitScript)),
                            "/docker/presto-init.d/presto-init.sh");
                }
            }

            if (container.getLogicalName().startsWith(HADOOP)) {
                String hadoopInitScript = getenv("HADOOP_INIT_SCRIPT");

                if (!Strings.isNullOrEmpty(hadoopInitScript)) {
                    container.withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath(hadoopInitScript)),
                            CONTAINER_HADOOP_INIT_D + "/hadoop-presto-init.sh");
                }
            }
        });
    }

    private static String getEnvOrDefault(String envKey, String defaultValue)
    {
        return Optional
                .ofNullable(getenv(envKey))
                .orElse(defaultValue);
    }
}
