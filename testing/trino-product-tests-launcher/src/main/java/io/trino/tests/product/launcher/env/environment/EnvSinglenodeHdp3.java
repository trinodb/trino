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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

// HDP 3.1 images (code) + HDP 3.1-like configuration.
// See https://github.com/trinodb/trino/issues/1841 for more information.
@TestsEnvironment
public class EnvSinglenodeHdp3
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final String hadoopImagesVersion;

    @Inject
    protected EnvSinglenodeHdp3(DockerFiles dockerFiles, Standard standard, Hadoop hadoop, EnvironmentConfig environmentConfig)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.hadoopImagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = "ghcr.io/trinodb/testing/hdp3.1-hive:" + hadoopImagesVersion;

        builder.configureContainer(HADOOP, dockerContainer -> {
            dockerContainer.setDockerImageName(dockerImageName);
            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-hdp3/apply-hdp3-config.sh")),
                    CONTAINER_HADOOP_INIT_D + "apply-hdp3-config.sh");
        });

        builder.configureContainer(TESTS, dockerContainer -> {
            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-hive3.yaml")),
                    CONTAINER_TEMPTO_PROFILE_CONFIG);
        });
    }
}
