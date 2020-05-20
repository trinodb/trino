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
package io.prestosql.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.common.AbstractEnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

// HDP 3.1 images (code) + HDP 3.1-like configuration.
// See https://github.com/prestosql/presto/issues/1841 for more information.
@TestsEnvironment
public class SinglenodeHdp3
        extends AbstractEnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final String imagesVersion;

    @Inject
    protected SinglenodeHdp3(DockerFiles dockerFiles, Standard standard, Hadoop hadoop, EnvironmentOptions environmentOptions)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
    }

    @Override
    protected void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = "prestodev/hdp3.1-hive:" + imagesVersion;

        builder.configureContainer("hadoop-master", dockerContainer -> {
            dockerContainer.setDockerImageName(dockerImageName);
            dockerContainer.withCreateContainerCmdModifier(createContainerCmd ->
                    createContainerCmd.withEntrypoint(ImmutableList.of(("/docker/presto-product-tests/conf/environment/singlenode-hdp3/hadoop-entrypoint.sh"))));
        });

        builder.configureContainer("tests", dockerContainer -> {
            dockerContainer.withFileSystemBind(
                    dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-hive3.yaml"),
                    CONTAINER_TEMPTO_PROFILE_CONFIG,
                    READ_ONLY);
        });
    }
}
