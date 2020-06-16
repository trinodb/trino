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
package io.prestosql.tests.product.launcher.env.common;

import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;

import javax.inject.Inject;

import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class KerberosKms
        implements EnvironmentExtender
{
    private final DockerFiles dockerFiles;

    private final String hadoopImagesVersion;

    @Inject
    public KerberosKms(DockerFiles dockerFiles, EnvironmentOptions environmentOptions)
    {
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        requireNonNull(environmentOptions, "environmentOptions is null");
        hadoopImagesVersion = requireNonNull(environmentOptions.hadoopImagesVersion, "environmentOptions.hadoopImagesVersion is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        // TODO (https://github.com/prestosql/presto/issues/1652) create images with HDP and KMS
        String dockerImageName = "prestodev/cdh5.15-hive-kerberized-kms:" + hadoopImagesVersion;

        builder.configureContainer("hadoop-master", container -> {
            container.setDockerImageName(dockerImageName);
            container
                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("common/kerberos-kms/kms-core-site.xml")),
                            "/etc/hadoop-kms/conf/core-site.xml");
        });

        builder.configureContainer("presto-master", container -> container.setDockerImageName(dockerImageName));

        builder.configureContainer("tests", container -> {
            container.setDockerImageName(dockerImageName);
            container.withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-docker-kerberos-kms.yaml")), CONTAINER_TEMPTO_PROFILE_CONFIG);
        });
    }
}
