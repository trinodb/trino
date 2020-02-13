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
import io.prestosql.tests.product.launcher.env.common.Kerberos;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.prestosql.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_HIVE_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

@TestsEnvironment
public final class SinglenodeKerberosKmsHdfsImpersonation
        extends AbstractEnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final String imagesVersion;

    @Inject
    public SinglenodeKerberosKmsHdfsImpersonation(DockerFiles dockerFiles, Standard standard, Hadoop hadoop, Kerberos kerberos, EnvironmentOptions environmentOptions)
    {
        super(ImmutableList.of(standard, hadoop, kerberos));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
    }

    @Override
    @SuppressWarnings("resource")
    protected void extendEnvironment(Environment.Builder builder)
    {
        // TODO (https://github.com/prestosql/presto/issues/1652) create images with HDP and KMS
        String dockerImageName = "prestodev/cdh5.15-hive-kerberized-kms:" + imagesVersion;

        builder.configureContainer("hadoop-master", container -> {
            container.setDockerImageName(dockerImageName);
            container
                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-kerberos-kms-hdfs-impersonation/kms-core-site.xml"),
                            "/etc/hadoop-kms/conf/core-site.xml",
                            READ_ONLY)
                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-kerberos-kms-hdfs-impersonation/kms-acls.xml"),
                            "/etc/hadoop-kms/conf/kms-acls.xml",
                            READ_ONLY)
                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-kerberos-kms-hdfs-impersonation/hiveserver2-site.xml"),
                            "/etc/hive/conf/hiveserver2-site.xml",
                            READ_ONLY);
        });

        builder.configureContainer("presto-master", container -> {
            container.setDockerImageName(dockerImageName);
            container
                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-kerberos-kms-hdfs-impersonation/hive.properties"),
                            CONTAINER_PRESTO_HIVE_PROPERTIES,
                            READ_ONLY);
        });

        builder.configureContainer("tests", container -> {
            container.setDockerImageName(dockerImageName);
            container.withFileSystemBind(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-docker-kerberos-kms.yaml"), CONTAINER_TEMPTO_PROFILE_CONFIG, READ_ONLY);
        });
    }
}
