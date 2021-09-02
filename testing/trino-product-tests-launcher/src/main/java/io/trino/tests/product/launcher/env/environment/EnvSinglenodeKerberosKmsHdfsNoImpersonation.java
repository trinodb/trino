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
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.HadoopKerberosKms;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_ICEBERG_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvSinglenodeKerberosKmsHdfsNoImpersonation
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    @Inject
    public EnvSinglenodeKerberosKmsHdfsNoImpersonation(DockerFiles dockerFiles, Standard standard, HadoopKerberosKms hadoopKerberosKms)
    {
        super(ImmutableList.of(standard, hadoopKerberosKms));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    @SuppressWarnings("resource")
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> {
            container
                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-kerberos-kms-hdfs-no-impersonation/hive.properties")),
                            CONTAINER_PRESTO_HIVE_PROPERTIES)
                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-kerberos-kms-hdfs-no-impersonation/iceberg.properties")),
                            CONTAINER_PRESTO_ICEBERG_PROPERTIES);
        });
    }
}
