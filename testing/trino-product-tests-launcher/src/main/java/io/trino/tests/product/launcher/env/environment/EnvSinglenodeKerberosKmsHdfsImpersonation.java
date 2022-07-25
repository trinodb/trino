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
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.HadoopKerberosKms;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvSinglenodeKerberosKmsHdfsImpersonation
        extends EnvironmentProvider
{
    private final ResourceProvider configDir;

    @Inject
    public EnvSinglenodeKerberosKmsHdfsImpersonation(DockerFiles dockerFiles, Standard standard, HadoopKerberosKms hadoopKerberosKms)
    {
        super(ImmutableList.of(standard, hadoopKerberosKms));
        configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/singlenode-kerberos-kms-hdfs-impersonation");
    }

    @Override
    @SuppressWarnings("resource")
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(HADOOP, container -> {
            container
                    .withCopyFileToContainer(forHostPath(configDir.getPath("kms-acls.xml")), "/etc/hadoop-kms/conf/kms-acls.xml")
                    .withCopyFileToContainer(forHostPath(configDir.getPath("hiveserver2-site.xml")), "/etc/hive/conf/hiveserver2-site.xml");
        });
        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
        builder.addConnector("iceberg", forHostPath(configDir.getPath("iceberg.properties")));
    }
}
