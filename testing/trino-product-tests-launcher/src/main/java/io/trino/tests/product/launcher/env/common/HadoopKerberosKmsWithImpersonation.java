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
package io.trino.tests.product.launcher.env.common;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;

import java.util.List;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class HadoopKerberosKmsWithImpersonation
        implements EnvironmentExtender
{
    private final ResourceProvider configDir;
    private final HadoopKerberosKms hadoopKerberosKms;

    @Inject
    public HadoopKerberosKmsWithImpersonation(DockerFiles dockerFiles, HadoopKerberosKms hadoopKerberosKms)
    {
        configDir = dockerFiles.getDockerFilesHostDirectory("common/hadoop-kerberos-kms-with-impersonation");
        this.hadoopKerberosKms = requireNonNull(hadoopKerberosKms, "hadoopKerberosKms is null");
    }

    @Override
    @SuppressWarnings("resource")
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(
                HADOOP,
                container ->
                        container
                                .withCopyFileToContainer(forHostPath(configDir.getPath("kms-acls.xml")), "/etc/hadoop-kms/conf/kms-acls.xml")
                                .withCopyFileToContainer(forHostPath(configDir.getPath("hiveserver2-site.xml")), "/etc/hive/conf/hiveserver2-site.xml"));
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(hadoopKerberosKms);
    }
}
