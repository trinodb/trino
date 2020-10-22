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
import com.google.common.io.Closer;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.EnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Kerberos;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.prestosql.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static io.prestosql.tests.product.launcher.env.common.Hadoop.createHadoopContainer;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.testcontainers.containers.BindMode.READ_WRITE;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Two pseudo-distributed, kerberized Hadoop installations running on side-by-side,
 * each within single container, with single-node, kerberized Presto.
 */
@TestsEnvironment
public final class TwoKerberosHives
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final String hadoopBaseImage;
    private final String hadoopImagesVersion;

    private final Closer closer = Closer.create();

    @Inject
    public TwoKerberosHives(
            DockerFiles dockerFiles,
            Standard standard,
            Hadoop hadoop,
            Kerberos kerberos,
            EnvironmentConfig environmentConfig)
    {
        super(ImmutableList.of(standard, hadoop, kerberos));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        hadoopBaseImage = requireNonNull(environmentConfig, "environmentConfig is null").getHadoopBaseImage();
        hadoopImagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getHadoopImagesVersion();
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        closer.close();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String keytabsHostDirectory = createKeytabsHostDirectory().toString();

        builder.configureContainer(COORDINATOR, container -> {
            container
                    .withFileSystemBind(keytabsHostDirectory, "/etc/presto/conf", READ_WRITE)

                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/presto-krb5.conf")),
                            "/etc/krb5.conf")

                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/hive1.properties")),
                            CONTAINER_PRESTO_ETC + "/catalog/hive1.properties")

                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/hive2.properties")),
                            CONTAINER_PRESTO_ETC + "/catalog/hive2.properties")

                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/iceberg1.properties")),
                            CONTAINER_PRESTO_ETC + "/catalog/iceberg1.properties")

                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/iceberg2.properties")),
                            CONTAINER_PRESTO_ETC + "/catalog/iceberg2.properties");
        });

        builder.configureContainer(HADOOP, container -> {
            container.setDockerImageName(hadoopBaseImage + "-kerberized:" + hadoopImagesVersion);
            container.withFileSystemBind(keytabsHostDirectory, "/presto_keytabs", READ_WRITE);
            container.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/hadoop-master-copy-keytabs.sh")),
                    CONTAINER_HADOOP_INIT_D + "copy-kerberos.sh");
        });

        builder.addContainer(createHadoopMaster2(keytabsHostDirectory));
    }

    private Path createKeytabsHostDirectory()
    {
        try {
            // Cannot use Files.createTempDirectory() because on Mac by default it uses /var/folders/ which is not visible to Docker for Mac
            Path temporaryDirectory = Files.createDirectory(Paths.get("/tmp/keytabs-" + randomUUID().toString()));
            closer.register(() -> deleteRecursively(temporaryDirectory, ALLOW_INSECURE));
            return temporaryDirectory;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("resource")
    private DockerContainer createHadoopMaster2(String keytabsHostDirectory)
    {
        return createHadoopContainer(dockerFiles, hadoopBaseImage + "-kerberized-2:" + hadoopImagesVersion, HADOOP + "-2")
                .withFileSystemBind(keytabsHostDirectory, "/presto_keytabs", READ_WRITE)
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/hadoop-master-2-copy-keytabs.sh")),
                        CONTAINER_HADOOP_INIT_D + "copy-kerberos.sh");
    }
}
