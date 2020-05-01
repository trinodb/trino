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
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.common.AbstractEnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Kerberos;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.testcontainers.containers.BindMode.READ_ONLY;
import static org.testcontainers.containers.BindMode.READ_WRITE;

/**
 * Two pseudo-distributed, kerberized Hadoop installations running on side-by-side,
 * each within single container, with single-node, kerberized Presto.
 */
@TestsEnvironment
public final class TwoKerberosHives
        extends AbstractEnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final String hadoopBaseImage;
    private final String imagesVersion;

    private final Closer closer = Closer.create();

    @Inject
    public TwoKerberosHives(
            DockerFiles dockerFiles,
            Standard standard,
            Hadoop hadoop,
            Kerberos kerberos,
            EnvironmentOptions environmentOptions)
    {
        super(ImmutableList.of(standard, hadoop, kerberos));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        hadoopBaseImage = requireNonNull(environmentOptions.hadoopBaseImage, "environmentOptions.hadoopBaseImage is null");
        imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        closer.close();
    }

    @Override
    protected void extendEnvironment(Environment.Builder builder)
    {
        String keytabsHostDirectory = createKeytabsHostDirectory().toString();

        builder.configureContainer("presto-master", container -> {
            container
                    .withFileSystemBind(keytabsHostDirectory, "/etc/presto/conf", READ_WRITE)

                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/presto-krb5.conf"),
                            "/etc/krb5.conf",
                            READ_ONLY)

                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/hive1.properties"),
                            CONTAINER_PRESTO_ETC + "/catalog/hive1.properties",
                            READ_ONLY)

                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/hive2.properties"),
                            CONTAINER_PRESTO_ETC + "/catalog/hive2.properties",
                            READ_ONLY)

                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/iceberg1.properties"),
                            CONTAINER_PRESTO_ETC + "/catalog/iceberg1.properties",
                            READ_ONLY)

                    .withFileSystemBind(
                            dockerFiles.getDockerFilesHostPath("conf/environment/two-kerberos-hives/iceberg2.properties"),
                            CONTAINER_PRESTO_ETC + "/catalog/iceberg2.properties",
                            READ_ONLY);
        });

        builder.configureContainer("hadoop-master", container -> {
            container
                    .withFileSystemBind(keytabsHostDirectory, "/presto_keytabs", READ_WRITE)
                    .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withEntrypoint(ImmutableList.of(
                            "/docker/presto-product-tests/conf/environment/two-kerberos-hives/hadoop-master-entrypoint.sh")));
        });

        builder.addContainer("hadoop-master-2", createHadoopMaster2(keytabsHostDirectory));
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
        DockerContainer container = new DockerContainer(hadoopBaseImage + "-kerberized-2:" + imagesVersion)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath(), "/docker/presto-product-tests", READ_ONLY)
                .withFileSystemBind(keytabsHostDirectory, "/presto_keytabs", READ_WRITE)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withEntrypoint(ImmutableList.of(
                        "/docker/presto-product-tests/conf/environment/two-kerberos-hives/hadoop-master-2-entrypoint.sh")))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                // TODO .waitingFor(...)
                .withStartupTimeout(Duration.ofMinutes(5));

        return container;
    }
}
