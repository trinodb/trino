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
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.common.AbstractEnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Kerberos;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.time.Duration;

import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

/**
 * Two pseudo-distributed Hadoop installations running on side-by-side,
 * each within single container (one kerberized and one not), with single-node,
 * kerberized Presto.
 */
@TestsEnvironment
public final class TwoMixedHives
        extends AbstractEnvironmentProvider
{
    private final DockerFiles dockerFiles;

    private final String hadoopBaseImage;
    private final String imagesVersion;

    @Inject
    public TwoMixedHives(
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

    @Override
    protected void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer("presto-master", container -> {
            container.withFileSystemBind(
                    dockerFiles.getDockerFilesHostPath("conf/environment/two-mixed-hives/hive1.properties"),
                    CONTAINER_PRESTO_ETC + "/catalog/hive1.properties",
                    READ_ONLY);
            container.withFileSystemBind(
                    dockerFiles.getDockerFilesHostPath("conf/environment/two-mixed-hives/hive2.properties"),
                    CONTAINER_PRESTO_ETC + "/catalog/hive2.properties",
                    READ_ONLY);
            container.withFileSystemBind(
                    dockerFiles.getDockerFilesHostPath("conf/environment/two-mixed-hives/iceberg1.properties"),
                    CONTAINER_PRESTO_ETC + "/catalog/iceberg1.properties",
                    READ_ONLY);
            container.withFileSystemBind(
                    dockerFiles.getDockerFilesHostPath("conf/environment/two-mixed-hives/iceberg2.properties"),
                    CONTAINER_PRESTO_ETC + "/catalog/iceberg2.properties",
                    READ_ONLY);
        });

        builder.addContainer("hadoop-master-2", createHadoopMaster2());
    }

    @SuppressWarnings("resource")
    private DockerContainer createHadoopMaster2()
    {
        DockerContainer container = new DockerContainer(hadoopBaseImage + ":" + imagesVersion)
                .withFileSystemBind(
                        dockerFiles.getDockerFilesHostPath("conf/environment/two-mixed-hives/hadoop-master-2/core-site.xml"),
                        "/etc/hadoop/conf/core-site.xml",
                        READ_ONLY)
                .withFileSystemBind(
                        dockerFiles.getDockerFilesHostPath("conf/environment/two-mixed-hives/hadoop-master-2/mapred-site.xml"),
                        "/etc/hadoop/conf/mapred-site.xml",
                        READ_ONLY)
                .withFileSystemBind(
                        dockerFiles.getDockerFilesHostPath("conf/environment/two-mixed-hives/hadoop-master-2/yarn-site.xml"),
                        "/etc/hadoop/conf/yarn-site.xml",
                        READ_ONLY)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                // TODO .waitingFor(...)
                .withStartupTimeout(Duration.ofMinutes(5));

        return container;
    }
}
