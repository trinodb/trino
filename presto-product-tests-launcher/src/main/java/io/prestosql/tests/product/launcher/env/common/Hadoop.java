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
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.testcontainers.PortBinder;
import io.prestosql.tests.product.launcher.testcontainers.SelectedPortWaitStrategy;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.time.Duration;

import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public final class Hadoop
        implements EnvironmentExtender
{
    public static final String CONTAINER_PRESTO_HIVE_PROPERTIES = CONTAINER_PRESTO_ETC + "/catalog/hive.properties";
    public static final String CONTAINER_PRESTO_ICEBERG_PROPERTIES = CONTAINER_PRESTO_ETC + "/catalog/iceberg.properties";

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    private final String hadoopBaseImage;
    private final String hadoopImagesVersion;

    @Inject
    public Hadoop(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            EnvironmentOptions environmentOptions)
    {
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        requireNonNull(environmentOptions, "environmentOptions is null");
        hadoopBaseImage = requireNonNull(environmentOptions.hadoopBaseImage, "environmentOptions.hadoopBaseImage is null");
        hadoopImagesVersion = requireNonNull(environmentOptions.hadoopImagesVersion, "environmentOptions.hadoopImagesVersion is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer("hadoop-master", createHadoopMaster());

        builder.configureContainer("presto-master", container -> {
            container
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_PRESTO_HIVE_PROPERTIES)
                    .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/iceberg.properties")), CONTAINER_PRESTO_ICEBERG_PROPERTIES);

            if (System.getenv("HADOOP_PRESTO_INIT_SCRIPT") != null) {
                container
                        .withCopyFileToContainer(
                                forHostPath(dockerFiles.getDockerFilesHostPath(System.getenv("HADOOP_PRESTO_INIT_SCRIPT"))),
                                "/docker/presto-init.d/hadoop-presto-init.sh");
            }
        });
    }

    @SuppressWarnings("resource")
    private DockerContainer createHadoopMaster()
    {
        DockerContainer container = new DockerContainer(hadoopBaseImage + ":" + hadoopImagesVersion)
                // TODO HIVE_PROXY_PORT:1180
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath()), "/docker/presto-product-tests")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(10000)) // HiveServer2
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, 1180);  // socks proxy
        // TODO portBinder.exposePort(container, 5006); // debug port
        portBinder.exposePort(container, 8020);
        portBinder.exposePort(container, 8042);
        portBinder.exposePort(container, 8088);
        portBinder.exposePort(container, 9000);
        portBinder.exposePort(container, 9083); // Metastore Thrift
        portBinder.exposePort(container, 9864); // DataNode Web UI since Hadoop 3
        portBinder.exposePort(container, 9870); // NameNode Web UI since Hadoop 3
        portBinder.exposePort(container, 10000); // HiveServer2
        portBinder.exposePort(container, 19888);
        portBinder.exposePort(container, 50070); // NameNode Web UI prior to Hadoop 3
        portBinder.exposePort(container, 50075); // DataNode Web UI prior to Hadoop 3

        return container;
    }
}
