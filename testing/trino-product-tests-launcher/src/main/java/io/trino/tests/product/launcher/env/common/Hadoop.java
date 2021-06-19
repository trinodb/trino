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

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_CONF_ROOT;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_HEALTH_D;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forHealthcheck;
import static org.testcontainers.utility.MountableFile.forHostPath;

public final class Hadoop
        implements EnvironmentExtender
{
    public static final String CONTAINER_HADOOP_INIT_D = "/etc/hadoop-init.d/";
    public static final String CONTAINER_PRESTO_HIVE_PROPERTIES = CONTAINER_PRESTO_ETC + "/catalog/hive.properties";
    public static final String CONTAINER_PRESTO_HIVE_WITH_EXTERNAL_WRITES_PROPERTIES = CONTAINER_PRESTO_ETC + "/catalog/hive_with_external_writes.properties";
    public static final String CONTAINER_PRESTO_HIVE_TIMESTAMP_NANOS = CONTAINER_PRESTO_ETC + "/catalog/hive_timestamp_nanos.properties";
    public static final String CONTAINER_PRESTO_ICEBERG_PROPERTIES = CONTAINER_PRESTO_ETC + "/catalog/iceberg.properties";

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    private final String hadoopBaseImage;
    private final String hadoopImagesVersion;

    @Inject
    public Hadoop(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            EnvironmentConfig environmentConfig)
    {
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        hadoopBaseImage = requireNonNull(environmentConfig, "environmentConfig is null").getHadoopBaseImage();
        hadoopImagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createHadoopContainer(dockerFiles, hadoopBaseImage + ":" + hadoopImagesVersion, HADOOP));

        builder.configureContainer(HADOOP, container -> {
            portBinder.exposePort(container, 1180);  // socks proxy
            portBinder.exposePort(container, 5006); // debug port, exposed for manual use
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
        });

        builder.configureContainer(
                COORDINATOR,
                container -> container
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_PRESTO_HIVE_PROPERTIES)
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive_with_external_writes.properties")), CONTAINER_PRESTO_HIVE_WITH_EXTERNAL_WRITES_PROPERTIES)
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive_timestamp_nanos.properties")), CONTAINER_PRESTO_HIVE_TIMESTAMP_NANOS)
                        .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/iceberg.properties")), CONTAINER_PRESTO_ICEBERG_PROPERTIES));
    }

    @SuppressWarnings("resource")
    public static DockerContainer createHadoopContainer(DockerFiles dockerFiles, String dockerImage, String logicalName)
    {
        return new DockerContainer(dockerImage, logicalName)
                // TODO HIVE_PROXY_PORT:1180
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath()), CONTAINER_CONF_ROOT)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("health-checks/hadoop-health-check.sh")), CONTAINER_HEALTH_D + "hadoop-health-check.sh")
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hadoop-run.sh")), "/usr/local/hadoop-run.sh")
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/apply-config-overrides.sh")), CONTAINER_HADOOP_INIT_D + "00-apply-config-overrides.sh")
                .withCommand("/usr/local/hadoop-run.sh")
                .withExposedLogPaths("/var/log/hadoop-yarn", "/var/log/hadoop-hdfs", "/var/log/hive", "/var/log/container-health.log")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingForAll(forSelectedPorts(10000), forHealthcheck()) // HiveServer2
                .withStartupTimeout(Duration.ofMinutes(5))
                .withHealthCheck(dockerFiles.getDockerFilesHostPath("health-checks/health.sh"));
    }
}
