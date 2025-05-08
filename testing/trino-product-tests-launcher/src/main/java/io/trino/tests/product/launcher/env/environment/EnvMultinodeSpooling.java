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
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_RUN_VIEW_AS_INVOKER;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_TIMESTAMP_NANOS;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_WITH_EXTERNAL_WRITES_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_ICEBERG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_JVM_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeSpooling
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeSpooling(DockerFiles dockerFiles, StandardMultinode standardMultinode, Hadoop hadoop, SpoolingMinio spoolingMinio)
    {
        super(ImmutableList.of(standardMultinode, hadoop, spoolingMinio));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode-master-jvm.config")), CONTAINER_TRINO_JVM_CONFIG));

        builder.configureContainer(WORKER, container -> container
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode-worker-jvm.config")), CONTAINER_TRINO_JVM_CONFIG)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive_with_external_writes.properties")), CONTAINER_TRINO_HIVE_WITH_EXTERNAL_WRITES_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive_timestamp_nanos.properties")), CONTAINER_TRINO_HIVE_TIMESTAMP_NANOS)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive_with_run_view_as_invoker.properties")), CONTAINER_TRINO_HIVE_RUN_VIEW_AS_INVOKER)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/iceberg.properties")), CONTAINER_TRINO_ICEBERG_PROPERTIES));
    }
}
