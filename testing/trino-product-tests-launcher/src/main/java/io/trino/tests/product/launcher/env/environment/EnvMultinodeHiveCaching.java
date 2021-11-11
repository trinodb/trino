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
import com.google.common.collect.ImmutableMap;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Debug;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.ServerPackage;
import io.trino.tests.product.launcher.env.SupportedTrinoJdk;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import java.io.File;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.worker;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_JVM_CONFIG;
import static io.trino.tests.product.launcher.env.common.Standard.createPrestoContainer;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeHiveCaching
        extends EnvironmentProvider
{
    public static final String CONTAINER_PRESTO_HIVE_NON_CACHED_PROPERTIES = CONTAINER_PRESTO_ETC + "/catalog/hivenoncached.properties";

    private final DockerFiles dockerFiles;
    private final DockerFiles.ResourceProvider configDir;

    private final String imagesVersion;
    private final SupportedTrinoJdk jdkVersion;
    private final File serverPackage;
    private final boolean debug;

    @Inject
    public EnvMultinodeHiveCaching(
            DockerFiles dockerFiles,
            Standard standard,
            Hadoop hadoop,
            EnvironmentConfig environmentConfig,
            @ServerPackage File serverPackage,
            SupportedTrinoJdk jdkVersion,
            @Debug boolean debug)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment");
        this.imagesVersion = requireNonNull(environmentConfig, "environmentConfig is null").getImagesVersion();
        this.jdkVersion = requireNonNull(jdkVersion, "jdkVersion is null");
        this.serverPackage = requireNonNull(serverPackage, "serverPackage is null");
        this.debug = debug;
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode/multinode-master-jvm.config")), CONTAINER_PRESTO_JVM_CONFIG)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/standard-multinode/multinode-master-config.properties")), CONTAINER_PRESTO_CONFIG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_PRESTO_HIVE_NON_CACHED_PROPERTIES)
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode-cached/hive-coordinator.properties")), CONTAINER_PRESTO_HIVE_PROPERTIES)
                .withTmpFs(ImmutableMap.of("/tmp/cache", "rw")));

        createPrestoWorker(builder, 0);
        createPrestoWorker(builder, 1);
    }

    @SuppressWarnings("resource")
    private void createPrestoWorker(Environment.Builder builder, int workerNumber)
    {
        builder.addContainer(createPrestoContainer(dockerFiles, serverPackage, jdkVersion, debug, "ghcr.io/trinodb/testing/centos7-oj11:" + imagesVersion, worker(workerNumber))
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode/multinode-worker-jvm.config")), CONTAINER_PRESTO_JVM_CONFIG)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/standard-multinode/multinode-worker-config.properties")), CONTAINER_PRESTO_CONFIG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_PRESTO_HIVE_NON_CACHED_PROPERTIES)
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode-cached/hive-worker.properties")), CONTAINER_PRESTO_HIVE_PROPERTIES)
                .withTmpFs(ImmutableMap.of("/tmp/cache", "rw")));
    }
}
