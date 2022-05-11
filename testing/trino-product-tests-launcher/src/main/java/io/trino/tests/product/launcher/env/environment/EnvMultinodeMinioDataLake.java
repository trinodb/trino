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

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_ICEBERG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Trino with S3-compatible Data Lake setup based on MinIO
 */
@TestsEnvironment
public class EnvMultinodeMinioDataLake
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    @Inject
    public EnvMultinodeMinioDataLake(StandardMultinode standardMultinode, Hadoop hadoop, Minio minio, DockerFiles dockerFiles)
    {
        super(standardMultinode, hadoop, minio);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, this::configureTrinoContainer);
        builder.configureContainer(WORKER, this::configureTrinoContainer);
    }

    private void configureTrinoContainer(DockerContainer container)
    {
        container.withCopyFileToContainer(
                forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-minio-data-lake/hive.properties")),
                CONTAINER_PRESTO_HIVE_PROPERTIES);
        container.withCopyFileToContainer(
                forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-minio-data-lake/delta.properties")),
                CONTAINER_PRESTO_ETC + "/catalog/delta.properties");
        container.withCopyFileToContainer(
                forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-minio-data-lake/iceberg.properties")),
                CONTAINER_PRESTO_ICEBERG_PROPERTIES);
        container.withCopyFileToContainer(
                forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-minio-data-lake/memory.properties")),
                CONTAINER_PRESTO_ETC + "/catalog/memory.properties");
    }
}
