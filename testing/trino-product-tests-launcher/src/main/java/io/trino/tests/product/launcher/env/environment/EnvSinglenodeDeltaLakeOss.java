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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * SinglenodeDeltaLakeOss test environment consists of:
 * - Hive (used for metastore) (HDP 3.1)
 * - Spark with open source implementation of Delta lake
 * - MinIO S3-compatible storage to store table data
 */
@TestsEnvironment
public class EnvSinglenodeDeltaLakeOss
        extends EnvironmentProvider
{
    private static final int SPARK_THRIFT_PORT = 10213;

    private static final String SPARK_CONTAINER_NAME = "spark";

    private static final String DEFAULT_S3_BUCKET_NAME = "trino-ci-test";

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvSinglenodeDeltaLakeOss(
            Standard standard,
            Hadoop hadoop,
            DockerFiles dockerFiles,
            EnvironmentConfig config,
            PortBinder portBinder,
            Minio minio)
    {
        super(ImmutableList.of(standard, hadoop, minio));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = config.getHadoopImagesVersion();
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-delta-lake-oss");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String s3Bucket = getS3Bucket();

        // Using hdp3.1 so we are using Hive metastore with version close to versions of  hive-*.jars Spark uses
        builder.configureContainer(HADOOP, container -> {
            container.setDockerImageName("ghcr.io/trinodb/testing/hdp3.1-hive:" + hadoopImagesVersion);
        });

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
        builder.addConnector(
                "delta_lake",
                forHostPath(configDir.getPath("delta.properties")),
                CONTAINER_TRINO_ETC + "/catalog/delta.properties");

        builder.configureContainer(TESTS, dockerContainer -> {
            dockerContainer.withEnv("S3_BUCKET", s3Bucket)
                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-hive3.yaml")),
                            CONTAINER_TEMPTO_PROFILE_CONFIG);
        });

        builder.addContainer(createSparkContainer())
                // Ensure Hive metastore is up; Spark needs to access it during startup
                .containerDependsOn(SPARK_CONTAINER_NAME, HADOOP);

        // Initialize buckets in Minio
        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path minioBucketDirectory;
        try {
            minioBucketDirectory = Files.createTempDirectory("trino-ci-test", posixFilePermissions);
            minioBucketDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.configureContainer(MINIO_CONTAINER_NAME, container ->
                container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + s3Bucket));

        configureTempto(builder, configDir);
    }

    @SuppressWarnings("resource")
    private DockerContainer createSparkContainer()
    {
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3-delta:" + hadoopImagesVersion, SPARK_CONTAINER_NAME)
                .withCopyFileToContainer(forHostPath(configDir.getPath("spark-defaults.conf")), "/spark/conf/spark-defaults.conf")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));

        portBinder.exposePort(container, SPARK_THRIFT_PORT);

        return container;
    }

    private String getS3Bucket()
    {
        String s3Bucket = System.getenv("S3_BUCKET");
        if (s3Bucket == null) {
            s3Bucket = DEFAULT_S3_BUCKET_NAME;
        }
        return s3Bucket;
    }
}
