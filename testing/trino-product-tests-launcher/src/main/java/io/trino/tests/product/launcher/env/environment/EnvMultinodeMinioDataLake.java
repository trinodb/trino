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

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Trino with S3-compatible Data Lake setup based on MinIO
 */
@TestsEnvironment
public class EnvMultinodeMinioDataLake
        extends EnvironmentProvider
{
    private static final String S3_BUCKET_NAME = "test-bucket";

    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeMinioDataLake(StandardMultinode standardMultinode, Hadoop hadoop, Minio minio, DockerFiles dockerFiles)
    {
        super(standardMultinode, hadoop, minio);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-minio-data-lake");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(TESTS, dockerContainer -> {
            dockerContainer.withEnv("S3_BUCKET", S3_BUCKET_NAME);
        });

        // initialize buckets in minio
        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path minioBucketDirectory;
        try {
            minioBucketDirectory = Files.createTempDirectory("test-bucket-contents", posixFilePermissions);
            minioBucketDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.configureContainer(MINIO_CONTAINER_NAME, container ->
                container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + S3_BUCKET_NAME));

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
        builder.addConnector(
                "delta_lake",
                forHostPath(configDir.getPath("delta.properties")),
                CONTAINER_TRINO_ETC + "/catalog/delta.properties");
        builder.addConnector("iceberg", forHostPath(configDir.getPath("iceberg.properties")));
        builder.addConnector("memory", forHostPath(configDir.getPath("memory.properties")));
    }
}
