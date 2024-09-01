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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
@Testcontainers
public class TestIcebergS3FileSystemCompatibility
        extends AbstractTestQueryFramework
{
    private static final String ICEBERG_S3A_HADOOP_FS = "iceberg_s3_hadoop_fs";
    private static final String ICEBERG_S3A_NATIVE_FS = "iceberg_s3_native_fs";
    private static final String ACCESS_KEY = "accesskey";
    private static final String SECRET_KEY = "secretkey";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String SCHEMA_NAME = "test_schema";

    @Container
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0"))
            .withServices(LocalStackContainer.Service.S3);

    private S3Client s3;

    @AfterAll
    public void tearDown()
    {
        if (s3 != null) {
            s3.close();
            s3 = null;
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        s3 = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(LOCALSTACK.getRegion())).build();
        s3.createBucket(builder -> builder.bucket(BUCKET_NAME));

        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(ICEBERG_S3A_HADOOP_FS)
                        .setSchema(SCHEMA_NAME)
                        .build())
                .build();

        Map<String, String> icebergS3HadoopFsProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                .put("hive.s3.aws-access-key", ACCESS_KEY)
                .put("hive.s3.aws-secret-key", SECRET_KEY)
                .put("hive.s3.endpoint", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString())
                .put("hive.s3.region", Region.of(LOCALSTACK.getRegion()).toString())
                .put("fs.hadoop.enabled", "true")
                .put("fs.native-s3.enabled", "false")
                .buildOrThrow();

        Map<String, String> icebergS3NativeFsProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                .put("s3.aws-access-key", ACCESS_KEY)
                .put("s3.aws-secret-key", SECRET_KEY)
                .put("s3.endpoint", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString())
                .put("s3.region", Region.of(LOCALSTACK.getRegion()).toString())
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .buildOrThrow();
        try {
            // Copied necessary code from IcebergQueryRunner to add multiple catalogs in queryRunner
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
            queryRunner.installPlugin(new TestingIcebergPlugin(dataDir));
            queryRunner.createCatalog(ICEBERG_S3A_HADOOP_FS, "iceberg", icebergS3HadoopFsProperties);
            queryRunner.createCatalog(ICEBERG_S3A_NATIVE_FS, "iceberg", icebergS3NativeFsProperties);
            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testS3FileSystemCompatibility()
    {
        assertUpdate("CREATE SCHEMA %s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME));
        for (String s3Scheme : List.of("s3", "s3a", "s3n")) {
            testS3FileSystemCompatibility(s3Scheme, true);
            testS3FileSystemCompatibility(s3Scheme, false);
        }
    }

    private void testS3FileSystemCompatibility(String s3Scheme, boolean isNonCanonicalPath)
    {
        String tableName = "sample_table";
        String dir1 = "dir1";
        String dir2 = "dir2";

        if (isNonCanonicalPath) {
            // Create table in HadoopFs
            assertUpdate("CREATE TABLE %s.%s.%s WITH (location = '%s://%s/%s/%s//%s') AS SELECT 'test value' col".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName, s3Scheme, BUCKET_NAME, dir1, dir2, tableName), 1);
            // Verify that table is created at correct location
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s//%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isFalse();
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s/%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isTrue();
        }
        else {
            // Create table in HadoopFs
            assertUpdate("CREATE TABLE %s.%s.%s WITH (location = '%s://%s/%s/%s/%s') AS SELECT 'test value' col".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName, s3Scheme, BUCKET_NAME, dir1, dir2, tableName), 1);
            // Verify that table is created at correct location
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s/%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isFalse();
        }
        // Query table in HadoopFs
        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), "VALUES 'test value'");

        // Query the same table in NativeFs
        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_NATIVE_FS, SCHEMA_NAME, tableName), "VALUES 'test value'");

        assertUpdate("DROP TABLE %s.%s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName));
    }
}
