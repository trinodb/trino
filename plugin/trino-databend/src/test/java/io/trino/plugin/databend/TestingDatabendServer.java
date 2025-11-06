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
package io.trino.plugin.databend;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.databend.DatabendContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.Closeable;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingDatabendServer
        implements Closeable
{
    private static final DockerImageName DATABEND_IMAGE = DockerImageName.parse("datafuselabs/databend:nightly");
    private static final String DEFAULT_MINIO_IMAGE = "quay.io/minio/minio:RELEASE.2024-10-13T13-34-11Z";
    private static final DockerImageName MINIO_IMAGE = DockerImageName.parse(
            System.getProperty("databend.minio.image", DEFAULT_MINIO_IMAGE))
            .asCompatibleSubstituteFor("minio/minio");
    private static final int MINIO_PORT = 9000;
    private static final String MINIO_ALIAS = "minio";
    private static final String DATABEND_ALIAS = "databend";
    private static final String MINIO_ACCESS_KEY = "minioadmin";
    private static final String MINIO_SECRET_KEY = "minioadmin";
    private static final String S3_REGION = "us-east-1";
    private static final int BUCKET_CREATION_ATTEMPTS = 60;
    private static final String S3_BUCKET = "databend";

    private final Network network;
    private final GenericContainer<?> minioContainer;
    private final DatabendContainer databendContainer;
    private final String jdbcUrl;

    public TestingDatabendServer()
    {
        network = Network.newNetwork();

        minioContainer = new GenericContainer<>(MINIO_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(MINIO_ALIAS)
                .withEnv("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
                .withEnv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
                .withEnv("MINIO_REGION_NAME", S3_REGION)
                .withCommand("server", "/data")
                .withExposedPorts(MINIO_PORT)
                .waitingFor(Wait.forHttp("/minio/health/ready")
                        .forPort(MINIO_PORT)
                        .withStartupTimeout(Duration.ofMinutes(2)));

        minioContainer.start();
        createBucket();

        databendContainer = new DatabendContainer(DATABEND_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(DATABEND_ALIAS)
                .withUsername(getUser())
                .withPassword(getPassword())
                .withEnv("STORAGE_TYPE", "s3")
                .withEnv("STORAGE_S3_ENDPOINT_URL", format("http://%s:%s", MINIO_ALIAS, MINIO_PORT))
                .withEnv("STORAGE_S3_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
                .withEnv("STORAGE_S3_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
                .withEnv("STORAGE_S3_BUCKET", S3_BUCKET)
                .withEnv("STORAGE_S3_REGION", S3_REGION)
                .withEnv("STORAGE_S3_ENABLE_VIRTUAL_HOST_STYLE", "false")
                .withEnv("STORAGE_S3_FORCE_PATH_STYLE", "true")
                .withEnv("QUERY_DEFAULT_STORAGE_FORMAT", "native");

        databendContainer.start();

        jdbcUrl = ensureReadyAndGetJdbcUrl();
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getUser()
    {
        return "root";
    }

    public String getPassword()
    {
        return "";
    }

    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, getUser(), getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    @Override
    public void close()
    {
        RuntimeException suppressed = null;
        try {
            databendContainer.close();
        }
        catch (RuntimeException e) {
            suppressed = e;
        }

        try {
            minioContainer.close();
        }
        catch (RuntimeException e) {
            if (suppressed != null) {
                suppressed.addSuppressed(e);
            }
            else {
                suppressed = e;
            }
        }

        try {
            network.close();
        }
        catch (Exception e) {
            if (suppressed != null) {
                suppressed.addSuppressed(e);
            }
            else {
                suppressed = new RuntimeException("Failed to close Databend test network", e);
            }
        }

        if (suppressed != null) {
            throw suppressed;
        }
    }

    private void createBucket()
    {
        URI endpoint = URI.create(format("http://%s:%s", minioContainer.getHost(), minioContainer.getMappedPort(MINIO_PORT)));
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(MINIO_ACCESS_KEY, MINIO_SECRET_KEY));
        S3Configuration s3Configuration = S3Configuration.builder()
                .chunkedEncodingEnabled(false)
                .pathStyleAccessEnabled(true)
                .build();

        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt < BUCKET_CREATION_ATTEMPTS; attempt++) {
            try (S3Client s3Client = S3Client.builder()
                    .endpointOverride(endpoint)
                    .credentialsProvider(credentialsProvider)
                    .region(Region.of(S3_REGION))
                    .serviceConfiguration(s3Configuration)
                    .build()) {
                if (!bucketExists(s3Client)) {
                    s3Client.createBucket(CreateBucketRequest.builder()
                            .bucket(S3_BUCKET)
                            .createBucketConfiguration(builder -> builder.locationConstraint(S3_REGION))
                            .build());
                }
                return;
            }
            catch (RuntimeException e) {
                lastFailure = e;
                try {
                    Thread.sleep(1_000);
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while creating MinIO bucket", interrupted);
                }
            }
        }

        throw new RuntimeException("Failed to create MinIO bucket for Databend", lastFailure);
    }

    private static boolean bucketExists(S3Client s3Client)
    {
        try {
            s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(S3_BUCKET)
                    .build());
            return true;
        }
        catch (NoSuchBucketException e) {
            return false;
        }
        catch (S3Exception e) {
            if (e.statusCode() == 404 || e.statusCode() == 503) {
                return false;
            }
            throw e;
        }
    }

    private String ensureReadyAndGetJdbcUrl()
    {
        String url = databendContainer.getJdbcUrl();
        requireNonNull(url, "Databend JDBC URL is null");

        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();

        while (true) {
            try (Connection connection = DriverManager.getConnection(url, getUser(), getPassword());
                    Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
                statement.execute("CREATE DATABASE IF NOT EXISTS default");
                return url;
            }
            catch (SQLException ex) {
                if (System.nanoTime() > deadline) {
                    throw new RuntimeException("Databend server did not become ready in time", ex);
                }
                try {
                    Thread.sleep(2_000);
                }
                catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for Databend to start", interruptedException);
                }
            }
        }
    }
}
