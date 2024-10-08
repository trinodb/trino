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
package io.trino.plugin.exchange.filesystem.containers;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.KeyManagementServer;
import io.trino.testing.containers.Minio;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class MinioStorage
        implements AutoCloseable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";
    public static final String KMS_KEY_ID = "kms_key_id";

    private final String bucketName;
    private final Network network;
    private final Minio minio;
    private final Optional<KeyManagementServer> kms;

    public MinioStorage(String bucketName)
    {
        this(bucketName, false);
    }

    public MinioStorage(String bucketName, boolean withKms)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.network = newNetwork();
        this.kms = withKms ? Optional.of(KeyManagementServer.builder().withNetwork(network).build()) : Optional.empty();
        this.minio = buildMinio(kms);
    }

    private Minio buildMinio(Optional<KeyManagementServer> kms)
    {
        Minio.Builder minioBuilder = Minio.builder()
                .withNetwork(network)
                .withEnvVars(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", SECRET_KEY)
                        .buildOrThrow());

        kms.ifPresent(aKms -> minioBuilder
                .withEnvVars(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", SECRET_KEY)
                        .put("MINIO_KMS_KES_ENDPOINT", aKms.getMinioKesEndpointURL())
                        .put("MINIO_KMS_KES_CERT_FILE", "/kms_client.crt")
                        .put("MINIO_KMS_KES_KEY_FILE", "/kms_client.key")
                        .put("MINIO_KMS_KES_KEY_NAME", KMS_KEY_ID)
                        .put("MINIO_KMS_KES_CAPATH", "/kms.crt")
                        .put("MINIO_KMS_KES_CA_PATH", "/kms.crt")
                        .buildOrThrow())
                .withFilesToMount(Map.of(
                        "/kms_client.key", getPathFromClassPathResource("minio/kms_client.key"),
                        "/kms_client.crt", getPathFromClassPathResource("minio/kms_client.crt"),
                        "/kms.crt", getPathFromClassPathResource("kms/kms.crt"))));

        return minioBuilder.build();
    }

    public void start()
    {
        kms.ifPresent(KeyManagementServer::start);
        minio.start();
        S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:" + minio.getMinioApiEndpoint().getPort()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(US_EAST_1)
                .forcePathStyle(true)
                .build();
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3Client.createBucket(createBucketRequest);
    }

    public Minio getMinio()
    {
        return minio;
    }

    public String getBucketName()
    {
        return bucketName;
    }

    @Override
    public void close()
            throws Exception
    {
        network.close();
        minio.close();
        kms.ifPresent(KeyManagementServer::close);
    }

    public static Map<String, String> getExchangeManagerProperties(MinioStorage minioStorage)
    {
        return ImmutableMap.<String, String>builder()
                .put("exchange.base-directories", "s3://" + minioStorage.getBucketName())
                // to trigger file split in some tests
                .put("exchange.sink-max-file-size", "16MB")
                .put("exchange.s3.aws-access-key", MinioStorage.ACCESS_KEY)
                .put("exchange.s3.aws-secret-key", MinioStorage.SECRET_KEY)
                .put("exchange.s3.region", "us-east-1")
                .put("exchange.s3.path-style-access", "true")
                .put("exchange.s3.endpoint", "http://" + minioStorage.getMinio().getMinioApiEndpoint())
                // create more granular source handles given the fault-tolerant execution target task input size is set to lower value for testing
                .put("exchange.source-handle-target-data-size", "1MB")
                .buildOrThrow();
    }

    public static Map<String, String> getExchangeManagerPropertiesWithKms(MinioStorage minioStorage)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(getExchangeManagerProperties(minioStorage))
                .put("exchange.s3.sse.type", "KMS")
                .put("exchange.s3.sse.kms-key-id", KMS_KEY_ID)
                .buildOrThrow();
    }

    public static Map<String, String> getExchangeManagerPropertiesWithSseS3(MinioStorage minioStorage)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(getExchangeManagerProperties(minioStorage))
                .put("exchange.s3.sse.type", "S3")
                .buildOrThrow();
    }
}
