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
package io.trino.plugin.exchange.containers;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.Minio;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class MinioStorage
        implements AutoCloseable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";

    private final String bucketName;
    private final Network network;
    private final Minio minio;

    public MinioStorage(String bucketName)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.network = newNetwork();
        this.minio = Minio.builder()
                .withNetwork(network)
                .withEnvVars(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", SECRET_KEY)
                        .buildOrThrow())
                .build();
    }

    public void start()
    {
        minio.start();
        S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:" + minio.getMinioApiEndpoint().getPort()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(US_EAST_1)
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
    }

    public static Map<String, String> getExchangeManagerProperties(MinioStorage minioStorage)
    {
        return ImmutableMap.<String, String>builder()
                .put("exchange.base-directory", "s3n://" + minioStorage.getBucketName())
                // TODO: enable exchange encryption after https is supported for Trino MinIO
                .put("exchange.encryption-enabled", "false")
                // to trigger file split in some tests
                .put("exchange.sink-max-file-size", "16MB")
                .put("exchange.s3.aws-access-key", MinioStorage.ACCESS_KEY)
                .put("exchange.s3.aws-secret-key", MinioStorage.SECRET_KEY)
                .put("exchange.s3.region", "us-east-1")
                .put("exchange.s3.endpoint", "http://" + minioStorage.getMinio().getMinioApiEndpoint())
                .buildOrThrow();
    }
}
