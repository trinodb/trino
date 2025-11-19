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
package io.trino.filesystem.s3;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.trino.filesystem.Location;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.net.URI;
import java.util.Arrays;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_API_PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestS3Retries
{
    private static final int TOXIPROXY_CONTROL_PORT = 8474;
    private static final int MINIO_PROXY_PORT = 1234;

    private AutoCloseableCloser closer = AutoCloseableCloser.create();
    private String bucketName;
    private MinioClient minioClient;
    private Proxy toxiProxy;
    private S3Client s3client;

    @BeforeEach
    final void init()
            throws Exception
    {
        closer.close();
        closer = AutoCloseableCloser.create();

        Network network = closer.register(Network.newNetwork());
        Minio minio = closer.register(Minio.builder()
                .withNetwork(network)
                .build());
        minio.start();
        bucketName = "bucket-" + randomNameSuffix();
        minio.createBucket(bucketName);
        minioClient = closer.register(minio.createMinioClient());

        ToxiproxyContainer toxiproxy = closer.register(new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withExposedPorts(TOXIPROXY_CONTROL_PORT, MINIO_PROXY_PORT)
                .withNetwork(network));
        toxiproxy.start();

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        toxiProxy = toxiproxyClient.createProxy("minio", "0.0.0.0:" + MINIO_PROXY_PORT, "minio:" + MINIO_API_PORT);

        s3client = closer.register(S3Client.builder()
                .endpointOverride(URI.create("http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(MINIO_PROXY_PORT)))
                .region(Region.of(Minio.MINIO_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(Minio.MINIO_ACCESS_KEY, Minio.MINIO_SECRET_KEY)))
                // explicitly configure the number of retries
                .overrideConfiguration(o -> o.retryStrategy(b -> b.maxAttempts(3)))
                .build());
    }

    @AfterEach
    final void cleanup()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testRead()
            throws Exception
    {
        int testDataSize = 1024;
        byte[] data = new byte[testDataSize];
        Arrays.fill(data, (byte) 1);
        minioClient.putObject(bucketName, data, "object");

        // the number of transferred bytes includes both the response headers (around 570 bytes) and body
        toxiProxy.toxics()
                .limitData("broken_connection", ToxicDirection.DOWNSTREAM, 700);

        S3Location location = new S3Location(Location.of("s3://%s/object".formatted(bucketName)));
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .build();
        S3Input input = new S3Input(location.location(), s3client, request);

        byte[] bytes = new byte[testDataSize];
        assertThatThrownBy(() -> input.readFully(0, bytes, 0, testDataSize)).cause()
                .hasSuppressedException(SdkClientException.create("Request attempt 2 failure: Error reading getObject response"));
        assertThatThrownBy(() -> input.readTail(bytes, 0, testDataSize)).cause()
                .hasSuppressedException(SdkClientException.create("Request attempt 2 failure: Error reading getObject response"));
    }
}
