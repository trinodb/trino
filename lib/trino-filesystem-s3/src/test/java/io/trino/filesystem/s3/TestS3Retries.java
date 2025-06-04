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

import com.google.common.io.Closer;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.trino.filesystem.Location;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import static io.trino.testing.containers.Minio.MINIO_API_PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestS3Retries
{
    private static final int TOXIPROXY_CONTROL_PORT = 8474;
    private static final int MINIO_PROXY_PORT = 1234;
    private static final int TEST_DATA_SIZE = 1024;

    private S3Client s3client;

    private final Closer closer = Closer.create();

    @BeforeAll
    final void init()
            throws IOException
    {
        Network network = Network.newNetwork();
        closer.register(network::close);
        Minio minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();
        minio.createBucket("bucket");
        minio.writeFile(getTestData(), "bucket", "object");
        closer.register(minio::close);

        ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withExposedPorts(TOXIPROXY_CONTROL_PORT, MINIO_PROXY_PORT)
                .withNetwork(network)
                .withNetworkAliases("minio");
        toxiproxy.start();
        closer.register(toxiproxy::close);

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        Proxy proxy = toxiproxyClient.createProxy("minio", "0.0.0.0:" + MINIO_PROXY_PORT, "minio:" + MINIO_API_PORT);
        // the number of transferred bytes includes both the response headers (around 570 bytes) and body
        proxy.toxics()
                .limitData("broken connection", ToxicDirection.DOWNSTREAM, 700);

        s3client = S3Client.builder()
                .endpointOverride(URI.create("http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(MINIO_PROXY_PORT)))
                .region(Region.of(Minio.MINIO_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(Minio.MINIO_ACCESS_KEY, Minio.MINIO_SECRET_KEY)))
                // explicitly configure the number of retries
                .overrideConfiguration(o -> o.retryStrategy(b -> b.maxAttempts(3)))
                .build();
        closer.register(s3client::close);
    }

    @AfterAll
    final void cleanup()
            throws IOException
    {
        closer.close();
    }

    private static byte[] getTestData()
    {
        byte[] data = new byte[TEST_DATA_SIZE];
        Arrays.fill(data, (byte) 1);
        return data;
    }

    @Test
    public void testRetries()
    {
        S3Location location = new S3Location(Location.of("s3://bucket/object"));
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .build();
        S3Input input = new S3Input(location.location(), s3client, request);

        byte[] bytes = new byte[TEST_DATA_SIZE];
        assertThatThrownBy(() -> input.readFully(0, bytes, 0, TEST_DATA_SIZE)).cause()
                .hasSuppressedException(SdkClientException.create("Request attempt 2 failure: Error reading getObject response"));
        assertThatThrownBy(() -> input.readTail(bytes, 0, TEST_DATA_SIZE)).cause()
                .hasSuppressedException(SdkClientException.create("Request attempt 2 failure: Error reading getObject response"));
    }
}
