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
import io.airlift.units.DataSize;
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
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_API_PORT;
import static java.lang.Math.pow;
import static java.lang.Math.toIntExact;
import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.fail;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestS3Retries
{
    private static final int TOXIPROXY_CONTROL_PORT = 8474;
    private static final int MINIO_PROXY_PORT = 1234;

    private static final int S3_SDK_MAX_ATTEMPTS = 3;
    private static final Duration S3_CLIENT_HTTP_CONNECTION_TTL = Duration.ofMillis(1);

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

        ToxiproxyContainer toxiproxy = closer.register(new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0")
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
                .httpClient(ApacheHttpClient.builder()
                        // react to timeouts faster so that the test completes faster
                        .socketTimeout(Duration.ofSeconds(1))
                        // Limit connection reuse. toxiproxy probability-based circuit breaking works on per-TCP connection basis
                        // so once we have a single healthy connection, all further requests could use it.
                        // By limiting connection TTL and sleeping between test iterations, we avoid that.
                        // Note: the value needs to be non-zero to be effective.
                        .connectionTimeToLive(S3_CLIENT_HTTP_CONNECTION_TTL)
                        .build())
                // explicitly configure the number of retries
                .overrideConfiguration(o -> o.retryStrategy(b -> b.maxAttempts(S3_SDK_MAX_ATTEMPTS)))
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

    @Test
    public void testCreateExclusiveSmall()
            throws Exception
    {
        testCreateExclusive(new S3FileSystemConfig(), 10);
    }

    @Test
    public void testCreateExclusiveLarge()
            throws Exception
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setStreamingPartSize(DataSize.of(5, MEGABYTE)); // minimum
        int partSize = partSize(config);
        testCreateExclusive(config, partSize + 1);
    }

    private void testCreateExclusive(S3FileSystemConfig config, int dataSize)
            throws Exception
    {
        float connectionFailProbability = 0.3f;
        toxiProxy.toxics()
                .timeout("broken_connection", ToxicDirection.DOWNSTREAM, 0)
                .setToxicity(connectionFailProbability);

        // The test is needs to run multiple attempts to observe potential failure more reliably
        // At the same time we want number of attempts to be limited to avoid test taking too long.
        int attempts = 5;
        // - `connectionFailProbability` is the probability of S3 PutObject request failing on client side
        //   at the first attempt, but succeeding on the server side. This is the scenario we want to reproduce.
        // - `1 - connectionFailProbability` is a probability of S3 PutObject succeeding in the first attempt,
        //   i.e. that the test attempt fails to reproduce the problematic scenario.
        // - `(1 - connectionFailProbability) ^ attempts` is probability that  all attempts fail to reproduce
        //   the problematic scenario, assuming the attempts are independent. They seem to be independent
        //   because connection reuse is effectively disabled by connection TTL setting and sleeps.
        // - Therefore, `1 - (1 - connectionFailProbability) ^ attempts` is probability that at least one attempt
        //   reproduces the problematic scenario, i.e. the probability that the test is useful.
        double failFirstRequestInAnyAttemptProbability = 1 - pow((1 - connectionFailProbability), attempts);
        // For documentation purposes only what the value is. We definitely want the probability to be reasonably high.
        assertThat(failFirstRequestInAnyAttemptProbability).isEqualTo(0.83, within(0.01));

        S3Context context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                S3Context.S3SseContext.of(
                        config.getSseType(),
                        config.getSseKmsKeyId(),
                        config.getSseCustomerKey()),
                Optional.empty(),
                config.getStorageClass(),
                config.getCannedAcl());

        // The toxicity is randomized, so iterate for more determinism
        for (int iteration = 0; iteration < attempts; iteration++) {
            sleep(S3_CLIENT_HTTP_CONNECTION_TTL);
            testCreateExclusiveAttempt(context, dataSize);
        }
    }

    private void testCreateExclusiveAttempt(S3Context context, int dataSize)
    {
        String objectKey = "test-exclusive-create-" + randomNameSuffix();
        String s3Uri = "s3://%s/%s".formatted(bucketName, objectKey);
        S3Location location = new S3Location(Location.of(s3Uri));
        S3OutputFile outputFile = new S3OutputFile(directExecutor(), s3client, context, location, Optional.empty());

        byte[] data = new byte[dataSize];
        ThreadLocalRandom.current().nextBytes(data);

        try {
            outputFile.createExclusive(data);
        }
        catch (FileAlreadyExistsException e) {
            // Should never happen
            fail("Unexpected FileAlreadyExistsException when writing to a random path", e);
        }
        catch (IOException possibleException) {
            // In case all AWS SDK retries are interrupted by toxiproxy timeouts, we get an exception
            assertThat(possibleException)
                    .hasMessageFindingMatch("Put failed .* but provenance could not be verified|Unable to execute HTTP request.* \\(SDK Attempt Count: 3\\)");
        }

        assertThat(minioClient.getObjectContents(bucketName, objectKey)).as("Object data read back from storage")
                .isEqualTo(data);

        minioClient.removeObject(bucketName, objectKey);
    }

    private static int partSize(S3FileSystemConfig config)
    {
        return toIntExact(config.getStreamingPartSize().toBytes());
    }
}
