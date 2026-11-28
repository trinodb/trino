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

import com.sun.net.httpserver.HttpServer;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig.S3AuthType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.filesystem.s3.S3FileSystemUtils.createS3PreSigner;
import static java.net.InetAddress.ofLiteral;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Presigns through a client created by {@link S3FileSystemLoader}, so that the credentials provider
 * used for refreshing is the one the AWS SDK reports for the client, and not a provider built by the test.
 */
class TestS3PresigningThroughLoader
{
    @Test
    void testRefreshesRoleSessionThatDoesNotCoverUriLifetime()
            throws Exception
    {
        AtomicInteger calls = new AtomicInteger();
        HttpServer sts = HttpServer.create(new InetSocketAddress(ofLiteral("127.0.0.1"), 0), 0);
        sts.createContext("/", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                // The first session is too short for the URI, yet fresh enough for the SDK not to refresh it by itself
                boolean first = calls.incrementAndGet() == 1;
                byte[] response =
                        """
                        <AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                          <AssumeRoleResult>
                            <Credentials>
                              <AccessKeyId>%s</AccessKeyId>
                              <SecretAccessKey>secret</SecretAccessKey>
                              <SessionToken>token</SessionToken>
                              <Expiration>%s</Expiration>
                            </Credentials>
                          </AssumeRoleResult>
                          <ResponseMetadata><RequestId>test</RequestId></ResponseMetadata>
                        </AssumeRoleResponse>
                        """.formatted(first ? "shortsession" : "longsession", Instant.now().plusSeconds(first ? 600 : 10_800)).getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/xml");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        sts.start();
        try {
            S3FileSystemConfig config = new S3FileSystemConfig()
                    .setRegion("us-east-1")
                    .setAwsAccessKey("access")
                    .setAwsSecretKey("secret")
                    .setAuthType(S3AuthType.IAM_ROLE)
                    .setIamRole("arn:aws:iam::123456789012:role/test")
                    .setStsEndpoint("http://127.0.0.1:" + sts.getAddress().getPort())
                    .setRoleSessionDuration(new Duration(3, HOURS))
                    .setPresignedUriLifetimeValidationEnabled(true);

            S3FileSystemLoader loader = new S3FileSystemLoader(OpenTelemetry.noop(), config, new S3FileSystemStats(), Optional.empty());
            try (S3Client client = loader.createClient();
                    var presigner = createS3PreSigner(new S3PresignerSettings(config), client)) {
                var signed = presigner.presignGetObject(GetObjectPresignRequest.builder()
                        .signatureDuration(java.time.Duration.ofHours(1))
                        .getObjectRequest(GetObjectRequest.builder().bucket("bucket").key("key").build())
                        .build());
                assertThat(signed.url().getQuery()).contains("X-Amz-Credential=longsession%2F", "X-Amz-Expires=3600");
                assertThat(calls.get()).isGreaterThanOrEqualTo(2);
            }
            finally {
                loader.destroy();
            }
        }
        finally {
            sts.stop(0);
        }
    }
}
