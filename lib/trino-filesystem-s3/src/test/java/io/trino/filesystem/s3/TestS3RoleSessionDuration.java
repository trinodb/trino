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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.filesystem.s3.S3FileSystemUtils.createS3PreSigner;
import static java.net.InetAddress.ofLiteral;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

class TestS3RoleSessionDuration
{
    @Test
    void testRoleSessionDurationAppliedWithoutLifetimeValidation()
            throws Exception
    {
        assertRequestedDurations(new Duration(90, MINUTES), "DurationSeconds=5400");
    }

    @Test
    void testDefaultRoleSessionDurationRequested()
            throws Exception
    {
        assertRequestedDurations(null, "DurationSeconds=3600");
    }

    private static void assertRequestedDurations(Duration roleSessionDuration, String expectedDuration)
            throws Exception
    {
        List<String> requests = new CopyOnWriteArrayList<>();
        HttpServer sts = HttpServer.create(new InetSocketAddress(ofLiteral("127.0.0.1"), 0), 0);
        sts.createContext("/", exchange -> {
            try (exchange) {
                requests.add(new String(exchange.getRequestBody().readAllBytes(), UTF_8));
                byte[] response =
                        """
                        <AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                          <AssumeRoleResult>
                            <Credentials>
                              <AccessKeyId>role-access</AccessKeyId>
                              <SecretAccessKey>secret</SecretAccessKey>
                              <SessionToken>token</SessionToken>
                              <Expiration>%s</Expiration>
                            </Credentials>
                          </AssumeRoleResult>
                          <ResponseMetadata><RequestId>test</RequestId></ResponseMetadata>
                        </AssumeRoleResponse>
                        """.formatted(Instant.now().plusSeconds(5400)).getBytes(UTF_8);
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
                    .setStsEndpoint("http://127.0.0.1:" + sts.getAddress().getPort());
            if (roleSessionDuration != null) {
                config.setRoleSessionDuration(roleSessionDuration);
            }
            assertThat(config.isPresignedUriLifetimeValidationEnabled()).isFalse();

            S3FileSystemLoader loader = new S3FileSystemLoader(OpenTelemetry.noop(), config, new S3FileSystemStats());
            try (S3Client client = loader.createClient()) {
                // Session used by the S3 client
                client.serviceClientConfiguration().credentialsProvider().resolveIdentity().join();
                assertThat(requests).hasSize(1);
                assertDuration(requests.getFirst(), expectedDuration);

                // Session used by the presigner, which has its own credentials provider when validation is disabled
                try (var presigner = createS3PreSigner(new S3PresignerSettings(config), client)) {
                    presigner.presignGetObject(GetObjectPresignRequest.builder()
                            .signatureDuration(java.time.Duration.ofMinutes(30))
                            .getObjectRequest(GetObjectRequest.builder().bucket("bucket").key("key").build())
                            .build());
                }
                assertThat(requests).hasSize(2);
                assertDuration(requests.getLast(), expectedDuration);
            }
            finally {
                loader.destroy();
            }
        }
        finally {
            sts.stop(0);
        }
    }

    private static void assertDuration(String request, String expectedDuration)
    {
        assertThat(request).contains("Action=AssumeRole", expectedDuration);
    }
}
