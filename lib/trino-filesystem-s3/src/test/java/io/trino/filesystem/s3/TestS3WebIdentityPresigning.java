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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.net.InetAddress.ofLiteral;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.writeString;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated("Sets the AWS STS endpoint and region system properties")
class TestS3WebIdentityPresigning
{
    @Test
    void testRefreshReadsRotatedTokenAndPreservesSessionDuration(@TempDir Path directory)
            throws Exception
    {
        Path token = directory.resolve("token");
        writeString(token, "first-token");
        var requests = new CopyOnWriteArrayList<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress(ofLiteral("127.0.0.1"), 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                requests.add(new String(exchange.getRequestBody().readAllBytes(), UTF_8));
                int call = requests.size();
                byte[] response =
                        """
                        <AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                          <AssumeRoleWithWebIdentityResult>
                            <Credentials>
                              <AccessKeyId>access%s</AccessKeyId>
                              <SecretAccessKey>secret</SecretAccessKey>
                              <SessionToken>token%s</SessionToken>
                              <Expiration>%s</Expiration>
                            </Credentials>
                          </AssumeRoleWithWebIdentityResult>
                          <ResponseMetadata><RequestId>test</RequestId></ResponseMetadata>
                        </AssumeRoleWithWebIdentityResponse>
                        """.formatted(call, call, Instant.now().plusSeconds(call == 1 ? 600 : 7200)).getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/xml");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        String previousEndpoint = System.getProperty("aws.endpointUrlSts");
        String previousRegion = System.getProperty("aws.region");
        server.start();
        try {
            System.setProperty("aws.endpointUrlSts", "http://127.0.0.1:" + server.getAddress().getPort());
            System.setProperty("aws.region", "us-east-1");
            try (var provider = WebIdentityTokenFileCredentialsProvider.builder()
                    .roleArn("arn:aws:iam::123456789012:role/test")
                    .roleSessionName("test")
                    .webIdentityTokenFile(token)
                    .roleSessionDuration(Duration.ofHours(2))
                    .build()) {
                assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo("access1");
                writeString(token, "rotated-token");
                S3PresigningCredentials credentials = new S3PresigningCredentials(provider);
                var fresh = credentials.resolve(Duration.ofHours(1));
                assertThat(fresh.accessKeyId()).isEqualTo("access2");
                assertThat(credentials.resolve(Duration.ofHours(1))).isSameAs(fresh);
                assertThat(requests).hasSize(2);
                assertThat(requests.getFirst()).contains("WebIdentityToken=first-token", "DurationSeconds=7200");
                assertThat(requests.getLast()).contains("WebIdentityToken=rotated-token", "DurationSeconds=7200");
            }
        }
        finally {
            restoreProperty("aws.endpointUrlSts", previousEndpoint);
            restoreProperty("aws.region", previousRegion);
            server.stop(0);
        }
    }

    private static void restoreProperty(String name, String value)
    {
        if (value == null) {
            System.clearProperty(name);
        }
        else {
            System.setProperty(name, value);
        }
    }
}
