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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.filesystem.s3.S3FileSystemUtils.createS3PreSigner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestS3PresignedUriGenerator
{
    @Test
    void testSignsWithValidatedSnapshot()
            throws Exception
    {
        AtomicInteger resolutions = new AtomicInteger();
        AwsCredentialsProvider provider = () -> {
            int count = resolutions.incrementAndGet();
            return AwsSessionCredentials.builder()
                    .accessKeyId("access" + count)
                    .secretAccessKey("secret")
                    .sessionToken("token" + count)
                    .expirationTime(Instant.now().plusSeconds(3600))
                    .build();
        };
        try (var generator = new S3PresignedUriGenerator(S3Presigner.builder().region(Region.US_EAST_1).build(), provider)) {
            var signed = generator.presignGetObject(request(Duration.ofMinutes(30)));
            assertThat(signed.url().getQuery()).contains("X-Amz-Credential=access1%2F", "X-Amz-Security-Token=token1");
            assertThat(generator.presignGetObject(request(Duration.ofMinutes(30))).url().getQuery()).contains("X-Amz-Credential=access1%2F");
            assertThat(resolutions.get()).isEqualTo(1);
        }
    }

    @Test
    void testRequestOverrideDoesNotUseDefaultIdentity()
            throws Exception
    {
        AwsCredentialsProvider defaults = () -> {
            throw new AssertionError("Default credentials must not be resolved");
        };
        try (var generator = new S3PresignedUriGenerator(S3Presigner.builder().region(Region.US_EAST_1).build(), defaults)) {
            var request = request(Duration.ofMinutes(30));
            var overridden = request.toBuilder().getObjectRequest(request.getObjectRequest().toBuilder()
                    .overrideConfiguration(builder -> builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("override", "secret"))))
                    .build()).build();
            assertThat(generator.presignGetObject(overridden).url().getQuery()).contains("X-Amz-Credential=override%2F");

            var unknownExpiry = overridden.toBuilder().getObjectRequest(overridden.getObjectRequest().toBuilder()
                    .overrideConfiguration(builder -> builder.credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create("override", "secret", "token"))))
                    .build()).build();
            assertThatThrownBy(() -> generator.presignGetObject(unknownExpiry)).hasMessageContaining("no expiration time");
        }
    }

    @Test
    void testUsesClientCredentialsRegionAndEndpoint()
            throws Exception
    {
        try (var client = S3Client.builder()
                .region(Region.EU_WEST_1)
                .endpointOverride(URI.create("https://mapped-storage.example"))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("mapped", "secret")))
                .build();
                var generator = createS3PreSigner(new S3FileSystemConfig().setPathStyleAccess(true), client)) {
            var signed = generator.presignGetObject(request(Duration.ofMinutes(30)));
            assertThat(signed.url().getHost()).isEqualTo("mapped-storage.example");
            assertThat(signed.url().getPath()).isEqualTo("/bucket/key");
            assertThat(signed.url().getQuery()).contains("X-Amz-Credential=mapped%2F", "%2Feu-west-1%2Fs3%2F");
        }
    }

    @Test
    void testInvalidTtlDoesNotResolveCredentials()
    {
        try (var generator = new S3PresignedUriGenerator(S3Presigner.builder().region(Region.US_EAST_1).build(), () -> {
            throw new AssertionError("Credentials must not be resolved");
        })) {
            assertThatThrownBy(() -> generator.presignGetObject(request(Duration.ZERO))).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> generator.presignGetObject(request(Duration.ofDays(8)))).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static GetObjectPresignRequest request(Duration ttl)
    {
        return GetObjectPresignRequest.builder()
                .signatureDuration(ttl)
                .getObjectRequest(GetObjectRequest.builder().bucket("bucket").key("key").build())
                .build();
    }
}
