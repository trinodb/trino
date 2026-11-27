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
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
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
    void testRequestOverrideCredentialsAreNotCached()
            throws Exception
    {
        AtomicInteger clientResolutions = new AtomicInteger();
        AwsCredentialsProvider client = () -> session("client" + clientResolutions.incrementAndGet());
        AtomicInteger overrideResolutions = new AtomicInteger();
        AwsCredentialsProvider override = () -> session("override" + overrideResolutions.incrementAndGet());
        try (var generator = new S3PresignedUriGenerator(S3Presigner.builder().region(Region.US_EAST_1).build(), client)) {
            var request = request(Duration.ofMinutes(30));
            var overridden = request.toBuilder().getObjectRequest(request.getObjectRequest().toBuilder()
                    .overrideConfiguration(builder -> builder.credentialsProvider(override))
                    .build()).build();
            assertThat(generator.presignGetObject(overridden).url().getQuery()).contains("X-Amz-Credential=override1%2F");
            // Override providers are created per file system, so their credentials are resolved for every URI
            assertThat(generator.presignGetObject(overridden).url().getQuery()).contains("X-Amz-Credential=override2%2F");
            assertThat(overrideResolutions.get()).isEqualTo(2);

            // The client's own credentials are still reused while they cover the URI lifetime
            assertThat(generator.presignGetObject(request).url().getQuery()).contains("X-Amz-Credential=client1%2F");
            assertThat(generator.presignGetObject(request).url().getQuery()).contains("X-Amz-Credential=client1%2F");
            assertThat(clientResolutions.get()).isEqualTo(1);
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
                var generator = createS3PreSigner(new S3PresignerSettings(new S3FileSystemConfig().setPathStyleAccess(true).setPresignedUriLifetimeValidationEnabled(true)), client)) {
            var signed = generator.presignGetObject(request(Duration.ofMinutes(30)));
            assertThat(signed.url().getHost()).isEqualTo("mapped-storage.example");
            assertThat(signed.url().getPath()).isEqualTo("/bucket/key");
            assertThat(signed.url().getQuery()).contains("X-Amz-Credential=mapped%2F", "%2Feu-west-1%2Fs3%2F");
        }
    }

    @Test
    void testClientIdentityProviderIsAdapted()
            throws Exception
    {
        S3PresignerSettings settings = new S3PresignerSettings(new S3FileSystemConfig().setPresignedUriLifetimeValidationEnabled(true));
        try (var client = S3Client.builder()
                .region(Region.EU_WEST_1)
                .credentialsProvider(identityProvider(AwsCredentialsIdentity.create("identity", "secret")))
                .build();
                var generator = createS3PreSigner(settings, client)) {
            assertThat(generator.presignGetObject(request(Duration.ofMinutes(30))).url().getQuery()).contains("X-Amz-Credential=identity%2F");
        }

        // Creating the presigner must not fail, so that file system operations other than presigning keep working
        try (var client = S3Client.builder()
                .region(Region.EU_WEST_1)
                .credentialsProvider(identityProvider(AwsSessionCredentialsIdentity.create("session", "secret", "token")))
                .build();
                var generator = createS3PreSigner(settings, client)) {
            assertThatThrownBy(() -> generator.presignGetObject(request(Duration.ofMinutes(30)))).hasMessageContaining("no expiration time");
        }
    }

    private static IdentityProvider<AwsCredentialsIdentity> identityProvider(AwsCredentialsIdentity identity)
    {
        return new IdentityProvider<>()
        {
            @Override
            public Class<AwsCredentialsIdentity> identityType()
            {
                return AwsCredentialsIdentity.class;
            }

            @Override
            public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request)
            {
                return CompletableFuture.completedFuture(identity);
            }
        };
    }

    @Test
    void testDefaultPresignerIsNotValidating()
            throws Exception
    {
        // A one-hour URI can never be covered by this ten-minute session, yet the default presigner keeps signing as it did before validation existed
        AwsSessionCredentials shortSession = AwsSessionCredentials.builder()
                .accessKeyId("short")
                .secretAccessKey("secret")
                .sessionToken("token")
                .expirationTime(Instant.now().plusSeconds(600))
                .build();
        try (var client = S3Client.builder()
                .region(Region.EU_WEST_1)
                .credentialsProvider(StaticCredentialsProvider.create(shortSession))
                .build()) {
            S3PresignerSettings defaults = new S3PresignerSettings(new S3FileSystemConfig().setRegion("eu-west-1").setAwsAccessKey("configured").setAwsSecretKey("secret"));
            assertThat(defaults.lifetimeValidation()).isFalse();
            try (var presigner = createS3PreSigner(defaults, client)) {
                assertThat(presigner).isNotInstanceOf(S3PresignedUriGenerator.class);
                // The previous presigner builds its own credentials from the configuration, not from the client
                assertThat(presigner.presignGetObject(request(Duration.ofHours(1))).url().getQuery()).contains("X-Amz-Credential=configured%2F", "X-Amz-Expires=3600");
            }
            try (var presigner = createS3PreSigner(new S3PresignerSettings(new S3FileSystemConfig().setRegion("eu-west-1").setPresignedUriLifetimeValidationEnabled(true)), client)) {
                assertThat(presigner).isInstanceOf(S3PresignedUriGenerator.class);
                assertThatThrownBy(() -> presigner.presignGetObject(request(Duration.ofHours(1)))).hasMessageContaining("shorter URI TTL");
            }
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

    private static AwsSessionCredentials session(String accessKey)
    {
        return AwsSessionCredentials.builder()
                .accessKeyId(accessKey)
                .secretAccessKey("secret")
                .sessionToken("token")
                .expirationTime(Instant.now().plusSeconds(3600))
                .build();
    }
}
