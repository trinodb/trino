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
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.filesystem.s3.S3FileSystemUtils.PRE_SIGNER_CREDENTIALS_PREFETCH_TIME;
import static io.trino.filesystem.s3.S3FileSystemUtils.PRE_SIGNER_CREDENTIALS_STALE_TIME;
import static io.trino.filesystem.s3.S3FileSystemUtils.PRE_SIGNER_SESSION_DURATION;
import static io.trino.filesystem.s3.S3FileSystemUtils.createPreSignerAssumeRoleCredentialsProvider;
import static io.trino.filesystem.s3.S3FileSystemUtils.createPreSignerWebIdentityCredentialsProvider;
import static org.assertj.core.api.Assertions.assertThat;

final class TestS3PreSignerCredentials
{
    private static final String ROLE = "arn:aws:iam::123456789012:role/test";
    // Long enough for the cached credentials to go stale even if the test thread is briefly descheduled
    private static final Duration FIRST_CREDENTIALS_MARGIN = Duration.ofSeconds(2);
    private static final long AGEING_MILLIS = 2500;

    @Test
    void testRefreshTimes()
    {
        // credentials fetched with the full session duration left must not be stale yet
        assertThat(PRE_SIGNER_CREDENTIALS_PREFETCH_TIME).isLessThan(PRE_SIGNER_SESSION_DURATION);
        assertThat(PRE_SIGNER_CREDENTIALS_STALE_TIME).isLessThan(PRE_SIGNER_CREDENTIALS_PREFETCH_TIME);
    }

    @Test
    void testCredentialsAreRefreshedBeforeStaleTime()
            throws InterruptedException
    {
        // A pre-signed URI stops working when its signing credentials expire (S3 returns 400 ExpiredToken),
        // so the signing credentials must outlive every URI signed with them.
        try (TestingStsClient sts = new TestingStsClient(Instant.now().plus(PRE_SIGNER_CREDENTIALS_STALE_TIME).plus(FIRST_CREDENTIALS_MARGIN));
                StsAssumeRoleCredentialsProvider provider = createPreSignerAssumeRoleCredentialsProvider(sts, ROLE, "test", null)) {
            provider.resolveCredentials();
            // let the cached credentials age until less than the stale time is left
            Thread.sleep(AGEING_MILLIS);

            AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();

            assertThat(sts.requests()).hasSize(2);
            assertThat(credentials.expirationTime().orElseThrow()).isAfterOrEqualTo(Instant.now().plus(PRE_SIGNER_CREDENTIALS_STALE_TIME));
        }
    }

    @Test
    void testCredentialsAreReusedUntilStaleTime()
    {
        try (TestingStsClient sts = new TestingStsClient(Instant.now().plus(PRE_SIGNER_SESSION_DURATION));
                StsAssumeRoleCredentialsProvider provider = createPreSignerAssumeRoleCredentialsProvider(sts, ROLE, "test", null)) {
            provider.resolveCredentials();
            provider.resolveCredentials();

            assertThat(sts.requests()).hasSize(1);
        }
    }

    @Test
    void testAssumeRoleCredentialsProvider()
    {
        try (TestingStsClient sts = new TestingStsClient(Instant.now().plus(PRE_SIGNER_SESSION_DURATION));
                StsAssumeRoleCredentialsProvider provider = createPreSignerAssumeRoleCredentialsProvider(sts, ROLE, "test-session", "test-external-id")) {
            assertThat(provider.staleTime()).isEqualTo(PRE_SIGNER_CREDENTIALS_STALE_TIME);
            assertThat(provider.prefetchTime()).isEqualTo(PRE_SIGNER_CREDENTIALS_PREFETCH_TIME);

            provider.resolveCredentials();

            AssumeRoleRequest request = sts.requests().getFirst();
            assertThat(request.roleArn()).isEqualTo(ROLE);
            assertThat(request.roleSessionName()).isEqualTo("test-session");
            assertThat(request.externalId()).isEqualTo("test-external-id");
            assertThat(request.durationSeconds()).isEqualTo((int) PRE_SIGNER_SESSION_DURATION.toSeconds());
        }
    }

    @Test
    void testWebIdentityCredentialsProvider()
            throws ReflectiveOperationException
    {
        // The role and token file are set explicitly: when they are read from AWS_ROLE_ARN and
        // AWS_WEB_IDENTITY_TOKEN_FILE and those are missing, the provider drops its refresh times.
        // The token file is not read until credentials are resolved, so it does not need to exist.
        WebIdentityTokenFileCredentialsProvider.Builder builder = WebIdentityTokenFileCredentialsProvider.builder()
                .roleArn(ROLE)
                .webIdentityTokenFile(Path.of("/nonexistent/web-identity-token"));
        try (WebIdentityTokenFileCredentialsProvider provider = createPreSignerWebIdentityCredentialsProvider(builder)) {
            assertThat(webIdentityProviderField(provider, "staleTime")).isEqualTo(PRE_SIGNER_CREDENTIALS_STALE_TIME);
            assertThat(webIdentityProviderField(provider, "prefetchTime")).isEqualTo(PRE_SIGNER_CREDENTIALS_PREFETCH_TIME);
            assertThat(webIdentityProviderField(provider, "roleSessionDuration")).isEqualTo(PRE_SIGNER_SESSION_DURATION);
        }
    }

    /**
     * {@link WebIdentityTokenFileCredentialsProvider} builds its own STS client and does not expose its settings.
     */
    private static Object webIdentityProviderField(WebIdentityTokenFileCredentialsProvider provider, String fieldName)
            throws ReflectiveOperationException
    {
        Field field;
        try {
            field = WebIdentityTokenFileCredentialsProvider.class.getDeclaredField(fieldName);
        }
        catch (NoSuchFieldException e) {
            throw new AssertionError("AWS SDK no longer has WebIdentityTokenFileCredentialsProvider.%s, update this test".formatted(fieldName), e);
        }
        field.setAccessible(true);
        return field.get(provider);
    }

    /**
     * Returns credentials expiring at {@code firstExpiration} first, and credentials valid for the requested
     * session duration afterwards.
     */
    private static final class TestingStsClient
            implements StsClient
    {
        private final Instant firstExpiration;
        private final List<AssumeRoleRequest> requests = new CopyOnWriteArrayList<>();

        public TestingStsClient(Instant firstExpiration)
        {
            this.firstExpiration = firstExpiration;
        }

        public List<AssumeRoleRequest> requests()
        {
            return requests;
        }

        @Override
        public AssumeRoleResponse assumeRole(AssumeRoleRequest request)
        {
            requests.add(request);
            Instant expiration = firstExpiration;
            if (requests.size() > 1) {
                expiration = Instant.now().plusSeconds(request.durationSeconds());
            }
            return AssumeRoleResponse.builder()
                    .credentials(Credentials.builder()
                            .accessKeyId("ASIATEST" + requests.size())
                            .secretAccessKey("secret")
                            .sessionToken("token")
                            .expiration(expiration)
                            .build())
                    .build();
        }

        @Override
        public String serviceName()
        {
            return SERVICE_NAME;
        }

        @Override
        public void close() {}
    }
}
