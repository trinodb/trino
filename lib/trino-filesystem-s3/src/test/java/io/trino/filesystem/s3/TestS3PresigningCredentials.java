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

import io.airlift.testing.TestingClock;
import io.trino.filesystem.TrinoFileSystemException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestS3PresigningCredentials
{
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, UTC);

    @Test
    void testReuseUntilRequestedLifetimeExceedsRemainingValidity()
            throws Exception
    {
        AtomicInteger calls = new AtomicInteger();
        AwsSessionCredentials first = session("first", NOW.plusSeconds(3600));
        AwsSessionCredentials second = session("second", NOW.plusSeconds(7200));
        S3PresigningCredentials credentials = new S3PresigningCredentials(() -> calls.incrementAndGet() == 1 ? first : second, CLOCK);
        assertThat(credentials.resolve(Duration.ofMinutes(30))).isSameAs(first);
        assertThat(credentials.resolve(Duration.ofMinutes(59))).isSameAs(first);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(credentials.resolve(Duration.ofMinutes(59).plusNanos(1))).isSameAs(second);
        assertThat(credentials.resolve(Duration.ofMinutes(30))).isSameAs(second);
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void testRejectsShortSessionAfterOneRefreshAttempt()
    {
        AtomicInteger calls = new AtomicInteger();
        S3PresigningCredentials credentials = new S3PresigningCredentials(() -> {
            calls.incrementAndGet();
            return session("short", NOW.plusSeconds(3600));
        }, CLOCK);
        assertThatThrownBy(() -> credentials.resolve(Duration.ofHours(1)))
                .isInstanceOf(TrinoFileSystemException.class)
                .hasMessageContaining("shorter URI TTL");
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void testRejectsUnknownSessionExpiration()
    {
        S3PresigningCredentials credentials = new S3PresigningCredentials(
                StaticCredentialsProvider.create(AwsSessionCredentials.create("access", "secret", "token")), CLOCK);
        assertThatThrownBy(() -> credentials.resolve(Duration.ofMinutes(1)))
                .isInstanceOf(TrinoFileSystemException.class)
                .hasMessageContaining("no expiration time");
    }

    @Test
    void testBasicCredentialsDoNotRequireExpiration()
            throws Exception
    {
        AwsCredentials basic = AwsBasicCredentials.create("access", "secret");
        S3PresigningCredentials credentials = new S3PresigningCredentials(StaticCredentialsProvider.create(basic), CLOCK);
        AwsCredentials resolved = credentials.resolve(Duration.ofDays(7));
        assertThat(resolved.accessKeyId()).isEqualTo(basic.accessKeyId());
        assertThat(resolved.secretAccessKey()).isEqualTo(basic.secretAccessKey());
    }

    @Test
    void testResolutionTimeCountsAgainstRemainingValidity()
    {
        TestingClock clock = new TestingClock();
        Instant expiration = clock.instant().plus(Duration.ofMinutes(31));
        S3PresigningCredentials credentials = new S3PresigningCredentials(() -> {
            clock.increment(1, SECONDS);
            return session("slow", expiration);
        }, clock);
        assertThatThrownBy(() -> credentials.resolve(Duration.ofMinutes(30)))
                .isInstanceOf(TrinoFileSystemException.class)
                .hasMessageContaining("shorter URI TTL");
    }

    @Test
    void testRefreshFailureDoesNotReturnInsufficientCredentials()
            throws Exception
    {
        AtomicReference<AwsCredentials> current = new AtomicReference<>(session("first", NOW.plusSeconds(1800)));
        AtomicInteger calls = new AtomicInteger();
        AwsCredentialsProvider provider = () -> {
            if (calls.incrementAndGet() == 3) {
                throw SdkClientException.create("STS unavailable");
            }
            return current.get();
        };
        S3PresigningCredentials credentials = new S3PresigningCredentials(provider, CLOCK);
        assertThat(credentials.resolve(Duration.ofMinutes(10))).isSameAs(current.get());
        assertThatThrownBy(() -> credentials.resolve(Duration.ofMinutes(40)))
                .isInstanceOf(SdkClientException.class)
                .hasMessage("STS unavailable");
        current.set(session("replacement", NOW.plusSeconds(3600)));
        assertThat(credentials.resolve(Duration.ofMinutes(40))).isSameAs(current.get());
    }

    @Test
    void testConcurrentRefreshIsShared()
            throws Exception
    {
        AtomicInteger calls = new AtomicInteger();
        S3PresigningCredentials credentials = new S3PresigningCredentials(() -> {
            calls.incrementAndGet();
            return session("shared", NOW.plusSeconds(3600));
        }, CLOCK);
        CountDownLatch ready = new CountDownLatch(8);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = newFixedThreadPool(8)) {
            var futures = new ArrayList<Future<AwsCredentials>>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(10, SECONDS)).isTrue();
                    return credentials.resolve(Duration.ofMinutes(30));
                }));
            }
            assertThat(ready.await(10, SECONDS)).isTrue();
            start.countDown();
            AwsCredentials first = futures.getFirst().get(10, SECONDS);
            for (Future<AwsCredentials> future : futures) {
                assertThat(future.get(10, SECONDS)).isSameAs(first);
            }
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void testRefreshesCachedAssumeRoleCredentials()
            throws Exception
    {
        Instant now = Instant.now();
        AtomicInteger calls = new AtomicInteger();
        StsClient sts = new StsClient()
        {
            @Override
            public AssumeRoleResponse assumeRole(AssumeRoleRequest request)
            {
                assertThat(request.roleArn()).isEqualTo("arn:aws:iam::123456789012:role/test");
                assertThat(request.durationSeconds()).isEqualTo(7200);
                int call = calls.incrementAndGet();
                return AssumeRoleResponse.builder().credentials(Credentials.builder()
                                .accessKeyId("access" + call)
                                .secretAccessKey("secret")
                                .sessionToken("token" + call)
                                .expiration(call == 1 ? now.plusSeconds(600) : now.plusSeconds(7200))
                                .build())
                        .build();
            }

            @Override
            public String serviceName()
            {
                return "sts";
            }

            @Override
            public void close() {}
        };
        try (var provider = StsAssumeRoleCredentialsProvider.builder()
                .stsClient(sts)
                .refreshRequest(request -> request.roleArn("arn:aws:iam::123456789012:role/test").roleSessionName("test").durationSeconds(7200))
                .build()) {
            assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo("access1");
            S3PresigningCredentials credentials = new S3PresigningCredentials(provider);
            AwsCredentials fresh = credentials.resolve(Duration.ofHours(1));
            assertThat(fresh.accessKeyId()).isEqualTo("access2");
            assertThat(credentials.resolve(Duration.ofHours(1))).isSameAs(fresh);
            assertThat(calls.get()).isEqualTo(2);
        }
    }

    private static AwsSessionCredentials session(String accessKey, Instant expiration)
    {
        return AwsSessionCredentials.builder()
                .accessKeyId(accessKey)
                .secretAccessKey("secret")
                .sessionToken("token")
                .expirationTime(expiration)
                .build();
    }
}
