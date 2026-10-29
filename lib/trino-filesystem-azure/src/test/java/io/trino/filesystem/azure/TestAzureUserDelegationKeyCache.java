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
package io.trino.filesystem.azure;

import com.azure.storage.blob.models.UserDelegationKey;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAzureUserDelegationKeyCache
{
    private static final OffsetDateTime START = OffsetDateTime.parse("2026-01-01T00:00:00Z");
    private static final String ENDPOINT = "https://account.blob.core.windows.net";

    @Test
    void testReuseAndRefresh()
    {
        AzureUserDelegationKeyCache cache = new AzureUserDelegationKeyCache();
        AtomicInteger loads = new AtomicInteger();
        BiFunction<OffsetDateTime, OffsetDateTime, UserDelegationKey> loader = (start, expiry) -> {
            loads.incrementAndGet();
            return key(start, expiry);
        };

        UserDelegationKey first = cache.get(ENDPOINT, START, START.plusHours(1), loader);
        assertThat(first.getSignedExpiry()).isEqualTo(START.plusDays(7));
        assertThat(cache.get(ENDPOINT, START.plusHours(1), START.plusDays(7), loader)).isSameAs(first);
        assertThat(loads.get()).isEqualTo(1);

        // Refresh before expiration when the existing key cannot cover the requested URI lifetime.
        UserDelegationKey refreshed = cache.get(ENDPOINT, START.plusDays(6), START.plusDays(8), loader);
        assertThat(refreshed).isNotSameAs(first);
        assertThat(refreshed.getSignedExpiry()).isEqualTo(START.plusDays(13));
        assertThat(loads.get()).isEqualTo(2);

        assertThat(cache.get(ENDPOINT, START.plusDays(14), START.plusDays(15), loader)).isNotSameAs(refreshed);
        assertThat(loads.get()).isEqualTo(3);
    }

    @Test
    void testUsesReturnedKeyValidity()
    {
        AzureUserDelegationKeyCache cache = new AzureUserDelegationKeyCache();
        UserDelegationKey first = cache.get(ENDPOINT, START, START.plusHours(1), (_, _) -> key(START, START.plusHours(2)));
        assertThat(cache.get(ENDPOINT, START.plusHours(1), START.plusHours(3), TestAzureUserDelegationKeyCache::key)).isNotSameAs(first);
    }

    @Test
    void testEarlierStartRequiresNewKey()
    {
        AzureUserDelegationKeyCache cache = new AzureUserDelegationKeyCache();
        UserDelegationKey first = cache.get(ENDPOINT, START, START.plusHours(1), TestAzureUserDelegationKeyCache::key);
        assertThat(cache.get(ENDPOINT, START.minusMinutes(1), START.plusHours(1), TestAzureUserDelegationKeyCache::key)).isNotSameAs(first);
    }

    @Test
    void testEndpointAndCredentialIsolation()
    {
        AzureUserDelegationKeyCache cache = new AzureUserDelegationKeyCache();
        UserDelegationKey first = cache.get(ENDPOINT, START, START.plusHours(1), TestAzureUserDelegationKeyCache::key);
        assertThat(cache.get("https://other.blob.core.windows.net", START, START.plusHours(1), TestAzureUserDelegationKeyCache::key)).isNotSameAs(first);
        assertThat(cache.get("https://account.blob.core.usgovcloudapi.net", START, START.plusHours(1), TestAzureUserDelegationKeyCache::key)).isNotSameAs(first);
        assertThat(new AzureUserDelegationKeyCache().get(ENDPOINT, START, START.plusHours(1), TestAzureUserDelegationKeyCache::key)).isNotSameAs(first);
    }

    @Test
    void testFailureCanBeRetried()
    {
        AzureUserDelegationKeyCache cache = new AzureUserDelegationKeyCache();
        assertThatThrownBy(() -> cache.get(ENDPOINT, START, START.plusHours(1), (_, _) -> {
            throw new IllegalStateException("key request failed");
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("key request failed");
        assertThat(cache.get(ENDPOINT, START, START.plusHours(1), TestAzureUserDelegationKeyCache::key).getSignedExpiry()).isEqualTo(START.plusDays(7));
    }

    @Test
    void testFailedRefreshPreservesUsableKey()
    {
        AzureUserDelegationKeyCache cache = new AzureUserDelegationKeyCache();
        UserDelegationKey first = cache.get(ENDPOINT, START, START.plusHours(1), TestAzureUserDelegationKeyCache::key);
        assertThatThrownBy(() -> cache.get(ENDPOINT, START.plusDays(6), START.plusDays(8), (_, _) -> {
            throw new IllegalStateException("key request failed");
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("key request failed");
        assertThat(cache.get(ENDPOINT, START.plusDays(6), START.plusDays(7), TestAzureUserDelegationKeyCache::key)).isSameAs(first);
        assertThat(cache.get(ENDPOINT, START.plusDays(6), START.plusDays(8), TestAzureUserDelegationKeyCache::key)).isNotSameAs(first);
    }

    @Test
    void testTimestampPrecision()
    {
        AzureUserDelegationKeyCache cache = new AzureUserDelegationKeyCache();
        OffsetDateTime start = START.plusNanos(123_456_789);
        UserDelegationKey first = cache.get(ENDPOINT, start, start.plusHours(1), TestAzureUserDelegationKeyCache::key);
        assertThat(first.getSignedStart()).isEqualTo(START);
        assertThat(first.getSignedExpiry()).isEqualTo(START.plusDays(7));
        assertThat(cache.get(ENDPOINT, start.plusSeconds(1), start.plusHours(1), TestAzureUserDelegationKeyCache::key)).isSameAs(first);
    }

    @Test
    void testConcurrentRequestsShareLoad()
            throws Exception
    {
        AzureUserDelegationKeyCache cache = new AzureUserDelegationKeyCache();
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(8);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = newFixedThreadPool(8)) {
            var futures = new ArrayList<Future<UserDelegationKey>>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(10, SECONDS)).isTrue();
                    return cache.get(ENDPOINT, START, START.plusHours(1), (keyStart, keyExpiry) -> {
                        loads.incrementAndGet();
                        return key(keyStart, keyExpiry);
                    });
                }));
            }
            assertThat(ready.await(10, SECONDS)).isTrue();
            start.countDown();
            UserDelegationKey first = futures.getFirst().get(10, SECONDS);
            for (Future<UserDelegationKey> future : futures) {
                assertThat(future.get(10, SECONDS)).isSameAs(first);
            }
            assertThat(loads.get()).isEqualTo(1);
        }
    }

    private static UserDelegationKey key(OffsetDateTime start, OffsetDateTime expiry)
    {
        return new UserDelegationKey().setSignedStart(start).setSignedExpiry(expiry);
    }
}
