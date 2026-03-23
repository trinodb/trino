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
package io.trino.plugin.iceberg;

import com.google.common.cache.Cache;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.iceberg.IcebergUtil.CREDENTIAL_CACHE_TTL;
import static io.trino.plugin.iceberg.IcebergUtil.maybeRefreshVendedCredentials;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IcebergUtil#maybeRefreshVendedCredentials}, which provides the
 * worker-side credential refresh logic for long-running queries against the Iceberg REST catalog.
 */
class TestIcebergVendedCredentialRefresh
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");
    private static final Map<String, String> OLD_PROPS = Map.of("s3.access-key-id", "old-key");
    private static final Map<String, String> FRESH_PROPS = Map.of("s3.access-key-id", "fresh-key");

    // ------------------------------------------------------------------
    // Helper: credentials whose timestamp is artificially in the past.
    // ------------------------------------------------------------------

    private static IcebergTableCredentials staleCredentials()
    {
        long fetchedAt = System.currentTimeMillis() - CREDENTIAL_CACHE_TTL.toMillis() - 1_000;
        return new IcebergTableCredentials(OLD_PROPS, fetchedAt);
    }

    private static IcebergTableCredentials freshCredentials()
    {
        // Fetched just now – well within the TTL.
        return new IcebergTableCredentials(OLD_PROPS, System.currentTimeMillis());
    }

    private static Cache<SchemaTableName, IcebergTableCredentials> newCache()
    {
        return EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(5))
                .shareNothingWhenDisabled()
                .build();
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    @Test
    void testEmptyCredentialsAreReturnedUnchanged()
    {
        Optional<ConnectorTableCredentials> result = maybeRefreshVendedCredentials(
                newCache(), TABLE, Optional.empty(), () -> { throw new AssertionError("should not be called"); });
        assertThat(result).isEmpty();
    }

    @Test
    void testFreshCredentialsAreNotRefreshed()
    {
        AtomicInteger callCount = new AtomicInteger();
        Optional<ConnectorTableCredentials> result = maybeRefreshVendedCredentials(
                newCache(),
                TABLE,
                Optional.of(freshCredentials()),
                () -> {
                    callCount.incrementAndGet();
                    return new IcebergTableCredentials(FRESH_PROPS, System.currentTimeMillis());
                });

        assertThat(callCount.get()).isEqualTo(0);
        assertThat(result).isPresent();
        assertThat(((IcebergTableCredentials) result.get()).fileIoProperties()).isEqualTo(OLD_PROPS);
    }

    @Test
    void testStaleCredentialsAreRefreshed()
    {
        AtomicInteger callCount = new AtomicInteger();
        Optional<ConnectorTableCredentials> result = maybeRefreshVendedCredentials(
                newCache(),
                TABLE,
                Optional.of(staleCredentials()),
                () -> {
                    callCount.incrementAndGet();
                    return new IcebergTableCredentials(FRESH_PROPS, System.currentTimeMillis());
                });

        assertThat(callCount.get()).isEqualTo(1);
        assertThat(result).isPresent();
        assertThat(((IcebergTableCredentials) result.get()).fileIoProperties()).isEqualTo(FRESH_PROPS);
    }

    @Test
    void testRefreshIsCoalescedByCache()
    {
        // When multiple concurrent requests arrive for the same stale table, the refresher
        // should be called only once; subsequent calls return the cached result.
        AtomicInteger callCount = new AtomicInteger();
        Cache<SchemaTableName, IcebergTableCredentials> sharedCache = newCache();

        for (int i = 0; i < 5; i++) {
            Optional<ConnectorTableCredentials> result = maybeRefreshVendedCredentials(
                    sharedCache,
                    TABLE,
                    Optional.of(staleCredentials()),
                    () -> {
                        callCount.incrementAndGet();
                        return new IcebergTableCredentials(FRESH_PROPS, System.currentTimeMillis());
                    });
            assertThat(result).isPresent();
            assertThat(((IcebergTableCredentials) result.get()).fileIoProperties()).isEqualTo(FRESH_PROPS);
        }

        assertThat(callCount.get()).isEqualTo(1);
    }

    @Test
    void testCredentialsWithoutTimestampAreNotRefreshed()
    {
        // credentialsFetchedAtMs == 0 indicates non-REST catalog credentials; must not refresh.
        IcebergTableCredentials noTimestamp = new IcebergTableCredentials(OLD_PROPS, 0L);
        AtomicInteger callCount = new AtomicInteger();

        Optional<ConnectorTableCredentials> result = maybeRefreshVendedCredentials(
                newCache(),
                TABLE,
                Optional.of(noTimestamp),
                () -> {
                    callCount.incrementAndGet();
                    return new IcebergTableCredentials(FRESH_PROPS, System.currentTimeMillis());
                });

        assertThat(callCount.get()).isEqualTo(0);
        assertThat(((IcebergTableCredentials) result.get()).fileIoProperties()).isEqualTo(OLD_PROPS);
    }
}
