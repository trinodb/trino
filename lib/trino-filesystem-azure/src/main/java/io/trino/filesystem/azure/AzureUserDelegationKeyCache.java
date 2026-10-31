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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.trino.cache.NonEvictableLoadingCache;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Ticker.systemTicker;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.time.temporal.ChronoUnit.SECONDS;

final class AzureUserDelegationKeyCache
{
    // Each cache belongs to one OAuth credential, and keys are scoped to the storage endpoint.
    private final NonEvictableLoadingCache<String, CachedKey> keys;

    public AzureUserDelegationKeyCache()
    {
        this(systemTicker());
    }

    @VisibleForTesting
    AzureUserDelegationKeyCache(Ticker ticker)
    {
        // Local signing cannot observe revocation failures seen by clients. Limit reuse even when
        // a key's signed validity is longer, so revoked keys are eventually replaced.
        keys = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .maximumSize(1000)
                        .expireAfterWrite(Duration.ofMinutes(5))
                        .ticker(ticker),
                CacheLoader.from(CachedKey::new));
    }

    public UserDelegationKey get(
            String endpoint,
            OffsetDateTime startTime,
            OffsetDateTime expiryTime,
            BiFunction<OffsetDateTime, OffsetDateTime, UserDelegationKey> loader)
    {
        return keys.getUnchecked(endpoint).get(startTime, expiryTime, loader);
    }

    private static final class CachedKey
    {
        private Optional<UserDelegationKey> key = Optional.empty();

        public synchronized UserDelegationKey get(
                OffsetDateTime startTime,
                OffsetDateTime expiryTime,
                BiFunction<OffsetDateTime, OffsetDateTime, UserDelegationKey> loader)
        {
            if (key.isPresent() &&
                    !key.orElseThrow().getSignedStart().isAfter(startTime) &&
                    !key.orElseThrow().getSignedExpiry().isBefore(expiryTime)) {
                return key.orElseThrow();
            }

            // Azure serializes timestamps with second precision and allows keys valid for up to seven days.
            // Request the full lifetime so subsequent URIs can reuse the key without extending their own TTL.
            OffsetDateTime keyStart = startTime.truncatedTo(SECONDS);
            OffsetDateTime keyExpiry = keyStart.plusDays(7);
            if (expiryTime.isAfter(keyExpiry)) {
                // Let Azure reject requests exceeding its maximum lifetime, as for uncached requests.
                keyExpiry = expiryTime;
            }
            UserDelegationKey loaded = loader.apply(keyStart, keyExpiry);
            key = Optional.of(loaded);
            return loaded;
        }
    }
}
