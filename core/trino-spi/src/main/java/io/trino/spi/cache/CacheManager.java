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
package io.trino.spi.cache;

import io.trino.spi.HostAddress;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.io.Closeable;
import java.util.Optional;

public interface CacheManager
{
    /**
     * @return {@link SplitCache} for a given {@link PlanSignature}.
     * Matching of {@link PlanSignature} per split could be expensive,
     * therefore {@link SplitCache} is used to load or store data per split.
     */
    SplitCache getSplitCache(PlanSignature signature);

    /**
     * @return {@link PreferredAddressProvider} for a given {@link PlanSignature}.
     * {@link PreferredAddressProvider} can be used to return a preferred worker
     * on which split should be processed in order to improve cache hit ratio.
     */
    PreferredAddressProvider getPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager);

    /**
     * Triggers a memory revoke. {@link CacheManager} should revoke
     * at least {@code bytesToRevoke} bytes (if it has allocated
     * that much revocable memory) before allocating new memory.
     *
     * @return the number of revoked bytes
     */
    long revokeMemory(long bytesToRevoke);

    interface SplitCache
            extends Closeable
    {
        /**
         * @param predicate Predicate that should be enforced on cached rows.
         * Output of `cachedSplitA` can be used to derive output of matching `cachedSplitB`
         * (with corresponding {@link PlanSignature}) as long as `cachedSplitB.predicate` is a strict
         * subset of `cachedSplitA.predicate`. To do so, `cachedSplitB.predicate` must be
         * applied on output of `cachedSplitA`. Before serialization as a cache key, predicate
         * needs to be normalized using {@link io.trino.plugin.base.cache.CacheUtils#normalizeTupleDomain(TupleDomain)}.
         * @param unenforcedPredicate Unenforced (best-effort) predicate that should be applied on cached rows.
         * Output of `cachedSplitA` can be used to derive output of matching `cachedSplitB`
         * (with corresponding {@link PlanSignature}) as long as `cachedSplitB.unenforcedPredicate`
         * is a subset of `cachedSplitA.unenforcedPredicate`. Before serialization as a cache key, predicate
         * needs to be normalized using {@link io.trino.plugin.base.cache.CacheUtils#normalizeTupleDomain(TupleDomain)}.
         * @return cached pages for a given split.
         */
        Optional<ConnectorPageSource> loadPages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate);

        /**
         * @param predicate Predicate that was enforced on cached rows.
         * @param unenforcedPredicate Best-effort predicate that was applied on cached rows.
         * @return {@link ConnectorPageSink} for caching pages for a given split.
         * Might be empty if there isn't sufficient memory or split data is
         * already cached.
         */
        Optional<ConnectorPageSink> storePages(CacheSplitId splitId, TupleDomain<CacheColumnId> predicate, TupleDomain<CacheColumnId> unenforcedPredicate);
    }

    interface PreferredAddressProvider
    {
        HostAddress getPreferredAddress(CacheSplitId splitId);
    }
}
