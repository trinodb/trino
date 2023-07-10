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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.plugin.base.security.UserNameProvider;
import io.trino.plugin.hive.ForHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.thrift.TException;

import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TokenFetchingMetastoreClientFactory
        implements IdentityAwareMetastoreClientFactory
{
    private final TokenAwareMetastoreClientFactory clientProvider;
    private final UserNameProvider userNameProvider;
    private final boolean impersonationEnabled;
    private final NonEvictableLoadingCache<String, DelegationToken> delegationTokenCache;
    private final long refreshPeriod;

    @Inject
    public TokenFetchingMetastoreClientFactory(
            TokenAwareMetastoreClientFactory tokenAwareMetastoreClientFactory,
            @ForHiveMetastore UserNameProvider userNameProvider,
            ThriftMetastoreConfig thriftConfig)
    {
        this.clientProvider = requireNonNull(tokenAwareMetastoreClientFactory, "tokenAwareMetastoreClientFactory is null");
        this.impersonationEnabled = thriftConfig.isImpersonationEnabled();
        this.userNameProvider = requireNonNull(userNameProvider, "userNameProvider is null");

        this.delegationTokenCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(thriftConfig.getDelegationTokenCacheTtl().toMillis(), MILLISECONDS)
                        .maximumSize(thriftConfig.getDelegationTokenCacheMaximumSize()),
                CacheLoader.from(this::loadDelegationToken));
        this.refreshPeriod = Duration.ofMinutes(1).toNanos();
    }

    private ThriftMetastoreClient createMetastoreClient()
            throws TException
    {
        return clientProvider.createMetastoreClient(Optional.empty());
    }

    @Override
    public ThriftMetastoreClient createMetastoreClientFor(Optional<ConnectorIdentity> identity)
            throws TException
    {
        if (!impersonationEnabled) {
            return createMetastoreClient();
        }

        String username = identity.map(userNameProvider::get)
                .orElseThrow(() -> new IllegalStateException("End-user name should exist when metastore impersonation is enabled"));

        DelegationToken cachedDelegationToken = getDelegationToken(username);
        try {
            return clientProvider.createMetastoreClient(Optional.of(cachedDelegationToken.delegationToken()));
        }
        catch (TException e) {
            // Since the cached token may expire due to reasons such as restarting the Hive Metastore, refresh a delegation token and try to connect again.
            // Additionally, to avoid overloading the metastore, refreshes per user are executed at most once per minute.
            if (System.nanoTime() - cachedDelegationToken.writeTimeNanos() >= this.refreshPeriod) {
                DelegationToken refreshDelegationToken = loadDelegationToken(username);
                delegationTokenCache.put(username, refreshDelegationToken);
                return clientProvider.createMetastoreClient(Optional.of(refreshDelegationToken.delegationToken()));
            }
            throw e;
        }
    }

    private DelegationToken getDelegationToken(String username)
    {
        try {
            return delegationTokenCache.getUnchecked(username);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private DelegationToken loadDelegationToken(String username)
    {
        try (ThriftMetastoreClient client = createMetastoreClient()) {
            return new DelegationToken(System.nanoTime(), client.getDelegationToken(username));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private record DelegationToken(long writeTimeNanos, String delegationToken)
    {
        public DelegationToken
        {
            requireNonNull(delegationToken, "delegationToken is null");
        }
    }
}
