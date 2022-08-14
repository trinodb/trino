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
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreAuthenticationConfig.ThriftMetastoreAuthenticationType;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.thrift.TException;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TokenDelegationThriftMetastoreFactory
{
    private final MetastoreLocator clientProvider;
    private final boolean impersonationEnabled;
    private final boolean authenticationEnabled;
    private final NonEvictableLoadingCache<String, String> delegationTokenCache;

    @Inject
    public TokenDelegationThriftMetastoreFactory(
            MetastoreLocator metastoreLocator,
            ThriftMetastoreConfig thriftConfig,
            ThriftMetastoreAuthenticationConfig authenticationConfig,
            HdfsEnvironment hdfsEnvironment)
    {
        this.clientProvider = requireNonNull(metastoreLocator, "metastoreLocator is null");
        this.impersonationEnabled = thriftConfig.isImpersonationEnabled();
        this.authenticationEnabled = authenticationConfig.getAuthenticationType() != ThriftMetastoreAuthenticationType.NONE;

        this.delegationTokenCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(thriftConfig.getDelegationTokenCacheTtl().toMillis(), MILLISECONDS)
                        .maximumSize(thriftConfig.getDelegationTokenCacheMaximumSize()),
                CacheLoader.from(this::loadDelegationToken));
    }

    private ThriftMetastoreClient createMetastoreClient()
            throws TException
    {
        return clientProvider.createMetastoreClient(Optional.empty());
    }

    public ThriftMetastoreClient createMetastoreClient(Optional<ConnectorIdentity> identity)
            throws TException
    {
        if (!impersonationEnabled) {
            return createMetastoreClient();
        }

        String username = identity.map(ConnectorIdentity::getUser)
                .orElseThrow(() -> new IllegalStateException("End-user name should exist when metastore impersonation is enabled"));
        if (authenticationEnabled) {
            String delegationToken;
            try {
                delegationToken = delegationTokenCache.getUnchecked(username);
            }
            catch (UncheckedExecutionException e) {
                throwIfInstanceOf(e.getCause(), TrinoException.class);
                throw e;
            }
            return clientProvider.createMetastoreClient(Optional.of(delegationToken));
        }

        ThriftMetastoreClient client = createMetastoreClient();
        setMetastoreUserOrClose(client, username);
        return client;
    }

    private String loadDelegationToken(String username)
    {
        try (ThriftMetastoreClient client = createMetastoreClient()) {
            return client.getDelegationToken(username);
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static void setMetastoreUserOrClose(ThriftMetastoreClient client, String username)
            throws TException
    {
        try {
            client.setUGI(username);
        }
        catch (Throwable t) {
            // close client and suppress any error from close
            try (Closeable ignored = client) {
                throw t;
            }
            catch (IOException e) {
                // impossible; will be suppressed
            }
        }
    }
}
