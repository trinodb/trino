/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.auth;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.cache.NonEvictableLoadingCache;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.plugin.base.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CachingSnowflakeOauthService
        implements SnowflakeOauthService
{
    private static final Logger log = Logger.get(CachingSnowflakeOauthService.class);
    // Snowflake OAuth access token TTL (doesn't seem to be configurable)
    private static final Duration ACCESS_TOKEN_TTL = new Duration(10, MINUTES);
    private static final Duration TTL_WINDOW = new Duration(2, MINUTES);

    private final SnowflakeOauthService delegate;
    private final NonEvictableLoadingCache<UserPassword, OauthCredential> accessTokenCache;
    private final CredentialProvider credentialProvider;

    public CachingSnowflakeOauthService(SnowflakeOauthService delegate, CredentialProvider credentialProvider, Duration ttl, int cacheSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
        accessTokenCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(ofMinutes(ttl.roundTo(MINUTES) - TTL_WINDOW.roundTo(MINUTES)))
                        // access tokens are valid for 10 minutes; we make them eligible for refresh a bit earlier, to leave ourselves some headroom;
                        // entries are refreshed only on access (if eligible), so we won't waste resources refreshing unused credentials
                        .refreshAfterWrite(ofMinutes(ACCESS_TOKEN_TTL.roundTo(MINUTES) - TTL_WINDOW.roundTo(MINUTES)))
                        .maximumSize(cacheSize),
                new OauthCacheLoader(delegate));
    }

    @Override
    public OauthCredential getCredential(ConnectorIdentity identity)
    {
        try {
            return accessTokenCache.get(new UserPassword(identity, credentialProvider));
        }
        catch (ExecutionException e) {
            log.error(e, "Failed to retrieve credential for [%s] from the cache", identity);
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public OauthCredential refreshCredential(OauthCredential credential)
    {
        return delegate.refreshCredential(credential);
    }

    private static class UserPassword
    {
        private final ConnectorIdentity identity;
        private final String serializedCredential;

        public UserPassword(ConnectorIdentity identity, CredentialProvider credentialProvider)
        {
            this.identity = identity;
            String user = credentialProvider.getConnectionUser(Optional.of(identity))
                    .orElseThrow(() -> new IllegalStateException("Cannot determine user name"));
            String password = credentialProvider.getConnectionPassword(Optional.of(identity))
                    .orElseThrow(() -> new IllegalStateException("Cannot determine password"));
            serializedCredential = format("%s:%s", user, password);
        }

        public ConnectorIdentity getIdentity()
        {
            return identity;
        }

        @Override
        public int hashCode()
        {
            return serializedCredential.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            UserPassword that = (UserPassword) obj;

            return this.serializedCredential.equals(that.serializedCredential);
        }
    }

    private static class OauthCacheLoader
            extends CacheLoader<UserPassword, OauthCredential>
    {
        private final SnowflakeOauthService snowflakeOauthService;

        public OauthCacheLoader(SnowflakeOauthService snowflakeOauthService)
        {
            this.snowflakeOauthService = requireNonNull(snowflakeOauthService, "snowflakeOauthService is null");
        }

        @Override
        public OauthCredential load(UserPassword key)
        {
            return snowflakeOauthService.getCredential(key.getIdentity());
        }

        @Override
        public ListenableFuture<OauthCredential> reload(UserPassword key, OauthCredential credential)
        {
            if (credential.getRefreshTokenExpirationTime() < credential.getAccessTokenExpirationTime()) {
                log.debug("Refresh token is about to expire for [%s], not refreshing access token", key.getIdentity());
                // the user needs to re-authenticate soon, we'll get a new access token at that point
                return immediateFuture(credential);
            }
            else {
                log.debug("Refreshing authorization token for [%s]", key.getIdentity());
                // TODO: consider refreshing asynchronously
                return immediateFuture(snowflakeOauthService.refreshCredential(credential));
            }
        }
    }
}
