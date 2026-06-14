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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.OAUTH2_SERVER_URI;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN_EXCHANGE_ENABLED;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN_REFRESH_ENABLED;

public class TrinoIcebergRestCatalogFactory
        implements TrinoCatalogFactory, AutoCloseable
{
    private static final Logger log = Logger.get(TrinoIcebergRestCatalogFactory.class);
    private static final int USER_SESSION_CATALOG_CACHE_SIZE = 1000;
    private static final Set<String> CREDENTIAL_KEYS = Set.of(TOKEN, CREDENTIAL);
    private static final Set<String> SESSION_PROPERTY_KEYS = Set.of(OAUTH2_SERVER_URI, TOKEN_REFRESH_ENABLED, TOKEN_EXCHANGE_ENABLED);

    private final IcebergFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final CatalogName catalogName;
    private final String trinoVersion;
    private final boolean nestedNamespaceEnabled;
    private final Security security;
    private final SessionType sessionType;
    private final boolean viewEndpointsEnabled;
    private final SecurityProperties securityProperties;
    private final IcebergRestCatalogPropertiesProvider catalogPropertiesProvider;
    private final boolean uniqueTableLocation;
    private final TypeManager typeManager;
    private final boolean caseInsensitiveNameMatching;
    private final Cache<Namespace, Namespace> remoteNamespaceMappingCache;
    private final Cache<TableIdentifier, TableIdentifier> remoteTableMappingCache;
    private final ConcurrentMap<String, CachedRestSessionCatalog> userSessionCatalogCache = new ConcurrentHashMap<>();
    private final AtomicBoolean missingCredentialsWarned = new AtomicBoolean(false);
    private final long sessionTimeoutMillis;

    private final Object catalogLock = new Object();

    @GuardedBy("catalogLock")
    private RESTSessionCatalog icebergCatalog;

    @Inject
    public TrinoIcebergRestCatalogFactory(
            IcebergFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            CatalogName catalogName,
            IcebergRestCatalogConfig restConfig,
            SecurityProperties securityProperties,
            IcebergRestCatalogPropertiesProvider catalogPropertiesProvider,
            IcebergConfig icebergConfig,
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(restConfig, "restConfig is null");
        this.nestedNamespaceEnabled = restConfig.isNestedNamespaceEnabled();
        this.security = restConfig.getSecurity();
        this.sessionType = restConfig.getSessionType();
        this.viewEndpointsEnabled = restConfig.isViewEndpointsEnabled();
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.catalogPropertiesProvider = requireNonNull(catalogPropertiesProvider, "catalogPropertiesProvider is null");
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.uniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.caseInsensitiveNameMatching = restConfig.isCaseInsensitiveNameMatching();
        this.remoteNamespaceMappingCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(restConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
        this.remoteTableMappingCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(restConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
        this.sessionTimeoutMillis = restConfig.getSessionTimeout().toMillis();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        requireNonNull(identity, "identity is null");
        RESTSessionCatalog restSessionCatalog = restSessionCatalog(identity);

        // `OAuth2Properties.SCOPE` is not set as scope passed through credentials is unused in
        // https://github.com/apache/iceberg/blob/229d8f6fcd109e6c8943ea7cbb41dab746c6d0ed/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L714-L721
        Map<String, String> credentials = Maps.filterKeys(securityProperties.get(), CREDENTIAL_KEYS::contains);
        Map<String, String> sessionProperties = Maps.filterKeys(securityProperties.get(), SESSION_PROPERTY_KEYS::contains);

        return new TrinoRestCatalog(
                fileSystemFactory,
                restSessionCatalog,
                catalogName,
                security,
                sessionType,
                credentials,
                sessionProperties,
                nestedNamespaceEnabled,
                trinoVersion,
                typeManager,
                uniqueTableLocation,
                caseInsensitiveNameMatching,
                remoteNamespaceMappingCache,
                remoteTableMappingCache,
                viewEndpointsEnabled,
                missingCredentialsWarned);
    }

    private RESTSessionCatalog restSessionCatalog(ConnectorIdentity identity)
    {
        if (security == Security.OAUTH2 && sessionType == SessionType.USER) {
            Map<String, String> sessionCredentials = OAuth2SessionCredentials.fromIdentity(identity);
            Map<String, String> catalogProperties = OAuth2SessionCredentials.catalogPropertiesWithSessionCredentials(catalogPropertiesProvider.catalogProperties(), sessionCredentials);

            return OAuth2SessionCredentials.cacheKey(identity)
                    .map(cacheKey -> cachedRestSessionCatalog(cacheKey, sessionCredentials, catalogProperties, identity))
                    .orElseGet(() -> sharedRestSessionCatalog(catalogProperties));
        }

        // Creation of the RESTSessionCatalog is lazy due to required network calls
        // for authorization and config route.
        return sharedRestSessionCatalog(catalogPropertiesProvider.catalogProperties());
    }

    private RESTSessionCatalog sharedRestSessionCatalog(Map<String, String> catalogProperties)
    {
        synchronized (catalogLock) {
            if (icebergCatalog == null) {
                icebergCatalog = newRestSessionCatalog(catalogProperties);
            }
            return icebergCatalog;
        }
    }

    private RESTSessionCatalog cachedRestSessionCatalog(String cacheKey, Map<String, String> sessionCredentials, Map<String, String> catalogProperties, ConnectorIdentity identity)
    {
        long now = System.currentTimeMillis();
        long expiresAtMillis = sessionCredentialsExpirationMillis(sessionCredentials, now);
        if (expiresAtMillis <= now) {
            throw new TrinoException(PERMISSION_DENIED, format("OAuth2 credentials for user '%s' have already expired", identity.getUser()));
        }

        while (true) {
            CachedRestSessionCatalog cached = userSessionCatalogCache.get(cacheKey);
            if (cached != null && !cached.expired(now)) {
                return cached.catalog();
            }

            if (cached == null) {
                CachedRestSessionCatalog newCached = new CachedRestSessionCatalog(newRestSessionCatalog(catalogProperties), expiresAtMillis);
                CachedRestSessionCatalog previous = userSessionCatalogCache.putIfAbsent(cacheKey, newCached);
                if (previous == null) {
                    cleanUpUserSessionCatalogCache(now);
                    return newCached.catalog();
                }

                closeCatalog(newCached);
                if (!previous.expired(now)) {
                    return previous.catalog();
                }
                userSessionCatalogCache.remove(cacheKey, previous);
                continue;
            }

            // cached != null and expired: re-read before the expensive network call in case
            // another thread already replaced it.
            CachedRestSessionCatalog current = userSessionCatalogCache.get(cacheKey);
            if (current != null && !current.expired(now)) {
                return current.catalog();
            }

            CachedRestSessionCatalog newCached = new CachedRestSessionCatalog(newRestSessionCatalog(catalogProperties), expiresAtMillis);
            if (userSessionCatalogCache.replace(cacheKey, cached, newCached)) {
                cleanUpUserSessionCatalogCache(now);
                return newCached.catalog();
            }
            closeCatalog(newCached);
        }
    }

    private RESTSessionCatalog newRestSessionCatalog(Map<String, String> catalogProperties)
    {
        RESTSessionCatalog icebergCatalogInstance = new RESTSessionCatalog(
                config -> HTTPClient.builder(config)
                        .uri(config.get(CatalogProperties.URI))
                        .withHeaders(RESTUtil.configHeaders(config))
                        .build(),
                (context, config) -> {
                    ConnectorIdentity currentIdentity = (context.wrappedIdentity() != null)
                            ? ((ConnectorIdentity) context.wrappedIdentity())
                            : ConnectorIdentity.ofUser("fake");
                    return fileIoFactory.create(fileSystemFactory.create(currentIdentity, config), true, config);
                });
        icebergCatalogInstance.initialize(catalogName.toString(), catalogProperties);
        return icebergCatalogInstance;
    }

    private long sessionCredentialsExpirationMillis(Map<String, String> sessionCredentials, long now)
    {
        String token = sessionCredentials.get(TOKEN);
        if (token == null) {
            return now + sessionTimeoutMillis;
        }
        return tokenExpirationMillis(token).orElse(now + sessionTimeoutMillis);
    }

    // Parses the "exp" claim from the JWT payload using simple string matching.
    // Returns empty on any parse failure, causing the caller to use sessionTimeoutMillis as TTL.
    private static OptionalLong tokenExpirationMillis(String token)
    {
        String[] segments = token.split("\\.");
        if (segments.length < 2) {
            return OptionalLong.empty();
        }

        try {
            String payload = new String(Base64.getUrlDecoder().decode(segments[1]), UTF_8);
            int expirationKeyIndex = payload.indexOf("\"exp\"");
            if (expirationKeyIndex < 0) {
                return OptionalLong.empty();
            }

            int separatorIndex = payload.indexOf(':', expirationKeyIndex);
            if (separatorIndex < 0) {
                return OptionalLong.empty();
            }

            int start = separatorIndex + 1;
            while (start < payload.length() && Character.isWhitespace(payload.charAt(start))) {
                start++;
            }

            int end = start;
            while (end < payload.length() && Character.isDigit(payload.charAt(end))) {
                end++;
            }
            if (start == end) {
                return OptionalLong.empty();
            }

            return OptionalLong.of(Long.parseLong(payload.substring(start, end)) * 1000);
        }
        catch (IllegalArgumentException e) {
            return OptionalLong.empty();
        }
    }

    private void cleanUpUserSessionCatalogCache(long now)
    {
        // TODO: RESTSessionCatalog instances evicted from this cache are not closed because
        // TrinoRestCatalog instances holding references to them have no lifecycle callback
        // when a Trino transaction completes. Closing them prematurely causes active requests
        // to fail. Evicted catalogs are only closed when the factory itself shuts down.
        // A reference-counting scheme or Closeable wrapper would fix this properly.
        userSessionCatalogCache.forEach((cacheKey, cached) -> {
            if (cached.expired(now)) {
                userSessionCatalogCache.remove(cacheKey, cached);
            }
        });

        int entriesToEvict = userSessionCatalogCache.size() - USER_SESSION_CATALOG_CACHE_SIZE;
        if (entriesToEvict <= 0) {
            return;
        }

        for (Map.Entry<String, CachedRestSessionCatalog> entry : userSessionCatalogCache.entrySet()) {
            if (entriesToEvict <= 0) {
                break;
            }
            if (userSessionCatalogCache.remove(entry.getKey(), entry.getValue())) {
                entriesToEvict--;
            }
        }
    }

    private static void closeCatalog(CachedRestSessionCatalog catalog)
    {
        if (catalog == null) {
            return;
        }

        try {
            catalog.catalog().close();
        }
        catch (IOException | RuntimeException e) {
            log.warn(e, "Failed to close Iceberg REST session catalog");
        }
    }

    @Override
    public void close()
    {
        RESTSessionCatalog catalogToClose;
        synchronized (catalogLock) {
            catalogToClose = icebergCatalog;
        }
        if (catalogToClose != null) {
            try {
                catalogToClose.close();
            }
            catch (IOException | RuntimeException e) {
                log.warn(e, "Failed to close Iceberg REST catalog");
            }
        }

        userSessionCatalogCache.values().forEach(TrinoIcebergRestCatalogFactory::closeCatalog);
        userSessionCatalogCache.clear();
    }

    private record CachedRestSessionCatalog(RESTSessionCatalog catalog, long expiresAtMillis)
    {
        private CachedRestSessionCatalog
        {
            requireNonNull(catalog, "catalog is null");
        }

        private boolean expired(long now)
        {
            return expiresAtMillis <= now;
        }
    }
}
