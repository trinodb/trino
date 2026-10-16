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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.s3.S3RemoteSignerProvider;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.AuthSessionCache;
import org.apache.iceberg.rest.auth.OAuth2Util;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.hash.Hashing.sha256;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSigningContext.EXTRA_CREDENTIALS_PROPERTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.AUTH_SESSION_TIMEOUT_MS;
import static org.apache.iceberg.CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT;
import static org.apache.iceberg.util.PropertyUtil.propertyAsLong;
import static org.apache.iceberg.util.ThreadPools.authRefreshPool;

public final class IcebergRestCatalogS3RemoteSignerProvider
        implements S3RemoteSignerProvider, AutoCloseable
{
    private static final JsonCodec<Map<String, String>> AUTHENTICATION_CODEC = mapJsonCodec(String.class, String.class);

    private final HTTPClient httpClient;
    private final AuthSessionCache authSessions;

    @Inject
    public IcebergRestCatalogS3RemoteSignerProvider(IcebergRestCatalogPropertiesProvider propertiesProvider)
    {
        this(propertiesProvider.catalogProperties());
    }

    IcebergRestCatalogS3RemoteSignerProvider(Map<String, String> catalogProperties)
    {
        requireNonNull(catalogProperties, "catalogProperties is null");
        this.httpClient = HTTPClient.builder(catalogProperties)
                .withAuthSession(AuthSession.EMPTY)
                .build();
        this.authSessions = new AuthSessionCache(
                "s3-signer",
                Duration.ofMillis(propertyAsLong(catalogProperties, AUTH_SESSION_TIMEOUT_MS, AUTH_SESSION_TIMEOUT_MS_DEFAULT)));
    }

    @Override
    public Optional<HttpSigner<AwsCredentialsIdentity>> getSigner(ConnectorIdentity identity)
    {
        String contextJson = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_PROPERTY);
        if (contextJson == null) {
            return Optional.empty();
        }
        IcebergRestCatalogS3RemoteSigningContext context = IcebergRestCatalogS3RemoteSigningContext.fromJson(contextJson);
        // A token can be shared by tables with different refresh credentials or scopes
        String sessionKey = sha256().hashString(AUTHENTICATION_CODEC.toJson(new TreeMap<>(context.authentication())), UTF_8).toString();
        return Optional.of(new IcebergS3RemoteSigner(
                httpClient,
                () -> authSessions.cachedSession(sessionKey, _ -> createAuthSession(context.authentication())),
                context));
    }

    private OAuth2Util.AuthSession createAuthSession(Map<String, String> properties)
    {
        AuthConfig config = AuthConfig.fromProperties(properties);
        OAuth2Util.AuthSession parent = new OAuth2Util.AuthSession(OAuth2Util.authHeaders(config.token()), config);
        ScheduledExecutorService executor = config.keepRefreshed() ? authRefreshPool() : null;
        RESTClient refreshClient = refreshClient(properties);
        if (config.token() != null) {
            return OAuth2Util.AuthSession.fromAccessToken(refreshClient, executor, config.token(), config.expiresAtMillis(), parent);
        }
        if (config.credential() != null && !config.credential().isEmpty()) {
            return OAuth2Util.AuthSession.fromCredential(refreshClient, executor, config.credential(), parent);
        }
        return parent;
    }

    private RESTClient refreshClient(Map<String, String> properties)
    {
        Map<String, String> catalogHeaders = new HashMap<>(RESTUtil.configHeaders(properties));
        // Token requests must not inherit a catalog or table bearer token.
        catalogHeaders.keySet().removeIf("Authorization"::equalsIgnoreCase);
        return httpClient.withAuthSession(new OAuth2Util.AuthSession(catalogHeaders, AuthConfig.fromProperties(ImmutableMap.of())));
    }

    @PreDestroy
    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(httpClient);
            closer.register(authSessions::close);
        }
    }
}
