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
import com.google.common.collect.Maps;
import io.airlift.json.JsonCodec;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;

import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.PropertyUtil.propertyAsBoolean;

/**
 * OAuth settings of the session that authenticates the S3 signing requests of one table, and the
 * catalog headers of its token requests. The catalog client derives them when it loads the table.
 */
public record IcebergRestCatalogS3RemoteSigningAuthentication(
        Optional<String> token,
        Optional<Long> tokenExpiresInMillis,
        Optional<String> credential,
        String oauth2ServerUri,
        String scope,
        Optional<String> audience,
        Optional<String> resource,
        boolean tokenRefreshEnabled,
        boolean tokenExchangeEnabled,
        Map<String, String> headers)
{
    static final String AUTHENTICATION_PROPERTY = "internal$iceberg_rest_s3_signer_authentication$";

    private static final JsonCodec<IcebergRestCatalogS3RemoteSigningAuthentication> CODEC = jsonCodec(IcebergRestCatalogS3RemoteSigningAuthentication.class);
    // Iceberg's S3 signer requests tokens with this scope unless the catalog configures one
    private static final String DEFAULT_SCOPE = "sign";

    public IcebergRestCatalogS3RemoteSigningAuthentication
    {
        requireNonNull(token, "token is null");
        requireNonNull(tokenExpiresInMillis, "tokenExpiresInMillis is null");
        requireNonNull(credential, "credential is null");
        requireNonNull(oauth2ServerUri, "oauth2ServerUri is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(audience, "audience is null");
        requireNonNull(resource, "resource is null");
        headers = ImmutableMap.copyOf(requireNonNull(headers, "headers is null"));
    }

    static IcebergRestCatalogS3RemoteSigningAuthentication create(Map<String, String> catalogProperties, Map<String, String> tableProperties, AuthSession catalogSession)
    {
        Optional<String> token;
        Optional<String> credential;
        Optional<Long> tokenExpiresInMillis = parseTokenExpiresInMillis(tableProperties);
        if (tableProperties.containsKey(OAuth2Properties.TOKEN) || tableProperties.containsKey(OAuth2Properties.CREDENTIAL)) {
            token = Optional.ofNullable(tableProperties.get(OAuth2Properties.TOKEN));
            credential = Optional.ofNullable(tableProperties.get(OAuth2Properties.CREDENTIAL));
        }
        else if (!catalogProperties.getOrDefault(OAuth2Properties.CREDENTIAL, "").isEmpty()) {
            // A client credential takes precedence over a configured token. The catalog session works
            // the same way: it fetches its token with the credential during initialization.
            token = Optional.empty();
            credential = Optional.of(catalogProperties.get(OAuth2Properties.CREDENTIAL));
        }
        else {
            token = Optional.ofNullable(catalogProperties.get(OAuth2Properties.TOKEN));
            credential = Optional.ofNullable(catalogProperties.get(OAuth2Properties.CREDENTIAL));
            tokenExpiresInMillis = tokenExpiresInMillis.or(() -> parseTokenExpiresInMillis(catalogProperties));
            // The catalog properties hold the configured token, which the catalog session may have refreshed since
            if (token.isPresent() && catalogSession instanceof OAuth2Util.AuthSession oauthSession && oauthSession.token() != null) {
                token = Optional.of(oauthSession.token());
                // The configured lifetime applies to the configured token
                tokenExpiresInMillis = Optional.empty();
            }
        }

        // Token requests must not inherit a catalog bearer token
        Map<String, String> headers = Maps.filterKeys(RESTUtil.configHeaders(catalogProperties), name -> !name.equalsIgnoreCase("Authorization"));
        return new IcebergRestCatalogS3RemoteSigningAuthentication(
                token,
                tokenExpiresInMillis,
                credential,
                RESTUtil.resolveEndpoint(
                        catalogProperties.get(CatalogProperties.URI),
                        catalogProperties.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens())),
                catalogProperties.getOrDefault(OAuth2Properties.SCOPE, DEFAULT_SCOPE),
                Optional.ofNullable(catalogProperties.get(OAuth2Properties.AUDIENCE)),
                Optional.ofNullable(catalogProperties.get(OAuth2Properties.RESOURCE)),
                propertyAsBoolean(catalogProperties, OAuth2Properties.TOKEN_REFRESH_ENABLED, OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT),
                propertyAsBoolean(catalogProperties, OAuth2Properties.TOKEN_EXCHANGE_ENABLED, OAuth2Properties.TOKEN_EXCHANGE_ENABLED_DEFAULT),
                headers);
    }

    AuthConfig toAuthConfig()
    {
        ImmutableMap.Builder<String, String> optionalOAuthParams = ImmutableMap.builder();
        optionalOAuthParams.put(OAuth2Properties.SCOPE, scope);
        audience.ifPresent(value -> optionalOAuthParams.put(OAuth2Properties.AUDIENCE, value));
        resource.ifPresent(value -> optionalOAuthParams.put(OAuth2Properties.RESOURCE, value));
        AuthConfig config = AuthConfig.builder()
                .token(token.orElse(null))
                .credential(credential.orElse(null))
                .scope(scope)
                .oauth2ServerUri(oauth2ServerUri)
                .optionalOAuthParams(optionalOAuthParams.buildOrThrow())
                .keepRefreshed(tokenRefreshEnabled)
                .exchangeEnabled(tokenExchangeEnabled)
                .build();
        if (config.expiresAtMillis() != null || tokenExpiresInMillis.isEmpty()) {
            return config;
        }
        // Like AuthConfig.fromProperties, use the configured lifetime when the token has no expiry claim
        return AuthConfig.builder()
                .from(config)
                .expiresAtMillis(System.currentTimeMillis() + tokenExpiresInMillis.get())
                .build();
    }

    static IcebergRestCatalogS3RemoteSigningAuthentication fromJson(String json)
    {
        return CODEC.fromJson(json);
    }

    String toJson()
    {
        return CODEC.toJson(this);
    }

    private static Optional<Long> parseTokenExpiresInMillis(Map<String, String> properties)
    {
        return Optional.ofNullable(properties.get(OAuth2Properties.TOKEN_EXPIRES_IN_MS)).map(Long::parseLong);
    }
}
