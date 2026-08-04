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
package io.trino.server.security.oauth2;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.airlift.http.client.FormDataBodyBuilder;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.json.JsonCodec;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.security.credential.ForCredentialProvider;
import io.trino.server.InternalCommunicationEncryption;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Credential;
import io.trino.spi.security.CredentialProvider;
import io.trino.spi.security.CredentialRequest;
import io.trino.spi.security.OAuth2Credential;
import io.trino.spi.security.OAuth2CredentialRequest;

import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class OAuth2CredentialProvider
        implements CredentialProvider
{
    private static final JsonCodec<TokenExchangeResponse> TOKEN_EXCHANGE_RESPONSE_JSON_CODEC = jsonCodec(TokenExchangeResponse.class);
    private final LoadingCache<CacheKey, OAuth2Credential> cache;

    @Inject
    public OAuth2CredentialProvider(@ForCredentialProvider HttpClient httpClient, OAuth2Config config, OAuth2ServerConfigProvider serverConfigProvider, InternalCommunicationEncryption internalCommunicationEncryption)
    {
        requireNonNull(config, "config is null");
        requireNonNull(internalCommunicationEncryption, "internalCommunicationEncryption is null");

        if (internalCommunicationEncryption.isInternalCommunicationEnabled() && config.getTokenExchangeAllowed()) {
            OAuth2ServerConfigProvider.OAuth2ServerConfig serverConfig = requireNonNull(serverConfigProvider.get(), "serverConfig is null");
            this.cache = EvictableCacheBuilder.newBuilder()
                    .maximumSize(config.getTokenExchangeCacheMaxSize())
                    .build(new CacheLoader<>()
                    {
                        @Override
                        public OAuth2Credential load(CacheKey cacheKey)
                        {
                            byte[] accessToken = internalCommunicationEncryption.decrypt(Base64.getDecoder().decode(cacheKey.encryptedAccessToken()));
                            Request request = preparePost()
                                    .setUri(serverConfig.tokenUrl())
                                    .setMethod("POST")
                                    .setHeader(CONTENT_TYPE, "application/x-www-form-urlencoded")
                                    .setBodyGenerator(new FormDataBodyBuilder()
                                            .addField("client_id", requireNonNull(config.getClientId(), "clientId is null"))
                                            .addField("client_secret", requireNonNull(config.getClientSecret(), "clientSecret is null"))
                                            .addField("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
                                            .addField("subject_token", new String(accessToken, UTF_8))
                                            .addField("subject_token_type", "urn:ietf:params:oauth:token-type:access_token")
                                            .addField("scope", cacheKey.scope())
                                            .addField("audience", cacheKey.audience())
                                            .build())
                                    .build();

                            StringResponseHandler.StringResponse response = httpClient.execute(request, createStringResponseHandler());
                            if (response.getStatusCode() != 200) {
                                throw new IllegalStateException(serverConfig.tokenUrl() + " returned " + response.getStatusCode() + ": " + response.getBody());
                            }

                            TokenExchangeResponse exchangeResponse = TOKEN_EXCHANGE_RESPONSE_JSON_CODEC.fromJson(response.getBody());
                            return new OAuth2Credential(exchangeResponse.accessToken(), Instant.now().plusSeconds(exchangeResponse.expiresIn()).minusSeconds(30));
                        }
                    });
        }
        else {
            this.cache = null;
        }
    }

    @Override
    public Optional<Credential> getCredential(ConnectorIdentity identity, CredentialRequest credentialRequest)
    {
        if (credentialRequest instanceof OAuth2CredentialRequest request) {
            if (cache == null) {
                throw new TrinoException(CONFIGURATION_INVALID, "http-server.authentication.oauth2.token-exchange.allowed should be true and internal-communication.shared-secret should be set to use this feature");
            }

            try {
                CacheKey cacheKey = CacheKey.of(identity, request);
                OAuth2Credential credential = cache.get(cacheKey);
                if (credential.isValid()) {
                    return Optional.of(credential);
                }
                cache.invalidate(cacheKey);
                return Optional.of(cache.get(cacheKey));
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
        return Optional.empty();
    }

    private record CacheKey(String user, String encryptedAccessToken, String audience, String scope)
    {
        public static CacheKey of(ConnectorIdentity identity, OAuth2CredentialRequest request)
        {
            String encryptedAccessToken = requireNonNull(identity.getExtraCredentials().get("oauth_access_token"), "extra credentials oauth_access_token is null");
            return new CacheKey(identity.getUser(), encryptedAccessToken, request.audience(), request.scope());
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof CacheKey cacheKey)) {
                return false;
            }
            return Objects.equals(user, cacheKey.user) && Objects.equals(encryptedAccessToken, cacheKey.encryptedAccessToken) && Objects.equals(scope, cacheKey.scope) && Objects.equals(audience, cacheKey.audience);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, encryptedAccessToken, audience, scope);
        }
    }
}
