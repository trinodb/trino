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
package io.trino.plugin.credential.oidc;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.airlift.http.client.FormDataBodyBuilder;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.json.JsonCodec;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.credential.BearerTokenCredential;
import io.trino.spi.security.credential.Credential;
import io.trino.spi.security.credential.CredentialProvider;

import java.time.Instant;
import java.util.Objects;

import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class OidcCredentialProvider
        implements CredentialProvider
{
    private static final JsonCodec<TokenExchangeResponse> TOKEN_EXCHANGE_RESPONSE_JSON_CODEC = jsonCodec(TokenExchangeResponse.class);
    private final LoadingCache<CacheKey, BearerTokenCredential> cache;

    @Inject
    public OidcCredentialProvider(@ForOidcCredentialProviderClient HttpClient httpClient, OidcCredentialProviderConfig config)
    {
        requireNonNull(config, "config is null");
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(config.getCacheSize())
                .build(new CacheLoader<>()
                {
                    @Override
                    public BearerTokenCredential load(CacheKey cacheKey)
                    {
                        Request request = preparePost()
                                .setUri(config.getTokenUrl())
                                .setMethod("POST")
                                .setHeader(CONTENT_TYPE, "application/x-www-form-urlencoded")
                                .setBodyGenerator(new FormDataBodyBuilder()
                                        .addField("client_id", requireNonNull(config.getClientId(), "clientId is null"))
                                        .addField("client_secret", requireNonNull(config.getClientSecret(), "clientSecret is null"))
                                        .addField("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
                                        .addField("subject_token", cacheKey.parentAccessToken())
                                        .addField("subject_token_type", "urn:ietf:params:oauth:token-type:access_token")
                                        .addField("scope", config.getScopes())
                                        .addField("audience", config.getAudience())
                                        .build())
                                .build();

                        StringResponseHandler.StringResponse response = httpClient.execute(request, createStringResponseHandler());
                        if (response.getStatusCode() != 200) {
                            throw new IllegalStateException(config.getTokenUrl() + " returned " + response.getStatusCode() + ": " + response.getBody());
                        }

                        TokenExchangeResponse exchangeResponse = TOKEN_EXCHANGE_RESPONSE_JSON_CODEC.fromJson(response.getBody());
                        return new BearerTokenCredential(exchangeResponse.accessToken(), Instant.now().plusSeconds(exchangeResponse.expiresIn()).minusSeconds(30));
                    }
                });
    }

    @Override
    public <T extends Credential> T getCredential(ConnectorIdentity identity, Class<T> type)
    {
        assertSupportedTypes(type, BearerTokenCredential.class);

        String parentAccessToken = requireNonNull(identity.getExtraCredentials().get("oauth2_access_token"), "oauth2_access_token not found in extra credentials");

        CacheKey cacheKey = new CacheKey(identity.getUser(), parentAccessToken);
        BearerTokenCredential credential = cache.getUnchecked(cacheKey);
        if (credential.isValid()) {
            return (T) credential;
        }
        cache.invalidate(cacheKey);
        return (T) cache.getUnchecked(cacheKey);
    }

    record CacheKey(String user, String parentAccessToken)
    {
        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof CacheKey cacheKey)) {
                return false;
            }
            return Objects.equals(user, cacheKey.user) && Objects.equals(parentAccessToken, cacheKey.parentAccessToken);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, parentAccessToken);
        }
    }
}
