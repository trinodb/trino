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

import com.github.scribejava.apis.openid.OpenIdJsonTokenExtractor;
import com.github.scribejava.apis.openid.OpenIdOAuth2AccessToken;
import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.extractors.TokenExtractor;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.oauth.AccessTokenRequestParams;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.trino.server.security.oauth2.OAuth2Service.NONCE;
import static io.trino.server.security.oauth2.OAuth2Service.REDIRECT_URI;
import static io.trino.server.security.oauth2.OAuth2Service.STATE;
import static java.util.Objects.requireNonNull;

public class ScribeJavaOAuth2Client
        implements OAuth2Client
{
    private final DynamicCallbackOAuth2Service service;

    @Inject
    public ScribeJavaOAuth2Client(OAuth2Config config, @ForOAuth2 HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        requireNonNull(httpClient, "httpClient is null");
        service = new DynamicCallbackOAuth2Service(config, httpClient);
    }

    @Override
    public URI getAuthorizationUri(String state, URI callbackUri, Optional<String> nonceHash)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        parameters.put(REDIRECT_URI, callbackUri.toString());
        parameters.put(STATE, state);
        nonceHash.ifPresent(n -> parameters.put(NONCE, n));
        return URI.create(service.getAuthorizationUrl(parameters.buildOrThrow()));
    }

    @Override
    public OAuth2Response getOAuth2Response(String code, URI callbackUri)
            throws ChallengeFailedException
    {
        OpenIdOAuth2AccessToken accessToken = (OpenIdOAuth2AccessToken) service.getAccessToken(code, callbackUri.toString());
        return convertToOAuth2Response(accessToken);
    }

    @Override
    public OAuth2Response getRefreshedOAuth2Response(String refreshToken)
            throws IOException, InterruptedException, ExecutionException
    {
        OpenIdOAuth2AccessToken accessToken = (OpenIdOAuth2AccessToken) service.refreshAccessToken(refreshToken);
        return convertToOAuth2Response(accessToken);
    }

    private OAuth2Client.OAuth2Response convertToOAuth2Response(OpenIdOAuth2AccessToken accessToken)
    {
        Optional<Instant> validUntil = Optional.ofNullable(accessToken.getExpiresIn()).map(expiresSeconds -> Instant.now().plusSeconds(expiresSeconds));
        Optional<String> idToken = Optional.ofNullable(accessToken.getOpenIdToken());
        Optional<String> refreshToken = Optional.ofNullable(accessToken.getRefreshToken());
        return new OAuth2Response(accessToken.getAccessToken(), refreshToken, validUntil, idToken);
    }

    // Callback URI must be relative to client's view of the server.
    // For example, the client may be accessing the server through an HTTP proxy.
    static class DynamicCallbackOAuth2Service
            extends OAuth20Service
    {
        public DynamicCallbackOAuth2Service(OAuth2Config config, HttpClient httpClient)
        {
            super(
                    new OAuth2Api(
                            config.getTokenUrl(),
                            Optional.ofNullable(config.getRefreshTokenUrl()),
                            config.getAuthUrl()),
                    config.getClientId(),
                    config.getClientSecret(),
                    null,
                    String.join(" ", config.getScopes()),
                    "code",
                    null,
                    null,
                    null,
                    new ScribeHttpClient(httpClient));
        }

        public OAuth2AccessToken getAccessToken(String code, String callbackUrl)
                throws ChallengeFailedException
        {
            try {
                OAuthRequest request = createAccessTokenRequest(AccessTokenRequestParams.create(code));
                request.addParameter(REDIRECT_URI, callbackUrl);
                return sendAccessTokenRequestSync(request);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ChallengeFailedException("Interrupted while fetching access token", e);
            }
            catch (Exception e) {
                throw new ChallengeFailedException("Error while fetching access token", e);
            }
        }

        private static class OAuth2Api
                extends DefaultApi20
        {
            private final String accessTokenEndpoint;
            private final Optional<String> refreshTokenEndpoint;
            private final String authorizationBaseUrl;

            public OAuth2Api(String accessTokenEndpoint, Optional<String> refreshTokenEndpoint, String authorizationBaseUrl)
            {
                this.accessTokenEndpoint = requireNonNull(accessTokenEndpoint, "accessTokenEndpoint is null");
                this.refreshTokenEndpoint = requireNonNull(refreshTokenEndpoint, "refreshTokenEndpoint is null");
                this.authorizationBaseUrl = requireNonNull(authorizationBaseUrl, "authorizationBaseUrl is null");
            }

            @Override
            public String getAccessTokenEndpoint()
            {
                return accessTokenEndpoint;
            }

            @Override
            protected String getAuthorizationBaseUrl()
            {
                return authorizationBaseUrl;
            }

            @Override
            public String getRefreshTokenEndpoint()
            {
                return refreshTokenEndpoint.orElse(getAccessTokenEndpoint());
            }

            @Override
            public TokenExtractor<OAuth2AccessToken> getAccessTokenExtractor()
            {
                return OpenIdJsonTokenExtractor.instance();
            }
        }
    }
}
