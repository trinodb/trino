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
package io.prestosql.server.security.oauth2;

import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthConstants;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.oauth.AccessTokenRequestParams;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ScribeJavaOAuth2Client
        implements OAuth2Client
{
    private final DynamicCallbackOAuth2Service service;

    @Inject
    public ScribeJavaOAuth2Client(OAuth2Config config)
    {
        service = new DynamicCallbackOAuth2Service(config);
    }

    @Override
    public URI getAuthorizationUri(String state, URI callbackUri)
    {
        return URI.create(service.getAuthorizationUrl(ImmutableMap.<String, String>builder()
                .put(OAuthConstants.REDIRECT_URI, callbackUri.toString())
                .put(OAuthConstants.STATE, state)
                .build()));
    }

    @Override
    public AccessToken getAccessToken(String code, URI callbackUri)
            throws ChallengeFailedException
    {
        OAuth2AccessToken accessToken = service.getAccessToken(code, callbackUri.toString());
        Optional<Instant> validUntil = Optional.ofNullable(accessToken.getExpiresIn()).map(expiresSeconds -> Instant.now().plusSeconds(expiresSeconds));
        return new AccessToken(accessToken.getAccessToken(), validUntil);
    }

    // Callback URI must be relative to client's view of the server.
    // For example, the client may be accessing the server through an HTTP proxy.
    private static class DynamicCallbackOAuth2Service
            extends OAuth20Service
    {
        public DynamicCallbackOAuth2Service(OAuth2Config config)
        {
            super(
                    new OAuth2Api(config.getTokenUrl(), config.getAuthUrl()),
                    config.getClientId(),
                    config.getClientSecret(),
                    null,
                    "openid",
                    "code",
                    null,
                    null,
                    null,
                    null);
        }

        public OAuth2AccessToken getAccessToken(String code, String callbackUrl)
                throws ChallengeFailedException
        {
            try {
                OAuthRequest request = createAccessTokenRequest(AccessTokenRequestParams.create(code));
                request.addParameter(OAuthConstants.REDIRECT_URI, callbackUrl);
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
            private final String authorizationBaseUrl;

            public OAuth2Api(String accessTokenEndpoint, String authorizationBaseUrl)
            {
                this.accessTokenEndpoint = requireNonNull(accessTokenEndpoint, "accessTokenEndpoint is null");
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
        }
    }
}
