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

import javax.inject.Inject;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OAuth2Endpoints
{
    private final Optional<String> accessTokenIssuer;
    private final String authUrl;
    private final String tokenUrl;
    private final String jwksUrl;
    private final Optional<String> userinfoUrl;

    @Inject
    public OAuth2Endpoints(OAuth2EndpointsConfig config)
    {
        this.accessTokenIssuer = config.getAccessTokenIssuer();
        this.authUrl = config.getAuthUrl();
        this.tokenUrl = config.getTokenUrl();
        this.jwksUrl = config.getJwksUrl();
        this.userinfoUrl = config.getUserinfoUrl();
    }

    public OAuth2Endpoints(Optional<String> accessTokenIssuer, String authUrl, String tokenUrl, String jwksUrl, Optional<String> userinfoUrl)
    {
        this.accessTokenIssuer = requireNonNull(accessTokenIssuer, "accessTokenIssuer is null");
        this.authUrl = requireNonNull(authUrl, "authUrl is null");
        this.tokenUrl = requireNonNull(tokenUrl, "tokenUrl is null");
        this.jwksUrl = requireNonNull(jwksUrl, "jwksUrl is null");
        this.userinfoUrl = requireNonNull(userinfoUrl, "userinfoUrl is null");
    }

    public Optional<String> getAccessTokenIssuer()
    {
        return accessTokenIssuer;
    }

    public String getAuthUrl()
    {
        return authUrl;
    }

    public String getTokenUrl()
    {
        return tokenUrl;
    }

    public String getJwksUrl()
    {
        return jwksUrl;
    }

    public Optional<String> getUserinfoUrl()
    {
        return userinfoUrl;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OAuth2Endpoints that = (OAuth2Endpoints) o;
        return accessTokenIssuer.equals(that.accessTokenIssuer)
                && authUrl.equals(that.authUrl)
                && tokenUrl.equals(that.tokenUrl)
                && jwksUrl.equals(that.jwksUrl)
                && userinfoUrl.equals(that.userinfoUrl);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(accessTokenIssuer, authUrl, tokenUrl, jwksUrl, userinfoUrl);
    }
}
