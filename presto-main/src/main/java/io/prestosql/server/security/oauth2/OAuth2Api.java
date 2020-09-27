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

import static java.util.Objects.requireNonNull;

class OAuth2Api
        extends DefaultApi20
{
    private final String accessTokenEndpoint;
    private final String authorizationBaseUrl;

    OAuth2Api(String accessTokenEndpoint, String authorizationBaseUrl)
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

    static OAuth2Api create(OAuth2Config config)
    {
        return new OAuth2Api(config.getTokenUrl(), config.getAuthUrl());
    }
}
