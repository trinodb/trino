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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;

import java.util.Optional;

public class OAuth2SecurityConfig
{
    private String credential;
    private String token;

    public Optional<String> getCredential()
    {
        return Optional.ofNullable(credential);
    }

    @Config("iceberg.rest-catalog.oauth2.credential")
    @ConfigDescription("The credential to exchange for a token in the OAuth2 client credentials flow with the server")
    @ConfigSecuritySensitive
    public OAuth2SecurityConfig setCredential(String credential)
    {
        this.credential = credential;
        return this;
    }

    public Optional<String> getToken()
    {
        return Optional.ofNullable(token);
    }

    @Config("iceberg.rest-catalog.oauth2.token")
    @ConfigDescription("The Bearer token which will be used for interactions with the server")
    @ConfigSecuritySensitive
    public OAuth2SecurityConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    @AssertTrue(message = "OAuth2 requires a credential or token")
    public boolean credentialOrTokenPresent()
    {
        return credential != null || token != null;
    }
}
