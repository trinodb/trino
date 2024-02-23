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

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface OAuth2ServerConfigProvider
{
    OAuth2ServerConfig get();

    record OAuth2ServerConfig(Optional<String> accessTokenIssuer, URI authUrl, URI tokenUrl, URI jwksUrl, Optional<URI> userinfoUrl, Optional<URI> endSessionUrl)
    {
        public OAuth2ServerConfig
        {
            requireNonNull(accessTokenIssuer, "accessTokenIssuer is null");
            requireNonNull(authUrl, "authUrl is null");
            requireNonNull(tokenUrl, "tokenUrl is null");
            requireNonNull(jwksUrl, "jwksUrl is null");
            requireNonNull(userinfoUrl, "userinfoUrl is null");
            requireNonNull(endSessionUrl, "endSessionUrl is null");
        }
    }
}
