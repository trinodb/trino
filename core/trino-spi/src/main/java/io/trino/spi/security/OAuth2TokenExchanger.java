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
package io.trino.spi.security;

import java.util.Optional;

/**
 * Exchanges the Trino end-user's OAuth2 access token for a token that is valid for a specific
 * downstream service, using the OAuth 2.0 Token Exchange mechanism (RFC 8693).
 * <p>
 * The subject token is the access token the user authenticated to Trino with, which the engine
 * captures in {@link Identity#getExtraCredentials()} under {@link #INTERNAL_ACCESS_TOKEN_KEY}
 * when the {@code http-server.authentication.oauth2.capture-access-token} configuration property
 * is enabled. The exchanged token is scoped to the requested audience and can be used directly,
 * for example as a {@code Bearer} token when calling a REST API.
 * <p>
 * Connectors obtain an instance through {@link io.trino.spi.connector.ConnectorContext#getOAuth2TokenExchanger()}.
 */
public interface OAuth2TokenExchanger
{
    /**
     * Name of the engine-internal extra credential that holds the OAuth2 access token the current
     * user authenticated with. The name is reserved for engine use: clients cannot set credentials
     * starting with {@code internal$}.
     */
    String INTERNAL_ACCESS_TOKEN_KEY = "internal$oauth2.access_token";

    /**
     * Exchanges the OAuth2 access token of {@code identity} for a token usable for a downstream
     * service, as described by {@code request}.
     */
    Optional<OAuth2Token> exchangeToken(ConnectorIdentity identity, TokenExchangeRequest request);
}
