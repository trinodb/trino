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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Describes the target service a token is requested for in an OAuth2 token exchange (RFC 8693).
 * The token endpoint and client credentials that perform the exchange are configured in the
 * engine through the {@code oauth2-token-exchange.*} configuration properties.
 */
public record TokenExchangeRequest(
        List<String> audience,
        List<String> scope,
        Optional<String> requestedTokenType)
{
    public TokenExchangeRequest
    {
        requireNonNull(audience, "audience is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(requestedTokenType, "requestedTokenType is null");
    }

    public TokenExchangeRequest(List<String> audience, List<String> scope)
    {
        this(audience, scope, Optional.empty());
    }
}
